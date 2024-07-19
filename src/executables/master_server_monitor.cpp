/*
 *
 * Copyright 2016 CUBRID Corporation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

//
// master_server_monitor.cpp - Server Revive monitoring module
//

#include <sstream>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <system_parameter.h>
#include <master_request.h>
#include "master_server_monitor.hpp"

#define GET_ELAPSED_TIME(end_time, start_time) \
            ((double)(end_time.tv_sec - start_time.tv_sec) * 1000 + \
             (end_time.tv_usec - start_time.tv_usec)/1000.0)

std::unique_ptr<server_monitor> master_Server_monitor = nullptr;

server_monitor::server_monitor ()
{
  m_server_entry_list = std::make_unique<std::vector<server_entry>> ();
  fprintf (stdout, "server_entry_list is created. \n");

  m_thread_shutdown = false;
  m_monitoring_thread = std::make_unique<std::thread> ([this]()
  {
    while (!m_thread_shutdown)
      {
	for (auto &entry : *m_server_entry_list)
	  {
	    if (!entry.m_need_revive)
	      {
		continue;
	      }
	    fprintf (stdout, "Server %s is dead. \n", entry.m_server_name.c_str());
	    struct timeval tv;
	    pid_t pid;
	    gettimeofday (&tv, nullptr);
	    if (GET_ELAPSED_TIME (entry.m_last_revive_time,
				  tv) < prm_get_integer_value (PRM_ID_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF_IN_MSECS))
	      {
		goto cleanup;
	      }
	    int tries = 0;
	    int max_retries = prm_get_integer_value (PRM_ID_HA_MAX_PROCESS_START_CONFIRM);
	    while (tries < max_retries)
	      {
		fprintf (stdout, "Server %s is now reviving. \n", entry.m_server_name.c_str());
		pid = fork ();
		if (pid < 0)
		  {
		    tries++;
		    continue;
		  }
		  // Child process will execute the server with entry.m_argv and entry.m_exec_path. If it fails, remove the child process and try again with the next child process.
                  else if (pid == 0)
                    {
                      char *argv[entry.m_argv.size () + 1];
                      for (size_t i = 0; i < entry.m_argv.size (); i++)
                        {
                          argv[i] = const_cast<char *> (entry.m_argv[i].c_str ());
                        }
                      argv[entry.m_argv.size ()] = nullptr;
                      execv (entry.m_exec_path.c_str (), argv);
                    }
                  else
                    {
                      // Parent process will wait for the child process to finish.
                      int status;
                      waitpid (pid, &status, 0);
                      if (WIFEXITED (status) && WEXITSTATUS (status) == 0)
                        {
                          entry.m_pid = pid;
                          entry.m_need_revive = false;
                          gettimeofday (&entry.m_last_revive_time, NULL);
                          fprintf (stdout, "Server %s is revived. \n", entry.m_server_name.c_str());
                          break;
                        }
                      else
                        {
                          tries++;
                          continue;
                        }
                    }
	      }
              cleanup:
                m_server_entry_list->remove_server_entry_by_conn (entry.m_conn);
                css_remove_entry_by_conn (entry.m_conn, &css_Master_socket_anchor);
	  }
      }
  });
  fprintf (stdout, "server_monitor_thread is created. \n");
  fflush (stdout);
}

// In server_monitor destructor, it should guarentee that
// m_monitoring_thread is terminated before m_monitor_list is deleted.
server_monitor::~server_monitor ()
{
  m_thread_shutdown = true;
  if (m_monitoring_thread->joinable())
    {
      m_monitoring_thread->join();
      fprintf (stdout, "server_monitor_thread is terminated. \n");
    }

  assert (m_server_entry_list->size () == 0);
  fprintf (stdout, "server_entry_list is deleted. \n");
  fflush (stdout);
}

server_monitor::server_entry::
server_entry (int pid, const char *server_name, const char *exec_path, char *args, CSS_CONN_ENTRY *conn)
  : m_pid {pid}
  , m_server_name {server_name}
  , m_exec_path {exec_path}
  , m_conn {conn}
  , m_last_revive_time {0, 0}
  , m_need_revive {false}
{
  if (args != nullptr)
    {
      proc_make_arg (args);
    }
}

void
server_monitor::find_set_entry_to_revive (CSS_CONN_ENTRY *conn)
{
  fprintf (stdout, "Server will be checked. \n");
  for (auto &entry : *m_server_entry_list)
    {
      fprintf (stdout, "Server is being checked. \n");
      if (entry.m_conn == conn)
	{
	  entry.m_need_revive = true;
	  return;
	}
    }
}

void
server_monitor::server_entry::proc_make_arg (char *args)
{
  std::istringstream iss (args);
  std::string tok;
  while (iss >> tok)
    {
      m_argv.push_back (tok);
    }
}
