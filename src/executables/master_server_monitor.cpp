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

static int proc_execute_internal (const char *file, const char *args[], bool wait_child, bool close_output,
				  bool close_err, bool hide_cmd_args, int *out_pid);

server_monitor::server_monitor ()
{

  m_server_entry_list = std::make_unique<std::vector<server_entry>> ();
  fprintf (stdout, "server_entry_list is created. \n");

  m_thread_shutdown = false;
  m_monitoring_thread = std::make_unique<std::thread> ([this]()
  {
    int error_code;
    pid_t pid;
    struct timeval tv;
    int tries, max_retries;
    while (!m_thread_shutdown)
      {
	for (auto &entry : *m_server_entry_list)
	  {
	    if (!entry.m_need_revive)
	      {
		continue;
	      }
	    fprintf (stdout, "Server %s is dead. \n", entry.m_server_name.c_str());
	    gettimeofday (&tv, nullptr);
	    if (GET_ELAPSED_TIME (entry.m_last_revive_time,
				  tv) < prm_get_integer_value (PRM_ID_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF_IN_MSECS))
	      {
		m_server_entry_list->remove_server_entry_by_conn (entry.m_conn);
		css_remove_entry_by_conn (entry.m_conn, &css_Master_socket_anchor);
	      }
	    tries = 0;
	    max_retries = prm_get_integer_value (PRM_ID_HA_MAX_PROCESS_START_CONFIRM);
	    while (tries < max_retries)
	      {
		char *argv[entry.m_argv.size () + 1];
		for (int i = 0; i < entry.m_argv.size (); i++)
		  {
		    argv[i] = const_cast<char *> (entry.m_argv[i].c_str());
		  }
		argv[entry.m_argv.size ()] = nullptr;

		error_code = proc_execute_internal (entry.m_exec_path.c_str(), argv, false, false, false, false, &pid);
		if (error_code != NO_ERROR)
		  {
		    tries++;
		    continue;
		  }

		if (kill (pid, 0) != 0)
		  {
		    tries++;
		    continue;
		  }

		else
		  {
		    fprintf (stdout, "Server %s is revived. \n", entry.m_server_name.c_str());
		    m_server_entry_list->remove_server_entry_by_conn (entry.m_conn);
		    css_remove_entry_by_conn (entry.m_conn, &css_Master_socket_anchor);
		    break;
		  }
	      }
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


#if defined(WINDOWS)
static int
proc_execute_internal (const char *file, const char *args[], bool wait_child, bool close_output, bool close_err,
		       bool hide_cmd_args, int *out_pid)
{
  STARTUPINFO si;
  PROCESS_INFORMATION pi;
  int i, j, k, cmd_arg_len;
  char cmd_arg[1024];
  char executable_path[PATH_MAX];
  int ret_code = NO_ERROR;
  bool inherited_handle = TRUE;
  char fixed_arg[1024];		/* replace " with "" */

  if (out_pid)
    {
      *out_pid = 0;
    }

  (void) envvar_bindir_file (executable_path, PATH_MAX, file);

  for (i = 0, cmd_arg_len = 0; args[i]; i++)
    {
      if (strchr (args[i], '"') == NULL)
	{
	  cmd_arg_len += sprintf (cmd_arg + cmd_arg_len, "\"%s\" ", args[i]);
	}
      else
	{
	  k = 0;
	  for (j = 0; args[i][j] != '\0'; j++)
	    {
	      if (args[i][j] == '"')
		{
		  fixed_arg[k++] = '"';
		}
	      fixed_arg[k++] = args[i][j];
	    }
	  fixed_arg[k] = '\0';

	  cmd_arg_len += sprintf (cmd_arg + cmd_arg_len, "\"%s\" ", fixed_arg);
	}
    }

  GetStartupInfo (&si);
  si.dwFlags = si.dwFlags | STARTF_USESTDHANDLES;
  si.hStdInput = GetStdHandle (STD_INPUT_HANDLE);
  si.hStdOutput = GetStdHandle (STD_OUTPUT_HANDLE);
  si.hStdError = GetStdHandle (STD_ERROR_HANDLE);
  inherited_handle = TRUE;

  if (close_output)
    {
      si.hStdOutput = NULL;
    }

  if (close_err)
    {
      si.hStdError = NULL;
    }

  if (!CreateProcess (executable_path, cmd_arg, NULL, NULL, inherited_handle, 0, NULL, NULL, &si, &pi))
    {
      return ER_FAILED;
    }

  if (wait_child)
    {
      DWORD status = 0;
      WaitForSingleObject (pi.hProcess, INFINITE);
      GetExitCodeProcess (pi.hProcess, &status);
      ret_code = status;
    }
  else
    {
      if (out_pid)
	{
	  *out_pid = pi.dwProcessId;
	}
    }

  CloseHandle (pi.hProcess);
  CloseHandle (pi.hThread);
  return ret_code;
}

#else
static int
proc_execute_internal (const char *file, const char *args[], bool wait_child, bool close_output, bool close_err,
		       bool hide_cmd_args, int *out_pid)
{
  pid_t pid, tmp;
  char executable_path[PATH_MAX];

  if (out_pid)
    {
      *out_pid = 0;
    }

  (void) envvar_bindir_file (executable_path, PATH_MAX, file);

  /* do not process SIGCHLD, a child process will be defunct */
  if (wait_child)
    {
      signal (SIGCHLD, SIG_DFL);
    }
  else
    {
      signal (SIGCHLD, SIG_IGN);
    }

  pid = fork ();
  if (pid == -1)
    {
      perror ("fork");
      return ER_GENERIC_ERROR;
    }
  else if (pid == 0)
    {
      /* a child process handle SIGCHLD to SIG_DFL */
      signal (SIGCHLD, SIG_DFL);
      if (close_output)
	{
	  fclose (stdout);
	}

      if (close_err)
	{
	  fclose (stderr);
	}

      if (execv (executable_path, (char *const *) args) == -1)
	{
	  perror ("execv");
	  return ER_GENERIC_ERROR;
	}
    }
  else
    {
      int status = 0;

      if (hide_cmd_args == true)
	{
	  /* for hide password */
	  hide_cmd_line_args ((char **) args);
	}

      /* sleep (0); */
      if (wait_child)
	{
	  do
	    {
	      tmp = waitpid (-1, &status, 0);
	      if (tmp == -1)
		{
		  perror ("waitpid");
		  return ER_GENERIC_ERROR;
		}
	    }
	  while (tmp != pid);
	}
      else
	{
	  /* sleep (3); */
	  if (out_pid)
	    {
	      *out_pid = pid;
	    }
	  return NO_ERROR;
	}

      if (WIFEXITED (status))
	{
	  return WEXITSTATUS (status);
	}
    }
  return ER_GENERIC_ERROR;
}
#endif
