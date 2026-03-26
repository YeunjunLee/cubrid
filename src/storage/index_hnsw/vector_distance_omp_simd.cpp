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

#include "vector_distance.hpp"

#include <cmath>
#include <algorithm>
#include <omp.h>

#include "porting_inline.hpp"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace cubhnsw
{
  namespace
  {
    STATIC_INLINE distance_t __attribute__ ((ALWAYS_INLINE))
    cubvec_inner_product_distance_int8_batch4 (const std::int8_t *vec1, float scale1,
					       const std::int8_t *vec2, float scale2, std::size_t dim)
    {
      int sum0 = 0;
      int sum1 = 0;
      int sum2 = 0;
      int sum3 = 0;

      const std::size_t batch_end = dim - (dim % 4);
      #pragma omp simd reduction(+ : sum0, sum1, sum2, sum3)
      for (std::size_t i = 0; i < batch_end; i += 4)
	{
	  sum0 += static_cast<int> (vec1[i]) * static_cast<int> (vec2[i]);
	  sum1 += static_cast<int> (vec1[i + 1]) * static_cast<int> (vec2[i + 1]);
	  sum2 += static_cast<int> (vec1[i + 2]) * static_cast<int> (vec2[i + 2]);
	  sum3 += static_cast<int> (vec1[i + 3]) * static_cast<int> (vec2[i + 3]);
	}

      int sum = sum0 + sum1 + sum2 + sum3;
      #pragma omp simd reduction(+ : sum)
      for (std::size_t i = batch_end; i < dim; ++i)
	{
	  sum += static_cast<int> (vec1[i]) * static_cast<int> (vec2[i]);
	}

      return static_cast<float> (sum) * scale1 * scale2;
    }

    STATIC_INLINE distance_t __attribute__ ((ALWAYS_INLINE))
    cubvec_l2_distance_int8_batch4 (const std::int8_t *vec1, float scale1,
				    const std::int8_t *vec2, float scale2, std::size_t dim)
    {
      float sum = 0.0f;
      #pragma omp simd reduction(+ : sum)
      for (std::size_t i = 0; i < dim; ++i)
	{
	  const float d = static_cast<float> (vec1[i]) * scale1 - static_cast<float> (vec2[i]) * scale2;
	  sum += d * d;
	}
      return sum;
    }
  } // namespace

  bool
  cubvec_cosine_normalize (float *__restrict vec, std::size_t dim)
  {
    float norm_sq = 0.0f;

    #pragma omp simd reduction(+ : norm_sq)
    for (std::size_t i = 0; i < dim; ++i)
      {
	norm_sq += vec[i] * vec[i];
      }

    constexpr float eps = 1e-12f;
    if (norm_sq < eps)
      {
	// zero / near-zero vector is invalid for cosine/IP
	return false;
      }

    const float inv_norm = 1.0f / std::sqrt (norm_sq);

    #pragma omp simd
    for (std::size_t i = 0; i < dim; ++i)
      {
	vec[i] *= inv_norm;
      }

    return true;  // unit vector
  }

  STATIC_INLINE distance_t __attribute__ ((ALWAYS_INLINE))
  cubvec_cosine_distance (const float *vec1, const float *vec2, std::size_t dim)
  {
    float dot = 0.0f;

    #pragma omp simd reduction(+ : dot)
    for (std::size_t i = 0; i < dim; ++i)
      {
	dot += vec1[i] * vec2[i];
      }
    return 1.0f - dot;
  }

  STATIC_INLINE distance_t __attribute__ ((ALWAYS_INLINE))
  cubvec_inner_product_distance (const float *vec1, const float *vec2, std::size_t dim)
  {
    float sum = 0.0f;

    #pragma omp simd reduction(+ : sum)
    for (std::size_t i = 0; i < dim; ++i)
      {
	sum += vec1[i] * vec2[i];
      }
    return sum;
  };

  distance_t
  cubvec_cosine_distance_int8 (const std::int8_t *vec1, float scale1,
			       const std::int8_t *vec2, float scale2, std::size_t dim)
  {
    return 1.0f - cubvec_inner_product_distance_int8 (vec1, scale1, vec2, scale2, dim);
  }

  distance_t
  cubvec_inner_product_distance_int8 (const std::int8_t *vec1, float scale1,
				      const std::int8_t *vec2, float scale2, std::size_t dim)
  {
    return cubvec_inner_product_distance_int8_batch4 (vec1, scale1, vec2, scale2, dim);
  }

  STATIC_INLINE distance_t __attribute__ ((ALWAYS_INLINE))
  cubvec_l2_distance (const float *vec1, const float *vec2, std::size_t dim)
  {
    float sum = 0.0f;
    #pragma omp simd reduction(+ : sum)
    for (std::size_t i = 0; i < dim; ++i)
      {
	const float d = vec1[i] - vec2[i];
	sum += d * d;
      }
    return sum;
  }

  distance_t
  cubvec_l2_distance_int8 (const std::int8_t *vec1, float scale1,
			   const std::int8_t *vec2, float scale2, std::size_t dim)
  {
    return cubvec_l2_distance_int8_batch4 (vec1, scale1, vec2, scale2, dim);
  }

  const std::array<distance_fn_t,
	static_cast<std::size_t> (vector_distance_metric_t::MAX)>
	metric_table =
  {
    cubvec_cosine_distance,
    cubvec_l2_distance,
    cubvec_inner_product_distance
  };

} // namespace cubhnsw
