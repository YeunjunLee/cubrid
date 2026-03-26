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
#include <cstring>
#include <limits>
#include <omp.h>

#include "porting_inline.hpp"

// XXX: SHOULD BE THE LAST INCLUDE HEADER
#include "memory_wrapper.hpp"

namespace cubhnsw
{
  namespace
  {
    STATIC_INLINE std::uint32_t __attribute__ ((ALWAYS_INLINE))
    load_float_bits (float value)
    {
      std::uint32_t bits = 0;
      std::memcpy (&bits, &value, sizeof (bits));
      return bits;
    }

    STATIC_INLINE float __attribute__ ((ALWAYS_INLINE))
    store_float_bits (std::uint32_t bits)
    {
      float value = 0.0f;
      std::memcpy (&value, &bits, sizeof (value));
      return value;
    }

    STATIC_INLINE std::uint16_t __attribute__ ((ALWAYS_INLINE))
    float_to_half_bits (float value)
    {
      const std::uint32_t bits = load_float_bits (value);
      const std::uint32_t sign = (bits >> 16) & 0x8000u;
      std::uint32_t exponent = (bits >> 23) & 0xffu;
      std::uint32_t mantissa = bits & 0x007fffffu;

      if (exponent == 0xffu)
	{
	  if (mantissa != 0)
	    {
	      return static_cast<std::uint16_t> (sign | 0x7e00u);
	    }
	  return static_cast<std::uint16_t> (sign | 0x7c00u);
	}

      if (exponent == 0)
	{
	  return static_cast<std::uint16_t> (sign);
	}

      const int half_exponent = static_cast<int> (exponent) - 127 + 15;
      if (half_exponent >= 0x1f)
	{
	  return static_cast<std::uint16_t> (sign | 0x7c00u);
	}

      if (half_exponent <= 0)
	{
	  if (half_exponent < -10)
	    {
	      return static_cast<std::uint16_t> (sign);
	    }

	  mantissa |= 0x00800000u;
	  const std::uint32_t shift = static_cast<std::uint32_t> (1 - half_exponent);
	  std::uint32_t half_mantissa = mantissa >> (shift + 13);
	  const std::uint32_t round_bit = (mantissa >> (shift + 12)) & 1u;
	  half_mantissa += round_bit;
	  return static_cast<std::uint16_t> (sign | half_mantissa);
	}

      std::uint16_t half_bits =
	static_cast<std::uint16_t> (sign | (static_cast<std::uint32_t> (half_exponent) << 10) | (mantissa >> 13));
      if ((mantissa & 0x00001000u) != 0)
	{
	  ++half_bits;
	}
      return half_bits;
    }

    STATIC_INLINE float __attribute__ ((ALWAYS_INLINE))
    half_bits_to_float (std::uint16_t bits)
    {
      const std::uint32_t sign = (static_cast<std::uint32_t> (bits & 0x8000u)) << 16;
      const std::uint32_t exponent = (bits >> 10) & 0x1fu;
      const std::uint32_t mantissa = bits & 0x03ffu;

      if (exponent == 0)
	{
	  if (mantissa == 0)
	    {
	      return store_float_bits (sign);
	    }

	  float value = static_cast<float> (mantissa) / 1024.0f;
	  value = std::ldexp (value, -14);
	  return (sign != 0) ? -value : value;
	}

      if (exponent == 0x1fu)
	{
	  const std::uint32_t inf_nan_bits = sign | 0x7f800000u | (mantissa << 13);
	  return store_float_bits (inf_nan_bits);
	}

      const std::uint32_t float_bits =
	sign | ((exponent + (127u - 15u)) << 23) | (mantissa << 13);
      return store_float_bits (float_bits);
    }

    STATIC_INLINE distance_t __attribute__ ((ALWAYS_INLINE))
    cubvec_inner_product_distance_float16_batch4 (const float16 *vec1, const float16 *vec2, std::size_t dim)
    {
      float sum0 = 0.0f;
      float sum1 = 0.0f;
      float sum2 = 0.0f;
      float sum3 = 0.0f;

      const std::size_t batch_end = dim - (dim % 4);
      #pragma omp simd reduction(+ : sum0, sum1, sum2, sum3)
      for (std::size_t i = 0; i < batch_end; i += 4)
	{
	  sum0 += vec1[i].to_float () * vec2[i].to_float ();
	  sum1 += vec1[i + 1].to_float () * vec2[i + 1].to_float ();
	  sum2 += vec1[i + 2].to_float () * vec2[i + 2].to_float ();
	  sum3 += vec1[i + 3].to_float () * vec2[i + 3].to_float ();
	}

      float sum = sum0 + sum1 + sum2 + sum3;
      #pragma omp simd reduction(+ : sum)
      for (std::size_t i = batch_end; i < dim; ++i)
	{
	  sum += vec1[i].to_float () * vec2[i].to_float ();
	}

      return sum;
    }
  } // namespace

  float16::float16 ()
    : bits (0)
  {
  }

  float16::float16 (float value)
    : bits (float_to_half_bits (value))
  {
  }

  float
  float16::to_float () const
  {
    return half_bits_to_float (bits);
  }

  float16::operator float () const
  {
    return to_float ();
  }

  float16
  float16::from_bits (std::uint16_t raw_bits)
  {
    float16 value;
    value.bits = raw_bits;
    return value;
  }

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
  cubvec_cosine_distance_float16 (const float16 *vec1, const float16 *vec2, std::size_t dim)
  {
    return 1.0f - cubvec_inner_product_distance_float16 (vec1, vec2, dim);
  }

  distance_t
  cubvec_inner_product_distance_float16 (const float16 *vec1, const float16 *vec2, std::size_t dim)
  {
    return cubvec_inner_product_distance_float16_batch4 (vec1, vec2, dim);
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
  cubvec_l2_distance_float16 (const float16 *vec1, const float16 *vec2, std::size_t dim)
  {
    float sum = 0.0f;
    #pragma omp simd reduction(+ : sum)
    for (std::size_t i = 0; i < dim; ++i)
      {
	const float d = vec1[i].to_float () - vec2[i].to_float ();
	sum += d * d;
      }
    return sum;
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
