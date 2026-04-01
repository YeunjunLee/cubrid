#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace
{
  using distance_t = float;

#if defined(__AVX2__)
  static inline std::int32_t
  hsum_epi32_avx2 (__m256i v)
  {
    const __m128i lo = _mm256_castsi256_si128 (v);
    const __m128i hi = _mm256_extracti128_si256 (v, 1);
    __m128i sum = _mm_add_epi32 (lo, hi);
    sum = _mm_hadd_epi32 (sum, sum);
    sum = _mm_hadd_epi32 (sum, sum);
    return _mm_cvtsi128_si32 (sum);
  }

  static inline distance_t
  cubvec_inner_product_distance_int8_avx2 (const std::int8_t *vec1, float scale1,
                                           const std::int8_t *vec2, float scale2, std::size_t dim)
  {
    fprintf (stdout, "Using AVX2 optimized int8 inner product distance\n");
    std::size_t i = 0;
    __m256i acc32 = _mm256_setzero_si256 ();
    const __m256i ones16 = _mm256_set1_epi16 (1);

    for (; i + 31 < dim; i += 32)
      {
        const __m256i va = _mm256_loadu_si256 (reinterpret_cast<const __m256i *> (vec1 + i));
        const __m256i vb = _mm256_loadu_si256 (reinterpret_cast<const __m256i *> (vec2 + i));

        const __m256i va_lo_16 = _mm256_cvtepi8_epi16 (_mm256_castsi256_si128 (va));
        const __m256i vb_lo_16 = _mm256_cvtepi8_epi16 (_mm256_castsi256_si128 (vb));
        const __m256i prod_lo_16 = _mm256_mullo_epi16 (va_lo_16, vb_lo_16);
        acc32 = _mm256_add_epi32 (acc32, _mm256_madd_epi16 (prod_lo_16, ones16));

        const __m256i va_hi_16 = _mm256_cvtepi8_epi16 (_mm256_extracti128_si256 (va, 1));
        const __m256i vb_hi_16 = _mm256_cvtepi8_epi16 (_mm256_extracti128_si256 (vb, 1));
        const __m256i prod_hi_16 = _mm256_mullo_epi16 (va_hi_16, vb_hi_16);
        acc32 = _mm256_add_epi32 (acc32, _mm256_madd_epi16 (prod_hi_16, ones16));
      }

    std::int32_t sum = hsum_epi32_avx2 (acc32);
    for (; i < dim; ++i)
      {
        sum += static_cast<std::int32_t> (vec1[i]) * static_cast<std::int32_t> (vec2[i]);
      }

    return static_cast<float> (sum) * scale1 * scale2;
  }
#endif

  static inline distance_t
  cubvec_inner_product_distance_int8_batch4 (const std::int8_t *vec1, float scale1,
                                             const std::int8_t *vec2, float scale2, std::size_t dim)
  {
    std::int32_t sum0 = 0;
    std::int32_t sum1 = 0;
    std::int32_t sum2 = 0;
    std::int32_t sum3 = 0;

    const std::size_t batch_end = dim - (dim % 4);
    for (std::size_t i = 0; i < batch_end; i += 4)
      {
        sum0 += static_cast<std::int32_t> (vec1[i]) * static_cast<std::int32_t> (vec2[i]);
        sum1 += static_cast<std::int32_t> (vec1[i + 1]) * static_cast<std::int32_t> (vec2[i + 1]);
        sum2 += static_cast<std::int32_t> (vec1[i + 2]) * static_cast<std::int32_t> (vec2[i + 2]);
        sum3 += static_cast<std::int32_t> (vec1[i + 3]) * static_cast<std::int32_t> (vec2[i + 3]);
      }

    std::int32_t sum = sum0 + sum1 + sum2 + sum3;
    for (std::size_t i = batch_end; i < dim; ++i)
      {
        sum += static_cast<std::int32_t> (vec1[i]) * static_cast<std::int32_t> (vec2[i]);
      }

    return static_cast<float> (sum) * scale1 * scale2;
  }

  static inline distance_t
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

  static inline distance_t  cubvec_inner_product_distance_float (const float *vec1, const float *vec2, std::size_t dim)
  {
    float sum = 0.0f;

    #pragma omp simd reduction(+ : sum)
    for (std::size_t i = 0; i < dim; ++i)
      {
        sum += vec1[i] * vec2[i];
      }
    return sum;
  }

  static inline distance_t  cubvec_inner_product_distance_int8 (const std::int8_t *vec1, float scale1,
                                      const std::int8_t *vec2, float scale2, std::size_t dim)
  {
#if defined(__AVX2__)
    return cubvec_inner_product_distance_int8_avx2 (vec1, scale1, vec2, scale2, dim);
#else
    return cubvec_inner_product_distance_int8_batch4 (vec1, scale1, vec2, scale2, dim);
#endif
  }

  static inline distance_t
  cubvec_cosine_distance_int8 (const std::int8_t *vec1, float scale1,
                               const std::int8_t *vec2, float scale2, std::size_t dim)
  {
    return 1.0f - cubvec_inner_product_distance_int8 (vec1, scale1, vec2, scale2, dim);
  }

  static void
  normalize (std::vector<float> &v)
  {
    float norm_sq = 0.0f;
    for (float x : v)
      {
        norm_sq += x * x;
      }

    const float norm = std::sqrt (norm_sq);
    if (norm > 0.0f)
      {
        const float inv = 1.0f / norm;
        for (float &x : v)
          {
            x *= inv;
          }
      }
  }

  struct quantized_vec
  {
    std::vector<std::int8_t> values;
    float scale;
  };

  static quantized_vec
  quantize_to_int8 (const std::vector<float> &v)
  {
    float max_abs = 0.0f;
    for (float x : v)
      {
        max_abs = std::max (max_abs, std::fabs (x));
      }

    quantized_vec q;
    q.values.resize (v.size ());

    if (max_abs <= std::numeric_limits<float>::min ())
      {
        q.scale = 1.0f;
        std::fill (q.values.begin (), q.values.end (), 0);
        return q;
      }

    q.scale = max_abs / 127.0f;
    const float inv_scale = 1.0f / q.scale;

    for (std::size_t i = 0; i < v.size (); ++i)
      {
        const float scaled = v[i] * inv_scale;
        const float clamped = std::max (-127.0f, std::min (127.0f, std::round (scaled)));
        q.values[i] = static_cast<std::int8_t> (clamped);
      }

    return q;
  }
}

int
main (int argc, char **argv)
{
  std::size_t dim = 768;
  std::size_t n_pairs = 50000;
  int repeats = 5;

  if (argc >= 2)
    {
      dim = static_cast<std::size_t> (std::strtoull (argv[1], nullptr, 10));
    }
  if (argc >= 3)
    {
      n_pairs = static_cast<std::size_t> (std::strtoull (argv[2], nullptr, 10));
    }
  if (argc >= 4)
    {
      repeats = std::max (1, std::atoi (argv[3]));
    }

  std::mt19937 rng (42);
  std::uniform_real_distribution<float> dist (-1.0f, 1.0f);

  std::vector<std::vector<float>> a_float (n_pairs, std::vector<float> (dim));
  std::vector<std::vector<float>> b_float (n_pairs, std::vector<float> (dim));

  for (std::size_t i = 0; i < n_pairs; ++i)
    {
      for (std::size_t d = 0; d < dim; ++d)
        {
          a_float[i][d] = dist (rng);
          b_float[i][d] = dist (rng);
        }
      normalize (a_float[i]);
      normalize (b_float[i]);
    }

  std::vector<quantized_vec> a_i8 (n_pairs);
  std::vector<quantized_vec> b_i8 (n_pairs);
  for (std::size_t i = 0; i < n_pairs; ++i)
    {
      a_i8[i] = quantize_to_int8 (a_float[i]);
      b_i8[i] = quantize_to_int8 (b_float[i]);
    }

  volatile float sink = 0.0f;

  for (std::size_t i = 0; i < std::min<std::size_t> (1000, n_pairs); ++i)
    {
      sink += cubvec_cosine_distance (a_float[i].data (), b_float[i].data (), dim);
      sink += cubvec_cosine_distance_int8 (a_i8[i].values.data (), a_i8[i].scale,
                                           b_i8[i].values.data (), b_i8[i].scale, dim);
    }

  double best_float_ns = std::numeric_limits<double>::max ();
  double best_int8_ns = std::numeric_limits<double>::max ();

  for (int r = 0; r < repeats; ++r)
    {
      auto t0 = std::chrono::steady_clock::now ();
      for (std::size_t i = 0; i < n_pairs; ++i)
        {
          sink += cubvec_cosine_distance (a_float[i].data (), b_float[i].data (), dim);
        }
      auto t1 = std::chrono::steady_clock::now ();

      auto t2 = std::chrono::steady_clock::now ();
      for (std::size_t i = 0; i < n_pairs; ++i)
        {
          sink += cubvec_cosine_distance_int8 (a_i8[i].values.data (), a_i8[i].scale,
                                               b_i8[i].values.data (), b_i8[i].scale, dim);
        }
      auto t3 = std::chrono::steady_clock::now ();

      const double float_ns = static_cast<double> (std::chrono::duration_cast<std::chrono::nanoseconds> (t1 - t0).count ())
                              / static_cast<double> (n_pairs);
      const double int8_ns = static_cast<double> (std::chrono::duration_cast<std::chrono::nanoseconds> (t3 - t2).count ())
                             / static_cast<double> (n_pairs);

      best_float_ns = std::min (best_float_ns, float_ns);
      best_int8_ns = std::min (best_int8_ns, int8_ns);
    }

  std::cout << std::fixed << std::setprecision (2);
  std::cout << "dim=" << dim << ", n_pairs=" << n_pairs << ", repeats=" << repeats << "\n";
#if defined(__AVX2__)
  std::cout << "build_macro=__AVX2__ enabled\n";
#else
  std::cout << "build_macro=__AVX2__ disabled\n";
#endif
  std::cout << "float cosine: " << best_float_ns << " ns/call\n";
  std::cout << "int8 cosine : " << best_int8_ns << " ns/call\n";
  std::cout << "speedup (float/int8): " << (best_float_ns / best_int8_ns) << "x\n";
  std::cout << "checksum(ignore): " << sink << "\n";

  return 0;
}
