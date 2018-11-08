// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_UUID_H
#define CDSLIB_UUID_H

#include <random>
#include <climits>

#include <cds/threading/details/auto_detect.h>

namespace cds { namespace rand { 

namespace detail {

typedef std::minstd_rand engine;

inline std::minstd_rand& instance() {
  static thread_local std::minstd_rand value;
  return value;
}

} // namespace detail

typedef detail::engine::result_type int_type;

inline void seed(int_type seed) {
  detail::instance().seed(seed);
}

inline int_type rand() {
  return detail::instance()();
}

} // namespace rand

typedef uint64_t uuid_type;

inline uuid_type uuid() {
    static thread_local uuid_type counter = cds::threading::Manager::thread_data()->m_nThreadId;
    counter += (1 << CHAR_BIT); // Make 0 an illegal value
    return counter;
}

} //namespace cds

#endif    // #ifndef CDSLIB_UUID_H

