// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_UUID_H
#define CDSLIB_UUID_H

#include <random>
#include <thread>
#include <climits>

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
    static thread_local size_t tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
    uuid_type rnd = rand::rand();
    rnd <<= sizeof(rand::int_type) * CHAR_BIT;
    rnd |= rand::rand();
    return rnd ^ static_cast<uint64_t>(tid);
}

} //namespace cds

#endif    // #ifndef CDSLIB_UUID_H

