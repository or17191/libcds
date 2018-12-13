// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_DETAILS_SYSTEM_TIMER_H
#define CDSLIB_DETAILS_SYSTEM_TIMER_H

#include <chrono>
#include <exception>

#include <sys/times.h>
#include <unistd.h>

namespace cds { namespace details {

class SystemTimer {
public:
  using duration_type = std::chrono::milliseconds;
  using period_type = duration_type::period;
  struct ret_type {
    duration_type user;
    duration_type sys;
    duration_type clock;
  };

public:
  SystemTimer() : m_clk_tck(sysconf(_SC_CLK_TCK)) {}

  void start() {
    m_before_clock = times(&m_before);
    if (m_before_clock == clock_t(-1)) {
      std::terminate();
    }
  }

  ret_type stop() {
    struct tms after;
    clock_t after_clock = times(&after);
    if (after_clock == clock_t(-1)) {
      std::terminate();
    }
    return ret_type{ticks_to_duration((after.tms_utime - m_before.tms_utime) +
                                      (after.tms_cutime - m_before.tms_cutime)),
                    ticks_to_duration((after.tms_stime - m_before.tms_stime) +
                                      (after.tms_cstime - m_before.tms_cstime)),
                    ticks_to_duration(after_clock - m_before_clock)};
  }

private:
  duration_type ticks_to_duration(clock_t ticks) {
    return duration_type(ticks * period_type::den / m_clk_tck /
                         period_type::num);
  }

  intmax_t m_clk_tck;
  struct tms m_before;
  clock_t m_before_clock;
};

}} // namespace cds::details

#endif // #ifndef CDSLIB_DETAILS_SYSTEM_TIMER_H
