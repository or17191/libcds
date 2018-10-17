// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_SYNC_HTM_H
#define CDSLIB_SYNC_HTM_H

#ifdef __RTM__

#include <utility>

#include <immintrin.h>

namespace cds {
    /// Synchronization primitives
    namespace sync {
        /// HTM transaction utility
        /**
          Wraps the details of the transaction managment.
          Supports several retries, and failure handlers.
        */
        template <class Transaction, class Fallback>
        bool htm(Transaction &&transaction, Fallback &&fallback) {
            if (_xbegin() == _XBEGIN_STARTED) {
                transaction();
                _xend();
                return true;
            } else {
                return false;
            }
        }

        template <class Transaction>
        bool htm(Transaction &&transaction) {
            return htm(std::forward<Transaction>(transaction), []{});
        }

        template <class Transaction, class Fallback>
        bool htm(Transaction &&transaction, Fallback &&fallback, size_t tries) {
            for (size_t i = 0; i < tries; ++i) {
                if (htm(std::forward<Transaction>(transaction))) {
                    return true;
                }
            }
            fallback();
            return false;
        }


        template <class Transaction>
        bool htm(Transaction &&transaction, size_t tries) {
          return htm(std::forward<Transaction>(transaction), []{}, tries);
        }

        constexpr bool RTM_ENABLED = true;
    } // namespace sync
} // namespace cds

#else // __RTM__

namespace cds {
    namespace sync {
        template <class... Args>
        bool htm(Args&&...) {
          // Fast fail if don't have htm support
          std::terminate();
        }
        constexpr bool RTM_ENABLED = false;
    } // namespace sync
} // namespace cds

#endif // __RTM__

#endif // #ifndef CDSLIB_SYNC_HTM_H
