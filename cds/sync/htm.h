// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_SYNC_HTM_H
#define CDSLIB_SYNC_HTM_H

#if !defined( CDS_DISABLE_HTM ) && defined( __RTM__ )
#    define CDS_HTM_SUPPORT
#endif

#ifdef CDS_HTM_SUPPORT

#include <utility>
#include <exception>

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
            bool ok = _xbegin() == _XBEGIN_STARTED;
            if (ok) {
                transaction();
                _xend();
            } else {
                fallback();
            }
            return ok;
        }

        template <class Transaction>
        bool htm(Transaction &&transaction) {
            return htm(std::forward<Transaction>(transaction), [] {});
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
            return htm(std::forward<Transaction>(transaction), [] {}, tries);
        }

        template <class... Args>
        decltype(auto) abort(Args &&... args) {
            return _xabort(std::forward<Args>(args)...);
        }

    } // namespace sync
} // namespace cds

#else // CDS_HTM_SUPPORT

namespace cds {
    namespace sync {
        template <class... Args>
        bool htm(Args &&...) {
            // Fast fail if don't have htm support
            std::terminate();
        }
        template <class... Args>
        void abort(Args &&...) {
            std::terminate();
        }
    } // namespace sync
} // namespace cds

#endif // CDS_HTM_SUPPORT

#endif // #ifndef CDSLIB_SYNC_HTM_H
