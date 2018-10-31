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
        
        class htm_status {
        public:
          using int_type = decltype(_xbegin());
          explicit htm_status(int_type s): m_status(s) {}

          bool started() const { return m_status == _XBEGIN_STARTED; }
          explicit operator bool() const { return started(); }

          bool explicit_() const { return m_status & _XABORT_EXPLICIT != 0; }
          uint8_t explicit_code() const { return static_cast<uint8_t>(_XABORT_CODE(m_status)); }

        private:
          int_type m_status;
        };

        /**
          Wraps the details of the transaction managment.
          Supports several retries, and failure handlers.
        */
        template <class Transaction, class Fallback>
        htm_status htm(Transaction &&transaction, Fallback &&fallback) {
            htm_status status(_xbegin());
            if (status) {
                transaction();
                _xend();
            } else {
                fallback();
            }
            return status;
        }

        template <class Transaction>
        htm_status htm(Transaction &&transaction) {
            return htm(std::forward<Transaction>(transaction), [] {});
        }

        template <class Transaction, class Fallback>
        htm_status htm(Transaction &&transaction, Fallback &&fallback, size_t tries) {
            for (size_t i = 0; i < tries; ++i) {
                if (htm(std::forward<Transaction>(transaction))) {
                    return true;
                }
            }
            fallback();
            return false;
        }

        template <class Transaction>
        htm_status htm(Transaction &&transaction, size_t tries) {
            return htm(std::forward<Transaction>(transaction), [] {}, tries);
        }

        template <uint8_t Value>
        void abort() {
            _xabort(Value);
        }

    } // namespace sync
} // namespace cds

#else // CDS_HTM_SUPPORT

namespace cds {
    namespace sync {
        class htm_status {
        public:
          using int_type = unsigned int;
          explicit htm_status(int_type s): m_status(s) {}

          bool started() const { return false; }
          explicit operator bool() const { return started(); }

          bool explicit_() const { return false; }
          uint8_t explicit_code() const { return 0; }

        private:
          int_type m_status;
        };

        template <class... Args>
        htm_status htm(Args &&...) {
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
