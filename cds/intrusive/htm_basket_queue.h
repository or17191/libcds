// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
#define CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H

#include <type_traits>

#include <cds/intrusive/basket_queue.h>
#include <cds/sync/htm.h>

namespace cds { namespace intrusive {

    namespace htm_basket_queue {
      struct htm_insert {
        static void delay(size_t s) {
          volatile int x;
          for(size_t i = 0; i < s; ++i) {
            x = 0;
          }
        }

        using InsertResult = basket_queue::atomics_insert::InsertResult;

        template <class MemoryModel, class MarkedPtr>
        static InsertResult _(MarkedPtr old_node, MarkedPtr new_node, size_t thread_count = 1) {
          new_node->m_pNext.store(MarkedPtr{}, MemoryModel::memory_order_relaxed);
          auto& old = old_node->m_pNext;
          int ret;
          do {
            if ((ret = _xbegin()) == _XBEGIN_STARTED) {
              MarkedPtr pNext = old.load(MemoryModel::memory_order_relaxed);
              if (pNext.ptr() != nullptr) {
                _xabort(0x01);
              }
              delay(30 * thread_count);
              old.store(new_node, MemoryModel::memory_order_relaxed);
              _xend();
            }
            if (ret == _XBEGIN_STARTED) {
              return InsertResult::SUCCESSFUL_INSERT;
            }
            if ((ret & _XABORT_EXPLICIT) != 0) {
              return InsertResult::NOT_NULL;
            }
            if ((ret & _XABORT_CONFLICT) != 0) {
              delay(300);
              for(size_t i = 0; i < 5; ++ i) {
                if(old.load(MemoryModel::memory_order_relaxed).ptr() != nullptr) {
                  return InsertResult::FAILED_INSERT;
                }
                delay(100);
              }
            }
          } while (true);
          __builtin_unreachable();
        }
      };

      struct traits : opt::insert_policy<htm_insert>::template pack<basket_queue::traits> {};
    }

    template <typename GC, typename T, typename Traits = htm_basket_queue::traits>
    class HTMBasketQueue: public BasketQueue<GC, T, opt::insert_policy<htm_basket_queue::htm_insert>::template pack<Traits>> {
    private:
      typedef BasketQueue<GC, T, opt::insert_policy<htm_basket_queue::htm_insert>::template pack<Traits>> base_type;
    public:
      using base_type::base_type;
      static_assert(std::is_same<typename base_type::insert_policy, htm_basket_queue::htm_insert>::value, "Must use htm_insert");
    };

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
