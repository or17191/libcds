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
          auto transaction = [&] {
              MarkedPtr pNext = old_node->m_pNext.load(MemoryModel::memory_order_relaxed);
              if (pNext.ptr() != nullptr) {
                sync::abort<0x01>();
              }
              new_node->m_pNext.store(MarkedPtr{}, MemoryModel::memory_order_relaxed);
              delay(200 * thread_count);
              old_node->m_pNext.store(new_node, MemoryModel::memory_order_relaxed);
          };
          sync::htm_status status;
          do {
            status = sync::htm(transaction);
            if (status.started()) {
              return InsertResult::SUCCESSFUL_INSERT;
            }
            if (status.explicit_()) {
              return InsertResult::NOT_NULL;
            }
            if (status.conflict() && status.retry()) {
              if (old_node->m_pNext.load(MemoryModel::memory_order_relaxed).ptr() != nullptr) {
                return InsertResult::FAILED_INSERT;
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
