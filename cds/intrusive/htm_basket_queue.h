// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
#define CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H

#include <type_traits>
#include <sstream>
#include <iostream>

#include <cds/intrusive/basket_queue.h>
#include <cds/sync/htm.h>

namespace cds { namespace intrusive {

    namespace htm_basket_queue {

      using basket_queue::Linear;
      using basket_queue::Constant;

      template<class Latency=Linear<10>, class FinalLatency=Constant<30>>
      struct htm_insert : basket_queue::atomics_insert<> {
        static constexpr bool IS_HTM = true;

        size_t m_latency;
        size_t m_final_latency;

        htm_insert(size_t threads=1) :
             m_latency(Latency{}(threads)),
             m_final_latency(FinalLatency{}(threads)) {}

        template <class MemoryModel, class MarkedPtr>
        InsertResult _(MarkedPtr old_node, MarkedPtr new_node, MarkedPtr& new_value) const {
          auto& old = old_node->m_pNext;
          int ret;
          MarkedPtr pNext;
          while(true) {
            if ((ret = _xbegin()) == _XBEGIN_STARTED) {
              if(_xbegin() == _XBEGIN_STARTED) {
                pNext = old.load(std::memory_order_relaxed);
                if (pNext.ptr() != nullptr) {
                  _xabort(0x01);
                }
                delay(m_latency);
                _xend();
              }
              old.store(new_node, std::memory_order_relaxed);
              _xend();
              return InsertResult::SUCCESSFUL_INSERT;
            }
            if (ret == 0) {
              // Relatively uncommon
              continue;
            }
            if ((ret & _XABORT_EXPLICIT) != 0) {
              assert(_XABORT_CODE(ret) == 0x01);
              new_value = old.load(std::memory_order_acquire);
              return InsertResult::FAILED_INSERT;
            }
            const bool is_conflict = (ret & _XABORT_CONFLICT) != 0;
            const bool is_nested = (ret & _XABORT_NESTED) != 0;
            if (is_conflict && is_nested) {
              delay(m_final_latency);
              auto value = old.load(std::memory_order_acquire);
              if(value.ptr() != nullptr) {
                new_value = value;
                return InsertResult::FAILED_INSERT;
              }
              return InsertResult::RETRY;
            }
          }
          __builtin_unreachable();
        }
      };

      struct traits : opt::insert_policy<htm_insert<>>::template pack<basket_queue::traits> {};
    }

    template <typename GC, typename T, typename Traits = htm_basket_queue::traits>
    class HTMBasketQueue: public BasketQueue<GC, T, opt::insert_policy<htm_basket_queue::htm_insert<>>::template pack<Traits>> {
    private:
      typedef BasketQueue<GC, T, opt::insert_policy<htm_basket_queue::htm_insert<>>::template pack<Traits>> base_type;
    public:
      using base_type::base_type;
      static_assert(base_type::insert_policy::IS_HTM, "Must use htm_insert");
    };

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
