// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
#define CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H

#include <type_traits>
#include <sstream>

#include <cds/intrusive/basket_queue.h>
#include <cds/sync/htm.h>

namespace cds { namespace intrusive {

    namespace htm_basket_queue {

      template<size_t LATENCY=10, size_t FINAL_LATENCY=50, size_t PATIENCE=10>
      struct htm_insert : basket_queue::atomics_insert<> {
        static constexpr bool IS_HTM = true;
        template <class MemoryModel, class MarkedPtr>
        static InsertResult _(MarkedPtr old_node, MarkedPtr new_node, MarkedPtr& new_value, size_t thread_count = 1) {
          new_node->m_pNext.store(MarkedPtr{}, std::memory_order_relaxed);
          auto& old = old_node->m_pNext;
          int ret, ret2;
          bool might_be_not_null = true;
          const size_t latency = thread_count * LATENCY;
          MarkedPtr pNext;
          while(true) {
            if ((ret = _xbegin()) == _XBEGIN_STARTED) {
              if(_xbegin() == _XBEGIN_STARTED) {
                pNext = old.load(std::memory_order_relaxed);
                if (pNext.ptr() != nullptr) {
                  _xabort(0x01);
                }
                _xend();
              }
              delay(latency);
              if(_xbegin() == _XBEGIN_STARTED) {
                old.store(new_node, std::memory_order_relaxed);
                _xend();
              }
              _xend();
            }
            if (ret == 0) {
              continue;
            }
            if (ret == _XBEGIN_STARTED) {
              return InsertResult::SUCCESSFUL_INSERT;
            }
            if ((ret & _XABORT_EXPLICIT) != 0) {
              if(_XABORT_CODE(ret) != 0x01) {
                std::stringstream s;
                s << "Bad abort code " << _XABORT_CODE(ret);
                throw std::logic_error(s.str());
              }
              new_value = old.load(std::memory_order_acquire);
              if (might_be_not_null) {
                return InsertResult::NOT_NULL;
              } else {
                return InsertResult::FAILED_INSERT;
              }
            }
            const bool is_conflict = (ret & _XABORT_CONFLICT) != 0;
            const bool is_nested = (ret & _XABORT_NESTED) != 0;
            if (is_conflict && !is_nested) {
              might_be_not_null = false;
              delay(FINAL_LATENCY);
              for(size_t p = 0; p < PATIENCE; ++ p) {
                auto value = old.load(std::memory_order_acquire);
                if(value.ptr() != nullptr) {
                  new_value = value;
                  return InsertResult::FAILED_INSERT;
                }
                delay(FINAL_LATENCY);
              }
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
