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
        template <class MemoryModel, class Atomic, class OldValue, class NewValue>
        static bool _(Atomic& var, OldValue& old, NewValue&& new_) {
          auto transaction = [&] {
              if (var.load(MemoryModel::memory_order_relaxed) != old) {
                  sync::abort<0xff>();
              }
              var.store(std::forward<NewValue>(new_), MemoryModel::memory_order_relaxed);
          };
          sync::htm_status status;
          do {
            status = sync::htm(transaction);
            if (status) {
              return true;
            }
          } while (!status.explicit_() && !(status.conflict() && !status.retry()));
          old = var.load(MemoryModel::memory_order_relaxed);
          return false;
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
