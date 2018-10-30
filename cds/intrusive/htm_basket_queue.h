// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
#define CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H

#include <type_traits>

#include <cds/details/marked_ptr.h>
#include <cds/intrusive/basket_queue.h>
#include <cds/intrusive/details/single_link_struct.h>
#include <cds/sync/htm.h>

namespace cds { namespace intrusive {

    namespace htm_basket_queue {
      struct htm_insert {
        template <class MemoryModel, class Atomic, class Value>
        static bool _(Atomic& var, Value& old, Value new_) {
          auto transaction = [&] {
              if (var.load(MemoryModel::memory_order_relaxed) != old) {
                  sync::abort(0xff);
              }
              var.store(new_, MemoryModel::memory_order_relaxed);
          };
          return static_cast<bool>(sync::htm(transaction));
        }
      };

      struct traits : public cds::intrusive::basket_queue::traits {
        typedef htm_insert insert_policy;
      }; 
    }

    template <typename GC, typename T, typename Traits = htm_basket_queue::traits>
    class HTMBasketQueue: public BasketQueue<GC, T, Traits> {
    private:
      typedef BasketQueue<GC, T, Traits> base_type;
    public:
      using base_type::base_type;
    };

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
