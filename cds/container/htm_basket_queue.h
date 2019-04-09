// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_HTM_BASKET_QUEUE_H
#define CDSLIB_CONTAINER_HTM_BASKET_QUEUE_H

#include <cds/intrusive/htm_basket_queue.h>
#include <cds/container/basket_queue.h>
#include <cds/container/details/base.h>
#include <memory>

namespace cds { namespace container {

    namespace ci = cds::intrusive;

    /// HTMBasketQueue related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace htm_basket_queue {

        /// HTMBasketQueue default type traits
        struct traits : ci::opt::insert_policy<ci::htm_basket_queue::htm_insert<>>::template pack<basket_queue::traits> {};

    } // namespace htm_basket_queue

    //@cond

    template <typename GC, typename T, typename Traits = htm_basket_queue::traits >
    class HTMBasketQueue: public BasketQueue<GC, T, opt::insert_policy<ci::htm_basket_queue::htm_insert<>>::template pack<Traits>> {
    private:
      typedef BasketQueue<GC, T, opt::insert_policy<ci::htm_basket_queue::htm_insert<>>::template pack<Traits>> base_type;
    public:
      using base_type::base_type;
      static_assert(base_type::insert_policy::IS_HTM, "Must use htm_insert");
    };

}}  // namespace cds::container

#endif  // #ifndef CDSLIB_CONTAINER_HTM_BASKET_QUEUE_H
