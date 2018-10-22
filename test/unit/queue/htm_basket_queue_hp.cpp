// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "test_generic_queue.h"

#include <cds/gc/hp.h>
#include <cds/container/htm_basket_queue.h>

namespace {
    namespace cc = cds::container;
    typedef cds::gc::HP gc_type;


    class HTMBasketQueue_HP : public cds_test::generic_queue
    {
    protected:
        void SetUp()
        {
            typedef cc::HTMBasketQueue< gc_type, int > queue_type;

            cds::gc::hp::GarbageCollector::Construct( queue_type::c_nHazardPtrCount, 1, 16 );
            cds::threading::Manager::attachThread();
        }

        void TearDown()
        {
            cds::threading::Manager::detachThread();
            cds::gc::hp::GarbageCollector::Destruct( true );
        }
    };

#ifdef CDS_HTM_SUPPORT
    TEST_F( HTMBasketQueue_HP, defaulted )
    {
        typedef cds::container::HTMBasketQueue< gc_type, int > test_queue;

        test_queue q;
        test(q);
    }

    TEST_F( HTMBasketQueue_HP, item_counting )
    {
        typedef cds::container::HTMBasketQueue < gc_type, int,
            typename cds::container::htm_basket_queue::make_traits <
                cds::opt::item_counter < cds::atomicity::item_counter >
            > ::type
        > test_queue;

        test_queue q;
        test( q );
    }

    TEST_F( HTMBasketQueue_HP, relaxed )
    {
        typedef cds::container::HTMBasketQueue < gc_type, int,
            typename cds::container::htm_basket_queue::make_traits <
                cds::opt::item_counter< cds::atomicity::item_counter >
                , cds::opt::memory_model < cds::opt::v::relaxed_ordering >
            > ::type
        > test_queue;

        test_queue q;
        test( q );
    }

    TEST_F( HTMBasketQueue_HP, aligned )
    {
        typedef cds::container::HTMBasketQueue < gc_type, int,
            typename cds::container::htm_basket_queue::make_traits <
                cds::opt::memory_model< cds::opt::v::relaxed_ordering>
                , cds::opt::padding < 32 >
            >::type
        > test_queue;

        test_queue q;
        test( q );
    }

    TEST_F( HTMBasketQueue_HP, seq_cst )
    {
        struct traits : public cc::htm_basket_queue::traits
        {
            typedef cds::opt::v::sequential_consistent memory_model;
            typedef cds::atomicity::item_counter item_counter;
            enum { padding = 64 };
        };
        typedef cds::container::HTMBasketQueue < gc_type, int, traits > test_queue;

        test_queue q;
        test( q );
    }

    TEST_F( HTMBasketQueue_HP, move )
    {
        typedef cds::container::HTMBasketQueue< gc_type, std::string > test_queue;

        test_queue q;
        test_string( q );
    }

    TEST_F( HTMBasketQueue_HP, move_item_counting )
    {
        struct traits : public cc::htm_basket_queue::traits
        {
            typedef cds::atomicity::item_counter item_counter;
        };
        typedef cds::container::HTMBasketQueue< gc_type, std::string, traits > test_queue;

        test_queue q;
        test_string( q );
    }
#endif // CDS_HTM_SUPPORT

} // namespace

