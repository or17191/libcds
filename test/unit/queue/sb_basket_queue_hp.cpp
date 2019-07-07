// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)


#include <cds_test/check_size.h>

#include <cds/gc/hp.h>
#include <cds/algo/uuid.h>
#include <cds/container/sb_basket_queue.h>
#include <cds/container/bags.h>

namespace {
    namespace cc = cds::container;
    typedef cds::gc::HP gc_type;


    class SBBasketQueue_HP : public ::testing::Test
    {
    protected:
        template <typename Queue>
        void test( Queue& q )
        {
            const size_t nSize = 100;
            typedef typename Queue::value_type value_type;
            std::vector<value_type> vec(nSize);
            std::iota(vec.begin(), vec.end(), 0);

            cds::uuid_type basket;

            ASSERT_CONTAINER_SIZE( q, 0 );

            // enqueue/dequeue
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.enqueue( &vec[i], 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                value_type* it = nullptr;
                ASSERT_TRUE( q.dequeue( it, 0, &basket ));
                ASSERT_NE(nullptr, it);
                ASSERT_EQ( *it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_CONTAINER_SIZE( q, 0 );

            // push/pop
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( &vec[i], 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                value_type* it = nullptr;
                ASSERT_TRUE( q.pop( it , 0, &basket));
                ASSERT_NE(nullptr, it);
                ASSERT_EQ( *it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_CONTAINER_SIZE( q, 0 );

            // clear
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( &vec[i], 0));
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            q.clear();
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );

            // pop from empty queue
            value_type* it = nullptr;
            ASSERT_FALSE( q.pop( it, 0 ));
            ASSERT_EQ( nullptr, it);
            ASSERT_CONTAINER_SIZE( q, 0 );

            ASSERT_FALSE( q.dequeue( it, 0 ));
            ASSERT_EQ( nullptr, it);
            ASSERT_CONTAINER_SIZE( q, 0 );
        }

        template <class T>
        using bag_t = cds::container::bags::IdBag<T>;

        void SetUp()
        {
            typedef cc::SBBasketQueue< gc_type, int , bag_t> queue_type;

            cds::gc::hp::GarbageCollector::Construct( queue_type::c_nHazardPtrCount, 1, 16 );
            cds::threading::Manager::attachThread();
        }

        void TearDown()
        {
            cds::threading::Manager::detachThread();
            cds::gc::hp::GarbageCollector::Destruct( true );
        }
    };


    TEST_F( SBBasketQueue_HP, defaulted )
    {
        typedef cds::container::SBBasketQueue< gc_type, int, bag_t> test_queue;

        test_queue q(1);
        test(q);
    }

    TEST_F( SBBasketQueue_HP, item_counting )
    {
        typedef cds::container::SBBasketQueue < gc_type, int, bag_t,
            typename cds::container::sb_basket_queue::make_traits <
                cds::opt::item_counter < cds::atomicity::item_counter >
            > ::type
        > test_queue;

        test_queue q(1);
        test( q );
    }

    TEST_F( SBBasketQueue_HP, relaxed )
    {
        typedef cds::container::SBBasketQueue < gc_type, int, bag_t,
            typename cds::container::sb_basket_queue::make_traits <
                cds::opt::item_counter< cds::atomicity::item_counter >
                , cds::opt::memory_model < cds::opt::v::relaxed_ordering >
            > ::type
        > test_queue;

        test_queue q(1);
        test( q );
    }

    TEST_F( SBBasketQueue_HP, aligned )
    {
        typedef cds::container::SBBasketQueue < gc_type, int, bag_t,
            typename cds::container::sb_basket_queue::make_traits <
                cds::opt::memory_model< cds::opt::v::relaxed_ordering>
                , cds::opt::padding < 32 >
            >::type
        > test_queue;

        test_queue q(1);
        test( q );
    }

    TEST_F( SBBasketQueue_HP, seq_cst )
    {
        struct traits : public cc::sb_basket_queue::traits
        {
            typedef cds::opt::v::sequential_consistent memory_model;
            typedef cds::atomicity::item_counter item_counter;
            enum { padding = 64 };
        };
        typedef cds::container::SBBasketQueue < gc_type, int, bag_t, traits > test_queue;

        test_queue q(1);
        test( q );
    }

} // namespace

