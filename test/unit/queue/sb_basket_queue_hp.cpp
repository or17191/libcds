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
            typedef typename Queue::value_type value_type;
            value_type it;

            const size_t nSize = 100;

            cds::uuid_type basket;

            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );

            // enqueue/dequeue
            for ( size_t i = 0; i < nSize; ++i ) {
                it = static_cast<value_type>(i);
                ASSERT_TRUE( q.enqueue( it, 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_FALSE( q.empty());
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                it = -1;
                ASSERT_TRUE( q.dequeue( it, 0, &basket ));
                ASSERT_EQ( it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );

            // push/pop
            for ( size_t i = 0; i < nSize; ++i ) {
                it = static_cast<value_type>(i);
                ASSERT_TRUE( q.push( it, 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_FALSE( q.empty());
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                it = -1;
                ASSERT_TRUE( q.pop( it , 0, &basket));
                ASSERT_EQ( it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );

            // clear
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( static_cast<value_type>(i), 0));
            }
            ASSERT_FALSE( q.empty());
            ASSERT_CONTAINER_SIZE( q, nSize );

            q.clear();
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );

            // pop from empty queue
            it = nSize * 2;
            ASSERT_FALSE( q.pop( it, 0 ));
            ASSERT_EQ( it, static_cast<value_type>( nSize * 2 ));
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );

            ASSERT_FALSE( q.dequeue( it, 0 ));
            ASSERT_EQ( it, static_cast<value_type>( nSize * 2 ));
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );
        }

        template <class Queue>
        void test_string( Queue& q )
        {
            std::string str[3];
            str[0] = "one";
            str[1] = "two";
            str[2] = "three";
            const size_t nSize = sizeof( str ) / sizeof( str[0] );

            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( str[i].c_str(), 0));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_FALSE( q.empty());
            ASSERT_CONTAINER_SIZE( q, nSize );

            {
                std::string s;
                for ( size_t i = 0; i < nSize; ++i ) {
                    if ( i & 1 )
                        ASSERT_TRUE( q.pop( s, 0 ));
                    else
                        ASSERT_TRUE( q.dequeue( s, 0 ));

                    ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
                    ASSERT_EQ( s, str[i] );
                }
            }
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );


            // move push
            for ( size_t i = 0; i < nSize; ++i ) {
                std::string s = str[i];
                ASSERT_FALSE( s.empty());
                if ( i & 1 )
                    ASSERT_TRUE( q.enqueue( std::move( s ), 0));
                else
                    ASSERT_TRUE( q.push( std::move( s ), 0));
                ASSERT_TRUE( s.empty());
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_FALSE( q.empty());
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                std::string s;
                ASSERT_TRUE( q.pop( s, 0 ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
                ASSERT_EQ( s, str[i] );
            }
            ASSERT_TRUE( q.empty());
            ASSERT_CONTAINER_SIZE( q, 0 );
        }

        template <class T>
        using bag_t = cds::container::bags::SimpleBag<T>;

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
            typename cds::container::basket_queue::make_traits <
                cds::opt::item_counter < cds::atomicity::item_counter >
            > ::type
        > test_queue;

        test_queue q(1);
        test( q );
    }

    TEST_F( SBBasketQueue_HP, relaxed )
    {
        typedef cds::container::SBBasketQueue < gc_type, int, bag_t,
            typename cds::container::basket_queue::make_traits <
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
            typename cds::container::basket_queue::make_traits <
                cds::opt::memory_model< cds::opt::v::relaxed_ordering>
                , cds::opt::padding < 32 >
            >::type
        > test_queue;

        test_queue q(1);
        test( q );
    }

    TEST_F( SBBasketQueue_HP, seq_cst )
    {
        struct traits : public cc::basket_queue::traits
        {
            typedef cds::opt::v::sequential_consistent memory_model;
            typedef cds::atomicity::item_counter item_counter;
            enum { padding = 64 };
        };
        typedef cds::container::SBBasketQueue < gc_type, int, bag_t, traits > test_queue;

        test_queue q(1);
        test( q );
    }

    TEST_F( SBBasketQueue_HP, move )
    {
        typedef cds::container::SBBasketQueue< gc_type, std::string, bag_t> test_queue;

        test_queue q(1);
        test_string( q );
    }

    TEST_F( SBBasketQueue_HP, move_item_counting )
    {
        struct traits : public cc::basket_queue::traits
        {
            typedef cds::atomicity::item_counter item_counter;
        };
        typedef cds::container::SBBasketQueue< gc_type, std::string, bag_t, traits > test_queue;

        test_queue q(1);
        test_string( q );
    }

} // namespace

