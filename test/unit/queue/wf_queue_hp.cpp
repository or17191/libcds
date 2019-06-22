// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)


#include <cds_test/check_size.h>

#include <cds/gc/hp.h>
#include <cds/container/wf_queue.h>

namespace {
    namespace cc = cds::container;
    typedef cds::gc::HP gc_type;


    class WFQueue_HP : public ::testing::Test
    {
    protected:
        template <typename Queue>
        void test( Queue& q )
        {
            typedef typename Queue::value_type value_type;
            value_type it;

            const size_t nSize = 100;

            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // enqueue/dequeue
            for ( size_t i = 0; i < nSize; ++i ) {
                it = static_cast<value_type>(i);
                ASSERT_TRUE( q.enqueue( it, 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                it = -1;
                ASSERT_TRUE( q.dequeue( it, 0 ));
                ASSERT_EQ( it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // push/pop
            for ( size_t i = 0; i < nSize; ++i ) {
                it = static_cast<value_type>(i);
                ASSERT_TRUE( q.push( it, 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                it = -1;
                ASSERT_TRUE( q.pop( it , 0));
                ASSERT_EQ( it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // clear
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( static_cast<value_type>(i), 0));
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            q.clear(0);
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // pop from empty queue
        }

        template <class Queue>
        void test_string( Queue& q )
        {
            std::string it;
            std::string str[3];
            str[0] = "one";
            str[1] = "two";
            str[2] = "three";
            const size_t nSize = sizeof( str ) / sizeof( str[0] );

            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( str[i].c_str(), 0));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
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
            ASSERT_TRUE( !q.pop(it, 0) );
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
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                std::string s;
                ASSERT_TRUE( q.pop( s, 0 ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
                ASSERT_EQ( s, str[i] );
            }
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );
        }

        void SetUp()
        {
            typedef cc::WFQueue<int> queue_type;

            cds::threading::Manager::attachThread();
        }

        void TearDown()
        {
            cds::threading::Manager::detachThread();
            cds::gc::hp::GarbageCollector::Destruct( true );
        }
    };


    TEST_F( WFQueue_HP, defaulted )
    {
        typedef cds::container::WFQueue<int> test_queue;

        test_queue q(1);
        test(q);
    }


    TEST_F( WFQueue_HP, move )
    {
        typedef cds::container::WFQueue<std::string> test_queue;

        test_queue q(1);
        test_string( q );
    }

} // namespace

