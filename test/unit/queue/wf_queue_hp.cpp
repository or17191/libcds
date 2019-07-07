// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <numeric>


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
            const size_t nSize = 100;
            typedef typename Queue::value_type value_type;
            std::vector<value_type> vec(nSize);
            std::iota(vec.begin(), vec.end(), 0);
            value_type* it;


            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // enqueue/dequeue
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.enqueue( &vec[i], 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                it = nullptr;
                ASSERT_TRUE( q.dequeue( it, 0 ));
                ASSERT_NE( it, nullptr);
                ASSERT_EQ( *it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // push/pop
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( &vec[i], 0 ));
                ASSERT_CONTAINER_SIZE( q, i + 1 );
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            for ( size_t i = 0; i < nSize; ++i ) {
                it = nullptr;
                ASSERT_TRUE( q.pop( it , 0));
                ASSERT_NE( it, nullptr);
                ASSERT_EQ( *it, static_cast<value_type>( i ));
                ASSERT_CONTAINER_SIZE( q, nSize - i - 1 );
            }
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // clear
            for ( size_t i = 0; i < nSize; ++i ) {
                ASSERT_TRUE( q.push( &vec[i], 0));
            }
            ASSERT_CONTAINER_SIZE( q, nSize );

            q.clear(0);
            ASSERT_TRUE( !q.pop(it, 0) );
            ASSERT_CONTAINER_SIZE( q, 0 );

            // pop from empty queue
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
} // namespace

