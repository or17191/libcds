// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "intrusive_queue_type.h"
#include <vector>
#include <algorithm>
#include <unordered_map>

#include <cds/container/sb_basket_queue.h>

#include <cds_test/check_baskets.h>

// Multi-threaded random queue test
namespace {
    static size_t s_nThreadCount = 8;
    static size_t s_nQueueSize = 20000000 ;   // no more than 20 million records

    static atomics::atomic< size_t > s_nProducerCount(0);
    static size_t s_nThreadPushCount;

    struct empty {};

    class sb_queue_push : public cds_test::stress_fixture
    {
        typedef cds_test::stress_fixture base_class;

    protected:

        template <class Queue>
        class Producer: public cds_test::thread
        {
            typedef cds_test::thread base_class;

        public:
            Producer( cds_test::thread_pool& pool, Queue& q, size_t count)
                : base_class(pool)
                , m_Queue( q )
                , m_count(count)
            {}
            Producer( Producer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_count( src.m_count)
            {}

            virtual thread * clone()
            {
                return new Producer( *this );
            }

            virtual void test()
            {
                using value_type = typename Queue::value_type;
                size_t i = 1;
                const auto id_ = id();
                while ( i <= m_count ) {
                    if ( m_Queue.push(value_type{id_, i}, id_)) {
                        ++i;
                    }
                    else
                        ++m_nPushFailed;
                }
                s_nProducerCount.fetch_sub( 1, atomics::memory_order_release );
            }

        public:
            Queue&              m_Queue;
            size_t              m_nPushFailed = 0;

            size_t m_count;
        };

    public:

        static void SetUpTestCase()
        {
            cds_test::config const& cfg = get_config( "sb_queue_push" );

            s_nThreadCount = cfg.get_size_t( "ThreadCount", s_nThreadCount );
            s_nQueueSize = cfg.get_size_t( "QueueSize", s_nQueueSize );

            if ( s_nThreadCount == 0u )
                s_nThreadCount = 1;
            if ( s_nQueueSize == 0u )
                s_nQueueSize = 1000;
        }


        //static void TearDownTestCase();

    protected:
        template <class It>
        void check_baskets(It first, It last) {
          using value_type = decltype(*first);
          auto checker = cds_test::BasketsChecker::make(first, last, [](const value_type& e) { return e.basket_id; });
          EXPECT_EQ(0, checker.null_basket_count);
          EXPECT_GE(s_nThreadCount, checker.distribution.rbegin()->first) << " allow at most one element per thread in each basket";
          propout() << std::make_pair("basket_distribution", checker.distribution_str());
          auto mean_std = checker.mean_std();
          propout() << std::make_pair("basket_mean", mean_std.first);
          propout() << std::make_pair("basket_std", mean_std.second);
        }

        template <class Queue, class HasBaskets = std::false_type>
        void test( Queue& q )
        {
            s_nThreadPushCount = s_nQueueSize / s_nThreadCount;
            s_nQueueSize = s_nThreadPushCount * s_nThreadCount;

            propout() << std::make_pair( "producer_count", s_nThreadCount )
                << std::make_pair( "queue_size", s_nQueueSize );

            cds_test::thread_pool& pool = get_pool();

            pool.add( new Producer<Queue>( pool, q, s_nThreadPushCount), s_nThreadCount );

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair( "duration", duration );

            struct record {
              size_t writer_id;
              size_t number;
              cds::uuid_type basket_id;
            };

            std::vector<record> values;
            values.reserve(s_nQueueSize);
            int pops = 0;
            std::pair<size_t, size_t> value;
            cds::uuid_type basket;
            while(q.pop(value, &basket)) {
              ++pops;
              values.emplace_back(record{value.first, value.second, basket});
            }
            values.pop_back();
            EXPECT_EQ(s_nQueueSize, pops);
            EXPECT_TRUE( q.empty());

            analyze( q, values.begin(), values.end());

            propout() << q.statistics();
            check_baskets(values.begin(), values.end());
        }

        template <class Queue, class It>
        void analyze( Queue& queue, It pValStart, It pValEnd)
        {
            size_t nThreadItems = s_nQueueSize / s_nThreadCount;
            cds_test::thread_pool& pool = get_pool();

            size_t nPushFailed = 0;

            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get( i );
                Producer<Queue>& producer = static_cast<Producer<Queue>&>( thr );
                nPushFailed += producer.m_nPushFailed;
                EXPECT_EQ( producer.m_nPushFailed, 0u ) << "producer " << i;
            }
            propout() << std::make_pair( "failed_push", nPushFailed );

            std::vector<size_t> latest(s_nThreadCount, 0);
            for(auto it = pValStart; it != pValEnd; ++it) {
              ASSERT_LT(it->writer_id, s_nThreadCount);
              auto& last_item = latest[it->writer_id];
              EXPECT_EQ(last_item + 1, it->number);
              last_item = it->number;
            }

        }
    };

#define CDSSTRESS_QUEUE_F( QueueType ) \
    TEST_F( sb_queue_push, QueueType ) \
    { \
        QueueType q(s_nThreadCount); \
        test( q ); \
        QueueType::gc::force_dispose(); \
    }

    using SBBasketQueue_HP = cds::container::SBBasketQueue<cds::gc::HP, std::pair<size_t, size_t>, cds::container::SimpleBag>;

    struct htm_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert insert_policy;
    };

    using HTMSBBasketQueue_HP = cds::container::SBBasketQueue<cds::gc::HP, std::pair<size_t, size_t>, cds::container::SimpleBag, htm_traits>;

    static_assert(std::is_same<HTMSBBasketQueue_HP::insert_policy, htm_traits::insert_policy>::value, "Use htm");

    CDSSTRESS_QUEUE_F( SBBasketQueue_HP)

#ifdef CDS_HTM_SUPPORT
    CDSSTRESS_QUEUE_F( HTMSBBasketQueue_HP)
#endif // CDS_HTM_SUPPORT

#undef CDSSTRESS_QUEUE_F

} // namespace
