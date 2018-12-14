// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "intrusive_queue_type.h"
#include <vector>
#include <algorithm>
#include <unordered_map>

#include <boost/optional.hpp>

#include <cds_test/check_baskets.h>
#include <cds_test/topology.h>

#include <cds/details/system_timer.h>

// Multi-threaded random queue test
namespace {
    using cds_test::utils::topology::Topology;
    static size_t s_nThreadCount = 8;
    static size_t s_nQueueSize = 20000000 ;   // no more than 20 million records
    static boost::optional<Topology> s_Topology;

    static atomics::atomic< size_t > s_nProducerCount(0);
    static size_t s_nThreadPushCount;

    struct empty {};

    template <typename Base = empty >
    struct value_type: public Base
    {
        size_t      nNo;
        size_t      nWriterNo;
        size_t      nPopNo;
    };

    class intrusive_queue_push : public cds_test::stress_fixture
    {
        typedef cds_test::stress_fixture base_class;

    protected:

        template <class Queue>
        class Producer: public cds_test::thread
        {
            typedef cds_test::thread base_class;

        public:
            Producer( cds_test::thread_pool& pool, Queue& q, const Topology& topology )
                : base_class(pool)
                , m_Queue( q )
                , m_Topology( topology )
            {}
            Producer( Producer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_Topology( src.m_Topology )
            {}

            virtual thread * clone()
            {
                return new Producer( *this );
            }

            virtual void test()
            {
                auto id_ = id();
                m_Topology.pin_thread(id_);
                size_t i = 0;
                for ( typename Queue::value_type * p = m_pStart; p < m_pEnd; ) {
                    p->nNo = i;
                    p->nWriterNo = id_;
                    CDS_TSAN_ANNOTATE_HAPPENS_BEFORE( &p->nWriterNo );
                    if ( m_Queue.push( *p )) {
                        ++p;
                        ++i;
                    }
                    else
                        ++m_nPushFailed;
                }
                s_nProducerCount.fetch_sub( 1, atomics::memory_order_release );
            }

        public:
            Queue&              m_Queue;
            const Topology & m_Topology;
            size_t              m_nPushFailed = 0;

            // Interval in m_arrValue
            typename Queue::value_type *       m_pStart;
            typename Queue::value_type *       m_pEnd;
        };

    public:
        template <typename T>
        class value_array
        {
            std::unique_ptr<T[]> m_pArr;
        public:
            value_array( size_t nSize )
                : m_pArr( new T[nSize] )
            {}

            T * get() const { return m_pArr.get(); }
        };

        static void SetUpTestCase()
        {
            cds_test::config const& cfg = get_config( "queue_push" );

            s_nThreadCount = cfg.get_size_t( "ThreadCount", s_nThreadCount );
            s_nQueueSize = cfg.get_size_t( "QueueSize", s_nQueueSize );

            if ( s_nThreadCount == 0u )
                s_nThreadCount = 1;
            if ( s_nQueueSize == 0u )
                s_nQueueSize = 1000;

            s_Topology = Topology(s_nThreadCount);
        }


        //static void TearDownTestCase();

    protected:
        template <class Queue, class It>
        void check_baskets(It first, It last, std::true_type) {
          auto checker = cds_test::BasketsChecker::make(first, last, [](typename Queue::value_type& n) {return Queue::node_traits::to_node_ptr(n)->m_basket_id; });
          EXPECT_EQ(0, checker.null_basket_count);
          EXPECT_GE(s_nThreadCount, checker.distribution.rbegin()->first) << " allow at most one element per thread in each basket";
          propout() << std::make_pair("basket_distribution", checker.distribution_str());
          auto mean_std = checker.mean_std();
          propout() << std::make_pair("basket_mean", mean_std.first);
          propout() << std::make_pair("basket_std", mean_std.second);
        }

        template <class Queue, class It>
        void check_baskets(It first, It last, std::false_type) {}

        template <class Queue, class HasBaskets = std::false_type>
        void test( Queue& q, value_array<typename Queue::value_type>& arrValue, size_t nLeftOffset, size_t nRightOffset, HasBaskets has_baskets = {} )
        {
            s_nThreadPushCount = s_nQueueSize / s_nThreadCount;
            s_nQueueSize = s_nThreadPushCount * s_nThreadCount;

            propout() << std::make_pair( "producer_count", s_nThreadCount )
                << std::make_pair( "queue_size", s_nQueueSize );

            cds_test::thread_pool& pool = get_pool();

            typename Queue::value_type * pValStart = arrValue.get();
            typename Queue::value_type * pValEnd = pValStart + s_nQueueSize;

            pool.add( new Producer<Queue>( pool, q, *s_Topology ), s_nThreadCount );
            {
                for ( typename Queue::value_type * it = pValStart; it != pValEnd; ++it ) {
                    it->nNo = 0;
                    it->nWriterNo = 0;
                    it->nPopNo = 0;
                }

                typename Queue::value_type * pStart = pValStart;
                for ( size_t i = 0; i < pool.size(); ++i ) {
                    Producer<Queue>& producer = static_cast<Producer<Queue>&>( pool.get( i ));
                    producer.m_pStart = pStart;
                    pStart += s_nThreadPushCount;
                    producer.m_pEnd = pStart;
                }
            }

            cds::details::SystemTimer timer;
            timer.start();
            std::chrono::milliseconds duration = pool.run();
            auto times = timer.stop();

            propout() << std::make_pair( "duration", duration ) 
              << std::make_pair("clock_time", times.clock)
              << std::make_pair("system_time", times.sys)
              << std::make_pair("user_time", times.user);

            analyze( q, pValStart, pValStart);

            propout() << q.statistics();
            check_baskets<Queue>(pValStart, pValEnd, has_baskets);
        }

        template <class Queue>
        void analyze( Queue& queue, typename Queue::value_type * pValStart, typename Queue::value_type * pValEnd)
        {
            size_t nThreadItems = s_nQueueSize / s_nThreadCount;
            cds_test::thread_pool& pool = get_pool();

            size_t nPushFailed = 0;

            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get( i );
                Producer<Queue>& producer = static_cast<Producer<Queue>&>( thr );
                nPushFailed += producer.m_nPushFailed;
                if ( !std::is_base_of<cds::bounded_container, Queue>::value ) {
                    EXPECT_EQ( producer.m_nPushFailed, 0u ) << "producer " << i;
                }
            }
            propout() << std::make_pair( "failed_push", nPushFailed );

            {
              size_t nTotalPops = 1;
              typename Queue::value_type* item = nullptr;
              while(item = queue.pop()) {
                item->nPopNo = nTotalPops++;
              }
              size_t nQueueSize = s_nThreadPushCount * s_nThreadCount;
              EXPECT_EQ( nTotalPops - 1, nQueueSize );
              EXPECT_TRUE( queue.empty());
            }

            std::vector<size_t> latest(s_nThreadCount, 0);
            for(auto it = pValStart; it != pValEnd; ++it) {
              ASSERT_LT(it->nWriterNo, s_nThreadCount);
              auto& last_item = latest[it->nWriterNo];
              EXPECT_LT(last_item, it->nPopNo);
              last_item = it->nPopNo;
            }

        }
    };

#define CDSSTRESS_QUEUE_F( QueueType, NodeType ) \
    TEST_F( intrusive_queue_push, QueueType ) \
    { \
        typedef value_type<NodeType> node_type; \
        typedef typename queue::Types< node_type >::QueueType queue_type; \
        value_array<typename queue_type::value_type> arrValue( s_nQueueSize ); \
        { \
            queue_type q; \
            test( q, arrValue, 0, 0 , std::true_type{}); \
        } \
        queue_type::gc::force_dispose(); \
    }

    CDSSTRESS_QUEUE_F( BasketQueue_HP,       cds::intrusive::basket_queue::node<cds::gc::HP> )
    CDSSTRESS_QUEUE_F( BasketQueue_HP_ic,    cds::intrusive::basket_queue::node<cds::gc::HP> )
    CDSSTRESS_QUEUE_F( BasketQueue_HP_stat,  cds::intrusive::basket_queue::node<cds::gc::HP> )
    CDSSTRESS_QUEUE_F( BasketQueue_DHP,      cds::intrusive::basket_queue::node<cds::gc::DHP> )
    CDSSTRESS_QUEUE_F( BasketQueue_DHP_ic,   cds::intrusive::basket_queue::node<cds::gc::DHP> )
    CDSSTRESS_QUEUE_F( BasketQueue_DHP_stat, cds::intrusive::basket_queue::node<cds::gc::DHP> )

#ifdef CDS_HTM_SUPPORT
    CDSSTRESS_QUEUE_F( HTMBasketQueue_HP,       cds::intrusive::basket_queue::node<cds::gc::HP> )
    CDSSTRESS_QUEUE_F( HTMBasketQueue_HP_ic,    cds::intrusive::basket_queue::node<cds::gc::HP> )
    CDSSTRESS_QUEUE_F( HTMBasketQueue_HP_stat,  cds::intrusive::basket_queue::node<cds::gc::HP> )
#endif // CDS_HTM_SUPPORT
} // namespace
