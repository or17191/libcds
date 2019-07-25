// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "queue_type.h"
#include <vector>
#include <algorithm>
#include <unordered_map>

#include <boost/optional.hpp>
#include <cds/details/memkind_allocator.h>

#include <cds/container/sb_basket_queue.h>
#include <cds/container/wf_queue.h>
#include <cds/container/sb_block_basket_queue.h>
#include <cds/container/bags.h>

#include <cds/details/system_timer.h>

#include <cds_test/check_baskets.h>
#include <cds_test/topology.h>

// Multi-threaded random queue test
namespace {
    using cds_test::utils::topology::Topology;
    static size_t s_nThreadCount = 8;
    static size_t s_nQueueSize = 20000000 ;   // no more than 20 million records
    static boost::optional<Topology> s_Topology;

    static atomics::atomic< size_t > s_nProducerCount(0);
    static size_t s_nThreadPushCount;

    struct empty {};

    class sb_queue_push : public cds_test::stress_fixture
    {
        typedef cds_test::stress_fixture base_class;
    public:
       using clock_type = std::chrono::steady_clock;
       using duration_type = std::chrono::microseconds;

    protected:

        template <class Queue>
        class Producer: public cds_test::thread
        {
            typedef cds_test::thread base_class;

        public:
            Producer( cds_test::thread_pool& pool, Queue& q, const Topology& topology, size_t count)
                : base_class(pool)
                , m_Queue( q )
                , m_Topology(topology)
                , m_count(count)
            {}
            Producer( Producer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_Topology( src.m_Topology )
                , m_count( src.m_count)
            {}

            virtual thread * clone()
            {
                return new Producer( *this );
            }

            virtual void SetUp() {
              thread::SetUp();
              const auto id_ = id();
              auto pos = values;
              for(size_t i = 1; i <= m_count; ++i, ++pos) {
                pos->first = id_;
                pos->second = i;
              }
            }

            virtual void test()
            {
                using value_type = typename Queue::value_type;
                size_t i = 1;
                const auto id_ = id();
                m_Topology.pin_thread(id_);
                auto start = clock_type::now();
                for(size_t i = 1; i <= m_count; ++i, ++values) {
                  while(!m_Queue.push(values, id_)) {
                    ++m_nPushFailed;
                  }
                }
                auto end = clock_type::now();
                m_Duration = std::chrono::duration_cast<duration_type>(end - start);
                s_nProducerCount.fetch_sub( 1, atomics::memory_order_release );
                m_Topology.verify_pin(id_);
            }

        public:
            Queue&              m_Queue;
            const Topology& m_Topology;
            size_t              m_nPushFailed = 0;
            duration_type       m_Duration;

            size_t m_count;
            typename Queue::value_type* values;
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

            s_Topology = Topology(s_nThreadCount);

            s_nQueueSize *= s_nThreadCount;

            s_nThreadPushCount = s_nQueueSize / s_nThreadCount;
            s_nQueueSize = s_nThreadPushCount * s_nThreadCount;


            std::cout << "[ STAT     ] ThreadCount = " << s_nThreadCount << std::endl;
            std::cout << "[ STAT     ] QueueSize = " << s_nQueueSize << std::endl;

            s_Topology = Topology(s_nThreadCount);

            std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        }


        //static void TearDownTestCase();

    protected:
        template <class It>
        void check_baskets(It first, It last, std::true_type) {
          using value_type = decltype(*first);
          auto checker = cds_test::BasketsChecker::make(first, last, [](const value_type& e) { return e.basket_id; });
          EXPECT_EQ(0, checker.null_basket_count);
          EXPECT_GE(s_nThreadCount, checker.distribution.rbegin()->first) << " allow at most one element per thread in each basket";
          propout() << std::make_pair("basket_distribution", checker.distribution_str());
          auto mean_std = checker.mean_std();
          propout() << std::make_pair("basket_mean", mean_std.first);
          propout() << std::make_pair("basket_std", mean_std.second);
        }

        template <class It>
        void check_baskets(It first, It last, std::false_type) {
        }

        template <class Queue, class Value>
        bool pop(Queue& q, Value& value, size_t tid, cds::uuid_type& basket, std::true_type) {
          return q.pop(value, tid, std::addressof(basket));
        }

        template <class Queue, class Value>
        bool pop(Queue& q, Value& value, size_t tid, cds::uuid_type& basket, std::false_type) {
          basket = 0;
          return q.pop(value, tid);
        }

        template <class Queue, class HasBaskets>
        void test( Queue& q, HasBaskets)
        {
            propout() << std::make_pair( "producer_count", s_nThreadCount )
                << std::make_pair( "queue_size", s_nQueueSize );

            cds::details::memkind_vector<typename Queue::value_type> storage(s_nQueueSize);

            cds_test::thread_pool& pool = get_pool();

            pool.add( new Producer<Queue>( pool, q, *s_Topology, s_nThreadPushCount), s_nThreadCount );
            auto pos = storage.data();
            for(size_t i = 0; i < pool.size(); ++i, pos += s_nThreadPushCount) {
              auto& th = static_cast<Producer<Queue>&>(pool.get(i));
              th.values = pos;
            }

            cds::details::SystemTimer timer;

            timer.start();
            std::chrono::milliseconds duration = pool.run();
            auto times = timer.stop();

            propout() << std::make_pair( "duration", duration )
               << std::make_pair( "clock_time", times.clock )
               << std::make_pair( "system_time", times.sys )
               << std::make_pair( "user_time", times.user );

            std::cout << "[ STAT     ] Duration = " << duration.count() << "ms" << std::endl;

            struct record {
              intmax_t writer_id;
              intmax_t number;
              cds::uuid_type basket_id;
            };

            std::vector<record> values;
            values.reserve(s_nQueueSize);
            int pops = 0;
            typename Queue::value_type* value = nullptr;
            cds::uuid_type basket = 0;
            while(pop(q, value, 0, basket, HasBaskets{})) {
              EXPECT_NE(nullptr, value);
              ++pops;
              values.emplace_back(record{value->first, value->second, basket});
              value = nullptr;
              basket = 0;
            }
            EXPECT_EQ(s_nQueueSize, pops);
            EXPECT_EQ(s_nQueueSize, values.size());
            EXPECT_FALSE(pop(q, value, 0, basket, HasBaskets{}));

            analyze( q, values.cbegin(), values.cend());

            propout() << q.statistics();
            check_baskets(values.begin(), values.end(), HasBaskets{});
        }

        template <class Queue, class It>
        void analyze( Queue& queue, It pValStart, It pValEnd)
        {
            size_t nThreadItems = s_nQueueSize / s_nThreadCount;
            cds_test::thread_pool& pool = get_pool();

            size_t nPushFailed = 0;

            duration_type totalDuration{0};

            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get( i );
                Producer<Queue>& producer = static_cast<Producer<Queue>&>( thr );
                nPushFailed += producer.m_nPushFailed;
                totalDuration += producer.m_Duration;
                EXPECT_EQ( producer.m_nPushFailed, 0u ) << "producer " << i;
            }
            propout() << std::make_pair( "failed_push", nPushFailed );

            auto ns_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(totalDuration);

            double ns_throughput = double(ns_duration.count()) / (s_nQueueSize);
            propout() << std::make_pair( "throughput_nsop", ns_throughput );
            std::cout << "[ STAT     ] Throughput = " << ns_throughput << "ns/op" << std::endl;

            std::vector<size_t> latest(s_nThreadCount, 0);
            for(auto it = pValStart; it != pValEnd; ++it) {
              ASSERT_NE(-1, it->writer_id) << std::distance(pValStart, it);
              ASSERT_NE(-1, it->number) << std::distance(pValStart, it);
              ASSERT_LT(it->writer_id, s_nThreadCount);
              auto& last_item = latest[it->writer_id];
              EXPECT_EQ(last_item + 1, it->number) << it->writer_id;
              last_item = it->number;
            }

        }
    };

#define CDSSTRESS_QUEUE_F( QueueType ) \
    TEST_F( sb_queue_push, QueueType ) \
    { \
        QueueType q(s_nThreadCount); \
        test( q , std::true_type{}); \
        QueueType::gc::force_dispose(); \
    }

    using namespace cds::container::bags;
    using value_type = std::pair<intmax_t, intmax_t>;
    using gc_type = cds::gc::HP;
    namespace sb_basket_queue = cds::container::sb_basket_queue;

    using sb_basket_queue::Linear;
    using sb_basket_queue::Constant;

    //using SBSimpleBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, SimpleBag>;
    struct cas_id_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::basket_queue::atomics_insert<Constant<350>> insert_policy;
      //typedef cds::intrusive::basket_queue::atomics_insert<Constant<720>> insert_policy;
      //typedef cds::intrusive::basket_queue::atomics_insert<Constant<440>> insert_policy;
    };
    using SBIdBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, IdBag, cas_id_traits>;
    //using SBStackBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, StackBag>;

    struct htm_traits : cds::container::sb_basket_queue::traits {
      typedef sb_basket_queue::htm_insert<> insert_policy;
    };

    struct htm_id_traits : cds::container::sb_basket_queue::traits {
      typedef sb_basket_queue::htm_insert<Constant<350>, Constant<30>> insert_policy;
      //typedef cds::intrusive::htm_basket_queue::htm_insert<Constant<720>, Constant<30>> insert_policy;
      //typedef cds::intrusive::htm_basket_queue::htm_insert<Constant<440>, Constant<30>> insert_policy;
      //typedef cds::intrusive::htm_basket_queue::htm_insert<Linear<10, 40>, Constant<30>> insert_policy;
    };
    struct htm_id_stat_traits : public htm_id_traits
    {
        typedef cds::container::sb_basket_queue::stat<> stat;
    };

    struct stat_traits : public cds::container::sb_basket_queue::traits 
    {
        typedef cds::container::sb_basket_queue::stat<> stat;
    };

    //using HTMSBSimpleBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, SimpleBag, htm_traits>;
    using HTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, IdBag, htm_id_traits>;
    using HTMSBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<gc_type, value_type, IdBag, htm_id_stat_traits>;
    //using HTMSBStackBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, StackBag, htm_traits>;

    //static_assert(std::is_same<HTMSBSimpleBasketQueue_HP::insert_policy, htm_traits::insert_policy>::value, "Use htm");

    using SBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<gc_type, value_type, IdBag, stat_traits>;

   // CDSSTRESS_QUEUE_F( SBSimpleBasketQueue_HP)
    CDSSTRESS_QUEUE_F( SBIdBasketQueue_HP)
    //CDSSTRESS_QUEUE_F( SBStackBasketQueue_HP)

    CDSSTRESS_QUEUE_F( SBIdBasketQueue_HP_Stat)

#ifdef CDS_HTM_SUPPORT
    //CDSSTRESS_QUEUE_F( HTMSBSimpleBasketQueue_HP)
    CDSSTRESS_QUEUE_F( HTMSBIdBasketQueue_HP)
    CDSSTRESS_QUEUE_F( HTMSBIdBasketQueue_HP_Stat)
    //CDSSTRESS_QUEUE_F( HTMSBStackBasketQueue_HP)
#endif // CDS_HTM_SUPPORT

#undef CDSSTRESS_QUEUE_F

#define CDSSTRESS_QUEUE_F( QueueType, HasBasket ) \
    TEST_F( sb_queue_push, QueueType ) \
    { \
        QueueType q(s_nThreadCount); \
        test( q , HasBasket{}); \
        QueueType::gc::force_dispose(); \
    }

    namespace wf = cds::container::wf_queue;

    using WFQueue = cds::container::WFQueue<value_type>;
    CDSSTRESS_QUEUE_F( WFQueue, std::false_type )
    //static_assert(std::is_same<WFQueue::cell_getter, wf::default_cell_getter>::value, "");

    struct stat_wf_queue : public cds::container::wf_queue::traits {
      typedef cds::container::wf_queue::full_stat<> stat_type;
    };

    using WFQueue_stat = cds::container::WFQueue<value_type, stat_wf_queue>;
    CDSSTRESS_QUEUE_F( WFQueue_stat, std::false_type )
/*
    struct stat_block_queue : public cds::container::sb_block_basket_queue::traits {
      typedef cds::container::wf_queue::stat<> stat;
    };

    using SBBlockBasketQueue = cds::container::SBBlockBasketQueue<gc_type, value_type>;
    CDSSTRESS_QUEUE_F( SBBlockBasketQueue, std::true_type )

    using SBBlockBasketQueue_stat = cds::container::SBBlockBasketQueue<gc_type, value_type, stat_block_queue>;
    CDSSTRESS_QUEUE_F( SBBlockBasketQueue_stat, std::true_type )

    struct htm_block_basket : public cds::container::sb_block_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert insert_policy;
    };

    using SBHTMBlockBasketQueue = cds::container::SBBlockBasketQueue<gc_type, value_type, htm_block_basket>;
    static_assert(std::is_same<cds::intrusive::htm_basket_queue::htm_insert, SBHTMBlockBasketQueue::insert_policy>::value, "");
    CDSSTRESS_QUEUE_F( SBHTMBlockBasketQueue , std::true_type )
*/
#undef CDSSTRESS_QUEUE_F


} // namespace
