// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "queue_type.h"

#include <cds/container/sb_basket_queue.h>
#include <cds/container/wf_queue.h>
#include <cds/container/bags.h>

#include <vector>
#include <algorithm>
#include <type_traits>

#include <cds_test/topology.h>

#include <boost/optional.hpp>

// Multi-threaded queue pop test
namespace {

    using cds_test::utils::topology::Topology;
    static boost::optional<Topology> s_Topology;
    static size_t s_nThreadCount = 4;
    static size_t s_nQueueSize = 4000000;

    struct old_value
    {
        size_t nNo;
        size_t nWriterNo;
    };

    struct writer_compare {
      const std::less<size_t> cmp;
      bool operator()(const old_value* v, size_t writer) const { return cmp(v->nWriterNo, writer); }
      bool operator()(size_t writer, const old_value* v) const { return cmp(writer, v->nWriterNo); }
      bool operator()(const old_value* lhs, const old_value* rhs) const { return cmp(lhs->nWriterNo, rhs->nWriterNo); }
    };

    template<class Queue, class Value>
    static bool push(Queue& queue, Value&& value, size_t id,
        decltype(std::declval<Queue>().enqueue(std::forward<Value>(value), id))* = nullptr) {
      return queue.enqueue(std::forward<Value>(value), id);
    }

    template<class Queue, class Value>
    static bool push(Queue& queue, Value&& value, size_t id,
        decltype(std::declval<Queue>().enqueue(std::forward<Value>(value)))* = nullptr) {
      return queue.enqueue(std::forward<Value>(value));
    }

    template<class Queue, class Value>
    static bool pop(Queue& queue, Value&& value, size_t id,
        decltype(std::declval<Queue>().dequeue(std::forward<Value>(value), id))* = nullptr) {
      return queue.dequeue(std::forward<Value>(value), id);
    }

    template<class Queue, class Value>
    static bool pop(Queue& queue, Value&& value, size_t id,
        decltype(std::declval<Queue>().dequeue(std::forward<Value>(value)))* = nullptr) {
      return queue.dequeue(std::forward<Value>(value));
    }

    template<class T>
    using safe_add_pointer = std::add_pointer<typename std::remove_pointer<T>::type>;

    template <class Queue>
    size_t push_many(Queue& queue, const size_t first, const size_t last, const size_t producer, typename safe_add_pointer<typename Queue::value_type>::type values) {
      size_t failed = 0;
      for(size_t i = first; i < last; ++i, ++values) {
        values->nNo = i;
        values->nWriterNo = producer;
        while(!push(queue, values, producer)) {
          ++failed;
        }
      }
      return failed;
    }

    template<class Value = old_value>
    class sb_queue_pop: public cds_test::stress_fixture
    {
    public:
       using value_type = Value;
       using gc_type = cds::gc::HP;
       using clock_type = std::chrono::steady_clock;
       using duration_type = std::chrono::microseconds;
    protected:

        enum {
            producer_thread,
            consumer_thread
        };

        template <class Queue>
        class Producer: public cds_test::thread
        {
            typedef cds_test::thread base_class;

        public:
            Producer( cds_test::thread_pool& pool, Queue& queue, size_t nPushCount )
                : base_class( pool, producer_thread )
                , m_Queue( queue )
                , m_nPushCount( nPushCount )
            {}

            Producer( Producer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_nPushCount( src.m_nPushCount )
            {}

            virtual thread * clone()
            {
                return new Producer( *this );
            }

            virtual void test()
            {
                size_t const nPushCount = m_nPushCount;
                s_Topology->pin_thread(id());
                push_many(m_Queue, 0, m_nPushCount, id(), values);
                s_Topology->verify_pin(id());
            }

        public:
            Queue&              m_Queue;
            size_t const        m_nPushCount;
            size_t              m_nWriterId;
            typename safe_add_pointer<typename Queue::value_type>::type values;
        };

        template <class Queue>
        class Consumer: public cds_test::thread
        {
            typedef cds_test::thread base_class;

        public:
            Queue&              m_Queue;
            size_t const        m_nPushPerProducer;
            size_t              m_nPopEmpty;
            size_t              m_nPopped;
            size_t              m_nBadWriter;
            size_t              m_nReaderId;
            duration_type       m_Duration;

            typedef std::vector<value_type*> popped_data;
            typedef std::vector<size_t>::iterator       data_iterator;
            typedef std::vector<size_t>::const_iterator const_data_iterator;

            popped_data        m_WriterData;

        private:
            void initPoppedData()
            {
                m_WriterData.reserve( m_nPushPerProducer * s_nThreadCount );
            }

        public:
            Consumer( cds_test::thread_pool& pool, Queue& queue, size_t nPushPerProducer )
                : base_class( pool, consumer_thread )
                , m_Queue( queue )
                , m_nPushPerProducer( nPushPerProducer )
                , m_nPopEmpty( 0 )
                , m_nPopped( 0 )
                , m_nBadWriter( 0 )
            {
                initPoppedData();
            }
            Consumer( Consumer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_nPushPerProducer( src.m_nPushPerProducer )
                , m_nPopEmpty( 0 )
                , m_nPopped( 0 )
                , m_nBadWriter( 0 )
            {
                initPoppedData();
            }

            virtual thread * clone()
            {
                return new Consumer( *this );
            }

            virtual void test()
            {
                s_Topology->pin_thread(id());
                m_nPopEmpty = 0;
                m_nPopped = 0;
                m_nBadWriter = 0;
                const size_t nTotalWriters = s_nThreadCount;
                value_type* v;
                bool writers_done = false;
                auto start = clock_type::now();
                while ( true ) {
                    if (pop(m_Queue, v, m_nReaderId)) {
                        ++m_nPopped;
                        m_WriterData.emplace_back(v);
                    }
                    else {
                        ++m_nPopEmpty;
                        break;
                    }
                }
                auto end = clock_type::now();
                m_Duration = std::chrono::duration_cast<duration_type>(end - start);
                s_Topology->verify_pin(id());
            }

            virtual void TearDown() {
              thread::TearDown();
              std::stable_sort(m_WriterData.begin(), m_WriterData.end(), writer_compare{});
              auto pos = std::lower_bound(m_WriterData.begin(), m_WriterData.end(),
                  s_nThreadCount, writer_compare{});
              m_nBadWriter = std::distance(pos, m_WriterData.end());
            }
        };

    protected:
        static size_t s_nThreadPushCount;
        static size_t s_nThreadPreStoreSize;

    protected:
        template <class Queue>
        void analyze( Queue& q, size_t /*nLeftOffset*/ = 0, size_t nRightOffset = 0 )
        {
            cds_test::thread_pool& pool = get_pool();

            typedef Consumer<Queue> consumer_type;
            typedef Producer<Queue> producer_type;

            size_t nPostTestPops = 0;
            {
                value_type* v;
                while ( pop(q, v, 0))
                    ++nPostTestPops;
            }

            size_t nTotalPops = 0;
            size_t nPopFalse = 0;
            size_t nPoppedItems = 0;
            size_t nPushFailed = 0;

            std::vector< consumer_type * > arrConsumer;

            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get(i);
                ASSERT_EQ ( consumer_thread, thr.type());
                consumer_type& consumer = static_cast<consumer_type&>( thr );
                nTotalPops += consumer.m_nPopped;
                nPopFalse += consumer.m_nPopEmpty;
                arrConsumer.push_back( &consumer );
                EXPECT_EQ( consumer.m_nBadWriter, 0u ) << "consumer_thread_no " << i;
                nPoppedItems += consumer.m_WriterData.size();
            }
            EXPECT_EQ( nTotalPops, nPoppedItems );

            propout() << std::make_pair("empty_pops", nPopFalse);

            EXPECT_EQ( nTotalPops + nPostTestPops, s_nQueueSize ) << "nTotalPops=" << nTotalPops << ", nPostTestPops=" << nPostTestPops;
            value_type* v;
            EXPECT_FALSE( pop(q, v, 0));

            // Test consistency of popped sequence
            for ( size_t nWriter = 0; nWriter < s_nThreadCount; ++nWriter ) {
                std::vector<size_t> arrData;
                arrData.reserve( s_nThreadPushCount );
                for ( size_t nReader = 0; nReader < arrConsumer.size(); ++nReader ) {
                    auto& consumer = *arrConsumer[nReader];
                    auto rng = std::equal_range(consumer.m_WriterData.begin(), consumer.m_WriterData.end(),
                        nWriter, writer_compare{});
                    auto it = rng.first;
                    auto itEnd = rng.second;
                    if ( it != itEnd ) {
                        auto itPrev = it;
                        ASSERT_EQ(nWriter, (*it)->nWriterNo);
                        for ( ++it; it != itEnd; ++it ) {
                            ASSERT_LT( (*itPrev)->nNo, (*it)->nNo + nRightOffset ) << "consumer=" << nReader << ", producer=" << nWriter;
                            itPrev = it;
                        }
                    }

                    for ( it = rng.first; it != rng.second; ++it ) {
                        arrData.push_back( (*it)->nNo );
                    }
                }

                std::sort( arrData.begin(), arrData.end());
                for ( size_t i=1; i < arrData.size(); ++i ) {
                    EXPECT_EQ( arrData[i - 1] + 1, arrData[i] ) << "producer=" << nWriter;
                }

                EXPECT_EQ( arrData[0], 0u ) << "producer=" << nWriter;
                EXPECT_EQ( arrData[arrData.size() - 1], s_nThreadPushCount - 1 ) << "producer=" << nWriter;
            }
        }

        template <class Queue>
        void test_queue( Queue& q, typename safe_add_pointer<typename Queue::value_type>::type values)
        {
            typedef Consumer<Queue> consumer_type;
            typedef Producer<Queue> producer_type;

            cds_test::thread_pool& pool = get_pool();
            pool.add( new producer_type( pool, q, s_nThreadPushCount), s_nThreadCount );
            for(size_t i = 0; i < pool.size(); ++i) {
              auto& producer = static_cast<producer_type&>(pool.get(i));
              producer.values = values;
              values += s_nThreadPushCount;
            }
            pool.run();
            pool.clear();
            pool.add( new consumer_type( pool, q, s_nThreadPushCount ), s_nThreadCount );
            size_t writer_id = 0;
            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get(i);
                ASSERT_EQ ( consumer_thread, thr.type());
                consumer_type& consumer = static_cast<consumer_type&>( thr );
                consumer.m_nReaderId = writer_id++;
            }

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair( "thread_count", s_nThreadCount )
                << std::make_pair( "queue_size", s_nQueueSize );


            propout() << std::make_pair( "duration", duration );
            std::cout << "[ STAT     ] Duration = " << duration.count() << "ms" << std::endl;

            duration_type reader_duration{0};

            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get(i);
                ASSERT_EQ ( consumer_thread, thr.type());
                consumer_type& consumer = static_cast<consumer_type&>( thr );
                reader_duration += consumer.m_Duration;
            }
            auto ns_reader_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(reader_duration);

            double ns_reader_throughput = double(ns_reader_duration.count()) / (s_nQueueSize);

            propout() << std::make_pair( "reader_throughput_nsop", ns_reader_throughput );
            std::cout << "[ STAT     ] Reader Throughput = " << ns_reader_throughput << "ns/op" << std::endl;

        }

        template <class Queue>
        void test( Queue& q)
        {
            cds::details::memkind_vector<typename std::remove_pointer<typename Queue::value_type>::type> values(s_nQueueSize);
            test_queue( q, values.data());
            analyze( q );
            propout() << q.statistics();
        }

    public:
        static void SetUpTestCase()
        {
            cds_test::config const& cfg = get_config( "sb_queue_pop" );

            s_nThreadCount = cfg.get_size_t( "ThreadCount", s_nThreadCount );
            s_nQueueSize = cfg.get_size_t( "QueueSize", s_nQueueSize );

            if ( s_nThreadCount == 0u )
                s_nThreadCount = 1;

            s_nQueueSize *= s_nThreadCount;

            s_nThreadPushCount = s_nQueueSize / s_nThreadCount;
            s_nQueueSize = s_nThreadPushCount * s_nThreadCount;

            std::cout << "[ STAT     ] ThreadCount = " << s_nThreadCount << std::endl;
            std::cout << "[ STAT     ] QueueSize = " << s_nQueueSize << std::endl;

            s_Topology = Topology(s_nThreadCount);

            std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        }

        //static void TearDownTestCase();
    };

    template<class Queue>
    size_t sb_queue_pop<Queue>::s_nThreadPushCount;
    template<class Queue>
    size_t sb_queue_pop<Queue>::s_nThreadPreStoreSize;

    using simple_sb_queue_pop = sb_queue_pop<>;

#undef CDSSTRESS_Queue_F
#define CDSSTRESS_Queue_F( test_fixture, type_name ) \
    TEST_F( test_fixture, type_name ) \
    { \
        typedef type_name<test_fixture> queue_type; \
        queue_type queue( s_nThreadCount); \
        test( queue); \
    }

    template <class Fixture>
    using WFQueue = cds::container::WFQueue<typename Fixture::value_type>;

    template <class Fixture>
    using CCQueue = cds::container::CCQueue<typename Fixture::value_type>;

    CDSSTRESS_Queue_F( simple_sb_queue_pop, WFQueue)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, CCQueue)


    // template <class Fixture>
    // using BasketWFQueue = cds::container::BasketWFQueue<typename Fixture::gc_type, typename Fixture::value_type>;

    // TEST_F( simple_sb_queue_push_pop, BasketWFQueue )
    // {
    //     typedef BasketWFQueue<simple_sb_queue_push_pop> queue_type;
    //     ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount);
    //     queue_type queue( s_nConsumerThreadCount + s_nProducerThreadCount);
    //     test( queue, false );
    // }

    struct stat_wf_queue : public cds::container::wf_queue::traits {
      typedef cds::container::wf_queue::full_stat<> stat_type;
    };

    template <class Fixture>
    using WFQueue_Stat = cds::container::WFQueue<typename Fixture::value_type, stat_wf_queue>;
    CDSSTRESS_Queue_F( simple_sb_queue_pop, WFQueue_Stat)

    using namespace cds::container::bags;
    namespace sb_basket_queue = cds::container::sb_basket_queue;
    using sb_basket_queue::Linear;
    using sb_basket_queue::Constant;


    // template <class Fixture>
    // using SBStackBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, StackBag>;
    // CDSSTRESS_Queue_F( simple_sb_queue_pop, SBStackBasketQueue_HP )
    struct fast_cas_id_traits : cds::container::sb_basket_queue::traits {
      typedef sb_basket_queue::atomics_insert<Constant<350>> insert_policy;
    };
    template<class Fixture>
    using FastSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, fast_cas_id_traits>;
    struct slow_cas_id_traits : cds::container::sb_basket_queue::traits {
      typedef sb_basket_queue::atomics_insert<Constant<720>> insert_policy;
    };
    template<class Fixture>
    using SlowSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, slow_cas_id_traits>;

    struct fast_htm_id_traits : cds::container::sb_basket_queue::traits {
      typedef sb_basket_queue::htm_insert<Constant<350>, Constant<30>> insert_policy;
    };
    template<class Fixture>
    using FastHTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, fast_htm_id_traits>;
    struct slow_htm_id_traits : cds::container::sb_basket_queue::traits {
      typedef sb_basket_queue::htm_insert<Constant<720>, Constant<30>> insert_policy;
    };
    template<class Fixture>
    using SlowHTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, slow_htm_id_traits>;

    struct stat_traits : public slow_cas_id_traits
    {
        typedef cds::container::sb_basket_queue::stat<> stat;
    };
    struct htm_id_stat_traits : public slow_htm_id_traits
    {
        typedef cds::container::sb_basket_queue::stat<> stat;
    };

    template<class Fixture>
    using HTMSBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, htm_id_stat_traits>;
    template<class Fixture>
    using SBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, stat_traits>;

   // CDSSTRESS_Queue_F( SBSimpleBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, FastSBIdBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, SlowSBIdBasketQueue_HP)
    //CDSSTRESS_Queue_F( SBStackBasketQueue_HP)

    CDSSTRESS_Queue_F( simple_sb_queue_pop, SBIdBasketQueue_HP_Stat)

#ifdef CDS_HTM_SUPPORT
    //CDSSTRESS_Queue_F( HTMSBSimpleBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, FastHTMSBIdBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, SlowHTMSBIdBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, HTMSBIdBasketQueue_HP_Stat)
    //CDSSTRESS_Queue_F( HTMSBStackBasketQueue_HP)
#endif // CDS_HTM_SUPPORT

#undef CDSSTRESS_Queue_F
#define CDSSTRESS_QUEUE_F( QueueType) \
    TEST_F( simple_sb_queue_pop, QueueType ) \
    { \
        using queue_type = QueueType<simple_sb_queue_pop>;\
        queue_type q; \
        test( q ); \
        queue_type::gc::force_dispose(); \
    }
    struct vanilla_traits : cds::container::basket_queue::traits {
      using allocator = cds::details::memkind_allocator<int>;
    };
    template<class Fixture>
    using VanillaBasketQueue = cds::container::BasketQueue<typename Fixture::gc_type, typename Fixture::value_type*, vanilla_traits>;
    CDSSTRESS_QUEUE_F( VanillaBasketQueue )


} // namespace
