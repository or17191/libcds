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

namespace cds_test {

    template <typename Counter>
    static inline property_stream& operator <<( property_stream& o, cds::container::wf_queue::full_stat<Counter> const& s )
    {
        return o
            << CDSSTRESS_STAT_OUT( s, m_FastEnqueue )
            << CDSSTRESS_STAT_OUT( s, m_FastDequeue )
            << CDSSTRESS_STAT_OUT( s, m_SlowEnqueue )
            << CDSSTRESS_STAT_OUT( s, m_SlowDequeue )
            << CDSSTRESS_STAT_OUT( s, m_Empty );
    }

    static inline property_stream& operator <<( property_stream& o, cds::container::wf_queue::empty_stat const& /*s*/ )
    {
        return o;
    }
}

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

    template <class Queue>
    size_t push_many(Queue& queue, const size_t first, const size_t last, const size_t producer) {
      size_t failed = 0;
      old_value v;
      v.nNo = first;
      v.nWriterNo = producer;
      while(v.nNo < last) {
        if ( queue.push( v, producer )) {
            ++v.nNo;
        } else {
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
       using duration_type = std::chrono::milliseconds;
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
                push_many(m_Queue, 0, m_nPushCount, id());
                s_Topology->verify_pin(id());
            }

        public:
            Queue&              m_Queue;
            size_t const        m_nPushCount;
            size_t              m_nWriterId;
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

            typedef std::vector<size_t> popped_data;
            typedef std::vector<size_t>::iterator       data_iterator;
            typedef std::vector<size_t>::const_iterator const_data_iterator;

            std::vector<popped_data>        m_WriterData;

        private:
            void initPoppedData()
            {
                const size_t nProducerCount = s_nThreadCount;
                m_WriterData.resize( nProducerCount );
                for ( size_t i = 0; i < nProducerCount; ++i )
                    m_WriterData[i].reserve( m_nPushPerProducer );
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
                value_type v;
                bool writers_done = false;
                auto start = clock_type::now();
                while ( true ) {
                    if (m_Queue.pop(v, m_nReaderId)) {
                        ++m_nPopped;
                        if ( v.nWriterNo < nTotalWriters )
                            m_WriterData[ v.nWriterNo ].emplace_back( v.nNo );
                        else
                            ++m_nBadWriter;
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
        };

    protected:
        size_t m_nThreadPushCount;
        size_t m_nThreadPreStoreSize;

    protected:
        template <class Queue>
        void analyze( Queue& q, size_t /*nLeftOffset*/ = 0, size_t nRightOffset = 0 )
        {
            cds_test::thread_pool& pool = get_pool();

            typedef Consumer<Queue> consumer_type;
            typedef Producer<Queue> producer_type;

            size_t nPostTestPops = 0;
            {
                value_type v;
                while ( q.pop( v, 0))
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

                size_t nPopped = 0;
                for ( size_t n = 0; n < s_nThreadCount; ++n )
                    nPopped += consumer.m_WriterData[n].size();

                nPoppedItems += nPopped;
            }
            EXPECT_EQ( nTotalPops, nPoppedItems );

            propout() << std::make_pair("empty_pops", nPopFalse);

            EXPECT_EQ( nTotalPops + nPostTestPops, s_nQueueSize ) << "nTotalPops=" << nTotalPops << ", nPostTestPops=" << nPostTestPops;
            value_type v;
            EXPECT_FALSE( q.pop(v, 0));

            // Test consistency of popped sequence
            for ( size_t nWriter = 0; nWriter < s_nThreadCount; ++nWriter ) {
                std::vector<size_t> arrData;
                arrData.reserve( m_nThreadPushCount );
                for ( size_t nReader = 0; nReader < arrConsumer.size(); ++nReader ) {
                    auto it = arrConsumer[nReader]->m_WriterData[nWriter].begin();
                    auto itEnd = arrConsumer[nReader]->m_WriterData[nWriter].end();
                    if ( it != itEnd ) {
                        auto itPrev = it;
                        for ( ++it; it != itEnd; ++it ) {
                            EXPECT_LT( *itPrev, *it + nRightOffset ) << "consumer=" << nReader << ", producer=" << nWriter;
                            itPrev = it;
                        }
                    }

                    for ( it = arrConsumer[nReader]->m_WriterData[nWriter].begin(); it != itEnd; ++it ) {
                        arrData.push_back( *it );
                    }
                }

                std::sort( arrData.begin(), arrData.end());
                for ( size_t i=1; i < arrData.size(); ++i ) {
                    EXPECT_EQ( arrData[i - 1] + 1, arrData[i] ) << "producer=" << nWriter;
                }

                EXPECT_EQ( arrData[0], 0u ) << "producer=" << nWriter;
                EXPECT_EQ( arrData[arrData.size() - 1], m_nThreadPushCount - 1 ) << "producer=" << nWriter;
            }
        }

        template <class Queue>
        void test_queue( Queue& q)
        {
            typedef Consumer<Queue> consumer_type;
            typedef Producer<Queue> producer_type;

            m_nThreadPushCount = s_nQueueSize / s_nThreadCount;
            s_nQueueSize = m_nThreadPushCount * s_nThreadCount;

            cds_test::thread_pool& pool = get_pool();
            pool.add( new producer_type( pool, q, m_nThreadPushCount), s_nThreadCount );
            pool.run();
            pool.clear();
            pool.add( new consumer_type( pool, q, m_nThreadPushCount ), s_nThreadCount );
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
            double ns_reader_throughput = (reader_duration.count() * 1000.) / (s_nQueueSize);

            propout() << std::make_pair( "reader_throughput_nsop", ns_reader_throughput );
            std::cout << "[ STAT     ] Reader Throughput = " << ns_reader_throughput << "ns/op" << std::endl;

        }

        template <class Queue>
        void test( Queue& q)
        {
            test_queue( q );
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

            std::cout << "[ STAT     ] ThreadCount = " << s_nThreadCount << std::endl;
            std::cout << "[ STAT     ] QueueSize = " << s_nQueueSize << std::endl;

            s_Topology = Topology(s_nThreadCount);

            std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        }

        //static void TearDownTestCase();
    };

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

    CDSSTRESS_Queue_F( simple_sb_queue_pop, WFQueue)


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

    // template <class Fixture>
    // using SBStackBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, StackBag>;
    // CDSSTRESS_Queue_F( simple_sb_queue_pop, SBStackBasketQueue_HP )

    struct htm_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<> insert_policy;
    };

    struct htm_id_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<60, 20, 5> insert_policy;
    };
    struct htm_mod_id_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<20, 20, 5> insert_policy;
    };

    template <class Fixture>
    using SBSimpleBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, SimpleBag>;

    template <class Fixture>
    using SBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag>;

    CDSSTRESS_Queue_F( simple_sb_queue_pop, SBSimpleBasketQueue_HP )
    CDSSTRESS_Queue_F( simple_sb_queue_pop, SBIdBasketQueue_HP)

    template <class Fixture>
    using HTMSBSimpleBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, SimpleBag, htm_traits>;

    template <class Fixture>
    using HTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, htm_id_traits>;

    template <class Fixture>
    using HTMSBModIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, ModIdBag, htm_mod_id_traits>;

    CDSSTRESS_Queue_F( simple_sb_queue_pop, HTMSBSimpleBasketQueue_HP )
    CDSSTRESS_Queue_F( simple_sb_queue_pop, HTMSBIdBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_pop, HTMSBModIdBasketQueue_HP)

    struct htm_id_stat_traits : public htm_id_traits
    {
        typedef cds::container::basket_queue::stat<> stat;
    };
    template <class Fixture>
    using HTMSBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, htm_id_stat_traits>;

    CDSSTRESS_Queue_F( simple_sb_queue_pop, HTMSBIdBasketQueue_HP_Stat)
#undef CDSSTRESS_Queue_F


} // namespace
