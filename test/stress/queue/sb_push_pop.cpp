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
#include <cds_test/check_baskets.h>

#include <boost/optional.hpp>

namespace cds_test {

    template <typename Counter>
    static inline property_stream& operator <<( property_stream& o, cds::container::wf_queue::stat<Counter> const& s )
    {
        return o
            << CDSSTRESS_STAT_OUT( s, m_EnqueueCount )
            << CDSSTRESS_STAT_OUT( s, m_EnqueueRace )
            << CDSSTRESS_STAT_OUT( s, m_DequeueCount )
            << CDSSTRESS_STAT_OUT( s, m_EmptyDequeue )
            << CDSSTRESS_STAT_OUT( s, m_DequeueRace )
            << CDSSTRESS_STAT_OUT( s, m_AdvanceTailError )
            << CDSSTRESS_STAT_OUT( s, m_BadTail )
            << CDSSTRESS_STAT_OUT( s, m_TryAddBasket )
            << CDSSTRESS_STAT_OUT( s, m_AddBasketCount )
            << CDSSTRESS_STAT_OUT( s, m_NullBasket );
    }

    static inline property_stream& operator <<( property_stream& o, cds::container::wf_queue::empty_stat const& /*s*/ )
    {
        return o;
    }
}

// Multi-threaded queue push/pop test
namespace {

    using cds_test::utils::topology::Topology;
    static boost::optional<Topology> s_Topology;
    static size_t s_nConsumerThreadCount = 4;
    static size_t s_nProducerThreadCount = 4;
    static size_t s_nQueueSize = 4000000;
    static size_t s_nHeavyValueSize = 100;

    static std::atomic<size_t> s_nProducerDone( 0 );

    struct old_value
    {
        size_t nNo;
        size_t nWriterNo;
    };

    template<class Value = old_value>
    class sb_queue_push_pop: public cds_test::stress_fixture
    {
    public:
       using value_type = Value;
       using gc_type = cds::gc::HP;
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
                , m_nPushFailed( 0 )
                , m_nPushCount( nPushCount )
            {}

            Producer( Producer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_nPushFailed( 0 )
                , m_nPushCount( src.m_nPushCount )
            {}

            virtual thread * clone()
            {
                return new Producer( *this );
            }

            virtual void test()
            {
                auto id_ = id();
                size_t const nPushCount = m_nPushCount;
                s_Topology->pin_thread(id_);
                value_type v;
                v.nWriterNo = id();
                v.nNo = 0;
                m_nPushFailed = 0;

                while ( v.nNo < nPushCount ) {
                    if ( m_Queue.push( v, id_ ))
                        ++v.nNo;
                    else
                        ++m_nPushFailed;
                }

                s_nProducerDone.fetch_add( 1 );
                s_Topology->verify_pin(id_);
            }

        public:
            Queue&              m_Queue;
            size_t              m_nPushFailed;
            size_t const        m_nPushCount;
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

            typedef std::vector<size_t> popped_data;
            typedef std::vector<size_t>::iterator       data_iterator;
            typedef std::vector<size_t>::const_iterator const_data_iterator;

            std::vector<popped_data>        m_WriterData;

        private:
            void initPoppedData()
            {
                const size_t nProducerCount = s_nProducerThreadCount;
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
                auto id_ = id();
                s_Topology->pin_thread(id_);
                m_nPopEmpty = 0;
                m_nPopped = 0;
                m_nBadWriter = 0;
                const size_t nTotalWriters = s_nProducerThreadCount;
                value_type v;
                bool writers_done = false;
                while ( true ) {
                    if ( m_Queue.pop( v, id_ )) {
                        ++m_nPopped;
                        if ( v.nWriterNo < nTotalWriters )
                            m_WriterData[ v.nWriterNo ].push_back( v.nNo );
                        else
                            ++m_nBadWriter;
                    }
                    else {
                        ++m_nPopEmpty;
                        if (writers_done) {
                          break;
                        }
                        writers_done = s_nProducerDone.load() >= nTotalWriters;
                    }
                }
                s_Topology->verify_pin(id_);
            }
        };

    protected:
        size_t m_nThreadPushCount;

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
                if ( thr.type() == consumer_thread ) {
                    consumer_type& consumer = static_cast<consumer_type&>( thr );
                    nTotalPops += consumer.m_nPopped;
                    nPopFalse += consumer.m_nPopEmpty;
                    arrConsumer.push_back( &consumer );
                    EXPECT_EQ( consumer.m_nBadWriter, 0u ) << "consumer_thread_no " << i;

                    size_t nPopped = 0;
                    for ( size_t n = 0; n < s_nProducerThreadCount; ++n )
                        nPopped += consumer.m_WriterData[n].size();

                    nPoppedItems += nPopped;
                }
                else {
                    assert( thr.type() == producer_thread );

                    producer_type& producer = static_cast<producer_type&>( thr );
                    nPushFailed += producer.m_nPushFailed;
                    EXPECT_EQ( producer.m_nPushFailed, 0u ) << "producer_thread_no " << i;
                }
            }
            EXPECT_EQ( nTotalPops, nPoppedItems );

            EXPECT_EQ( nTotalPops + nPostTestPops, s_nQueueSize ) << "nTotalPops=" << nTotalPops << ", nPostTestPops=" << nPostTestPops;
            value_type v;
            EXPECT_FALSE( q.pop(v, 0));

            // Test consistency of popped sequence
            for ( size_t nWriter = 0; nWriter < s_nProducerThreadCount; ++nWriter ) {
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

                    for ( it = arrConsumer[nReader]->m_WriterData[nWriter].begin(); it != itEnd; ++it )
                        arrData.push_back( *it );
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
        void test_queue( Queue& q )
        {
            m_nThreadPushCount = s_nQueueSize / s_nProducerThreadCount;
            s_nQueueSize = m_nThreadPushCount * s_nProducerThreadCount;

            cds_test::thread_pool& pool = get_pool();
            pool.add( new Producer<Queue>( pool, q, m_nThreadPushCount ), s_nProducerThreadCount );
            pool.add( new Consumer<Queue>( pool, q, m_nThreadPushCount ), s_nConsumerThreadCount );

            s_nProducerDone.store( 0 );

            propout() << std::make_pair( "producer_count", s_nProducerThreadCount )
                << std::make_pair( "consumer_count", s_nConsumerThreadCount )
                << std::make_pair( "push_count", s_nQueueSize );

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair( "duration", duration );
        }

        template <class Queue>
        void test( Queue& q )
        {
            test_queue( q );
            analyze( q );
            propout() << q.statistics();
        }

    private:
        static void set_array_size( size_t size ) {
            const bool tmp = fc_test::has_set_array_size<value_type>::value;
            set_array_size(size, std::integral_constant<bool, tmp>());
        }

        static void set_array_size(size_t size, std::true_type){
            value_type::set_array_size(size);
        }

        static void set_array_size(size_t, std::false_type)
        {
        }

    public:
        static void SetUpTestCase()
        {
            cds_test::config const& cfg = get_config( "queue_push_pop" );

            s_nConsumerThreadCount = cfg.get_size_t( "ConsumerCount", s_nConsumerThreadCount );
            s_nProducerThreadCount = cfg.get_size_t( "ProducerCount", s_nProducerThreadCount );
            s_nQueueSize = cfg.get_size_t( "QueueSize", s_nQueueSize );
            s_nHeavyValueSize = cfg.get_size_t( "HeavyValueSize", s_nHeavyValueSize );

            if ( s_nConsumerThreadCount == 0u )
                s_nConsumerThreadCount = 1;
            if ( s_nProducerThreadCount == 0u )
                s_nProducerThreadCount = 1;
            if ( s_nQueueSize == 0u )
                s_nQueueSize = 1000;
            if ( s_nHeavyValueSize == 0 )
                s_nHeavyValueSize = 1;

            std::cout << "[ STAT     ] Producer = " << s_nProducerThreadCount << std::endl;
            std::cout << "[ STAT     ] Consumer = " << s_nConsumerThreadCount << std::endl;
            std::cout << "[ STAT     ] QueueSize = " << s_nQueueSize << std::endl;

            set_array_size( s_nHeavyValueSize );

            s_Topology = Topology(s_nProducerThreadCount + s_nConsumerThreadCount);

            std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        }

        //static void TearDownTestCase();
    };

    using simple_sb_queue_push_pop = sb_queue_push_pop<>;

#undef CDSSTRESS_Queue_F
#define CDSSTRESS_Queue_F( test_fixture, type_name ) \
    TEST_F( test_fixture, type_name ) \
    { \
        typedef type_name<test_fixture> queue_type; \
        queue_type queue( s_nConsumerThreadCount + s_nProducerThreadCount ); \
        test( queue ); \
    }

    template <class Fixture>
    using WFQueue = cds::container::WFQueue<typename Fixture::gc_type, typename Fixture::value_type>;

    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, WFQueue )

    using namespace cds::container::bags;

    // template <class Fixture>
    // using SBStackBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, StackBag>;
    // CDSSTRESS_Queue_F( simple_sb_queue_push_pop, SBStackBasketQueue_HP )

    template <class Fixture>
    using SBSimpleBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, SimpleBag>;
    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, SBSimpleBasketQueue_HP )

#undef CDSSTRESS_Queue_F


} // namespace
