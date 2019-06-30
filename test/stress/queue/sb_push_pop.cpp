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

// Multi-threaded queue push/pop test
namespace {

    using cds_test::utils::topology::Topology;
    static boost::optional<Topology> s_Topology;
    static size_t s_nConsumerThreadCount = 4;
    static size_t s_nProducerThreadCount = 4;
    static size_t s_nQueueSize = 4000000;
    static size_t s_nPreStoreSize = 0;
    static size_t s_nHeavyValueSize = 100;

    static std::atomic<size_t> s_nProducerDone( 0 );
    static bool s_nPreStoreDone( false );

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
    class sb_queue_push_pop: public cds_test::stress_fixture
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
            Producer( cds_test::thread_pool& pool, Queue& queue, size_t nStart, size_t nEnd )
                : base_class( pool, producer_thread )
                , m_Queue( queue )
                , m_nPushFailed( 0 )
                , m_nStart( nStart )
                , m_nEnd( nEnd )
            {}

            Producer( Producer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_nPushFailed( 0 )
                , m_nStart( src.m_nStart )
                , m_nEnd( src.m_nEnd )
            {}

            virtual thread * clone()
            {
                return new Producer( *this );
            }

            virtual void test()
            {
                s_Topology->pin_thread(id());
                auto start = clock_type::now();
                m_nPushFailed += push_many(m_Queue, m_nStart, m_nEnd, m_nWriterId);
                auto end = clock_type::now();
                m_Duration = std::chrono::duration_cast<duration_type>(end - start);
                s_nProducerDone.fetch_add( 1 );
                s_Topology->verify_pin(id());
            }

        public:
            Queue&              m_Queue;
            size_t              m_nPushFailed;
            size_t const        m_nStart;
            size_t const        m_nEnd;
            size_t              m_nWriterId;
            duration_type       m_Duration;
        };

        template <class Queue, class HasBasket>
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

            typedef std::vector<std::pair<size_t, size_t>> popped_data;
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

            bool pop(value_type& value, size_t tid, cds::uuid_type& basket, std::true_type) {
              return m_Queue.pop(value, tid, std::addressof(basket));
            }

            bool pop(value_type& value, size_t tid, cds::uuid_type& basket, std::false_type) {
              basket = 0;
              return m_Queue.pop(value, tid);
            }

            virtual void test()
            {
                s_Topology->pin_thread(id());
                m_nPopEmpty = 0;
                m_nPopped = 0;
                m_nBadWriter = 0;
                const size_t nTotalWriters = s_nProducerThreadCount;
                value_type v;
                bool writers_done = false;
                size_t basket;
                auto start = clock_type::now();
                while ( true ) {
                    if (pop(v, m_nReaderId, basket, HasBasket{})) {
                        ++m_nPopped;
                        if ( v.nWriterNo < nTotalWriters )
                            m_WriterData[ v.nWriterNo ].emplace_back( v.nNo, basket );
                        else
                            ++m_nBadWriter;
                    }
                    else {
                        ++m_nPopEmpty;
                        writers_done = s_nProducerDone.load() >= nTotalWriters;
                        if (writers_done) {
                          break;
                        }
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
        template <class It>
        void check_baskets(It first, It last, std::true_type) {
          using value_type = decltype(*first);
          auto checker = cds_test::BasketsChecker::make(first, last, [](const size_t& e) { return e; });
          EXPECT_EQ(0, checker.null_basket_count);
          EXPECT_GE(s_nProducerThreadCount, checker.distribution.rbegin()->first) << " allow at most one element per thread in each basket";
          propout() << std::make_pair("basket_distribution", checker.distribution_str());
          auto mean_std = checker.mean_std();
          propout() << std::make_pair("basket_mean", mean_std.first);
          propout() << std::make_pair("basket_std", mean_std.second);
        }

        template <class It>
        void check_baskets(It first, It last, std::false_type) {
        }

        template <class Queue, class HasBaskets>
        void analyze( Queue& q, HasBaskets, size_t /*nLeftOffset*/ = 0, size_t nRightOffset = 0 )
        {
            cds_test::thread_pool& pool = get_pool();

            typedef Consumer<Queue, HasBaskets> consumer_type;
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
            std::vector<size_t> baskets;

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

            propout() << std::make_pair("empty_pops", nPopFalse);

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
                            EXPECT_LT( itPrev->first, it->first + nRightOffset ) << "consumer=" << nReader << ", producer=" << nWriter;
                            itPrev = it;
                        }
                    }

                    for ( it = arrConsumer[nReader]->m_WriterData[nWriter].begin(); it != itEnd; ++it ) {
                        arrData.push_back( it->first );
                        baskets.push_back( it->second );
                    }
                }

                std::sort( arrData.begin(), arrData.end());
                for ( size_t i=1; i < arrData.size(); ++i ) {
                    EXPECT_EQ( arrData[i - 1] + 1, arrData[i] ) << "producer=" << nWriter;
                }

                EXPECT_EQ( arrData[0], 0u ) << "producer=" << nWriter;
                EXPECT_EQ( arrData[arrData.size() - 1], m_nThreadPushCount - 1 ) << "producer=" << nWriter;
            }

            check_baskets(baskets.begin(), baskets.end(), HasBaskets{});
        }

        template<class Producer, class Consumer>
        static void setup_pool_ids(cds_test::thread_pool& pool, bool independent_ids) {
          size_t writer_id = 0;
          size_t reader_id = 0;
          for ( size_t i = 0; i < pool.size(); ++i ) {
              cds_test::thread& thr = pool.get(i);
              if ( thr.type() == consumer_thread ) {
                  Consumer& consumer = static_cast<Consumer&>( thr );
                  if(independent_ids) {
                    consumer.m_nReaderId = reader_id++;
                  } else {
                    consumer.m_nReaderId = writer_id++;
                  }
              }
              else {
                  assert( thr.type() == producer_thread );
                  Producer& producer = static_cast<Producer&>( thr );
                  producer.m_nWriterId = writer_id++;
              }
          }
        }

        template <class Queue, class HasBaskets>
        void test_queue( Queue& q, bool independent_ids, bool sequential_pre_store, HasBaskets )
        {
            typedef Consumer<Queue, HasBaskets> consumer_type;
            typedef Producer<Queue> producer_type;

            m_nThreadPushCount = s_nQueueSize / s_nProducerThreadCount;
            s_nQueueSize = m_nThreadPushCount * s_nProducerThreadCount;

            m_nThreadPreStoreSize = s_nPreStoreSize / s_nProducerThreadCount;
            s_nPreStoreSize = m_nThreadPreStoreSize * s_nProducerThreadCount;

            cds_test::thread_pool& pool = get_pool();

            if (sequential_pre_store) {
              std::cout << "[ STAT     ] Sequential pre store" << std::endl;
              for(size_t i = 0; i < s_nProducerThreadCount; ++i) {
                size_t failed = push_many(q, 0, m_nThreadPreStoreSize, i);
                EXPECT_EQ(0, failed);
              }
            } else {
              std::cout << "[ STAT     ] Parallel pre store" << std::endl;
              pool.add( new producer_type( pool, q, 0 , m_nThreadPreStoreSize), s_nProducerThreadCount );
              setup_pool_ids<producer_type, consumer_type>(pool, independent_ids);
              pool.run();
              pool.reset();
            }
            pool.add( new producer_type( pool, q, m_nThreadPreStoreSize, m_nThreadPushCount), s_nProducerThreadCount );
            pool.add( new consumer_type( pool, q, m_nThreadPushCount ), s_nConsumerThreadCount );
            setup_pool_ids<producer_type, consumer_type>(pool, independent_ids);
            s_nPreStoreDone = true;
            s_nProducerDone.store( 0 );
            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair( "producer_count", s_nProducerThreadCount )
                << std::make_pair( "consumer_count", s_nConsumerThreadCount )
                << std::make_pair( "push_count", s_nQueueSize )
                << std::make_pair( "pre_store_count", s_nPreStoreSize );


            propout() << std::make_pair( "duration", duration );
            std::cout << "[ STAT     ] Duration = " << duration.count() << "ms" << std::endl;

            duration_type reader_duration{0};
            duration_type writer_duration{0};

            for ( size_t i = 0; i < pool.size(); ++i ) {
                cds_test::thread& thr = pool.get(i);
                if ( thr.type() == consumer_thread ) {
                    consumer_type& consumer = static_cast<consumer_type&>( thr );
                    reader_duration += consumer.m_Duration;
                }
                else {
                    assert( thr.type() == producer_thread );
                    producer_type& producer = static_cast<producer_type&>( thr );
                    writer_duration += producer.m_Duration;
                }
            }
            auto ns_reader_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(reader_duration);
            auto ns_writer_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(writer_duration);
            double ns_reader_throughput = double(ns_reader_duration.count()) / (s_nQueueSize);
            double ns_writer_throughput = double(ns_writer_duration.count()) / (s_nQueueSize - s_nPreStoreSize);

            propout() << std::make_pair( "reader_throughput_nsop", ns_reader_throughput );
            propout() << std::make_pair( "writer_throughput_nsop", ns_writer_throughput );
            std::cout << "[ STAT     ] Reader Throughput = " << ns_reader_throughput << "ns/op" << std::endl;
            std::cout << "[ STAT     ] Writer Throughput = " << ns_writer_throughput << "ns/op" << std::endl;

        }

        template <class Queue, class HasBaskets>
        void test( Queue& q, bool independent_ids, bool sequential_pre_store, HasBaskets hb)
        {
            test_queue( q , independent_ids, sequential_pre_store, hb);
            analyze( q , hb);
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
            cds_test::config const& cfg = get_config( "sb_queue_push_pop" );

            s_nConsumerThreadCount = cfg.get_size_t( "ConsumerCount", s_nConsumerThreadCount );
            s_nProducerThreadCount = cfg.get_size_t( "ProducerCount", s_nProducerThreadCount );
            s_nQueueSize = cfg.get_size_t( "QueueSize", s_nQueueSize );
            s_nPreStoreSize = cfg.get_size_t( "PreStore", s_nPreStoreSize );
            s_nHeavyValueSize = cfg.get_size_t( "HeavyValueSize", s_nHeavyValueSize );

            s_nQueueSize *= s_nProducerThreadCount;
            s_nPreStoreSize *= s_nProducerThreadCount;

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
            std::cout << "[ STAT     ] PreStoreSize = " << s_nPreStoreSize << std::endl;

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
        ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount); \
        queue_type queue( s_nConsumerThreadCount); \
        test( queue, true , false, std::false_type{}); \
    }

#define CDSSTRESS_WFQueue_F( test_fixture, type_name ) \
    TEST_F( test_fixture, type_name ) \
    { \
        typedef type_name<test_fixture> queue_type; \
        ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount); \
        queue_type queue( s_nConsumerThreadCount + s_nProducerThreadCount); \
        test( queue, false , true, std::false_type{}); \
    }

    template <class Fixture>
    using WFQueue = cds::container::WFQueue<typename Fixture::value_type>;

    CDSSTRESS_WFQueue_F( simple_sb_queue_push_pop, WFQueue)


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
    CDSSTRESS_WFQueue_F( simple_sb_queue_push_pop, WFQueue_Stat)

    using namespace cds::container::bags;

    // template <class Fixture>
    // using SBStackBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, StackBag>;
    // CDSSTRESS_Queue_F( simple_sb_queue_push_pop, SBStackBasketQueue_HP )

    struct htm_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<> insert_policy;
    };

    struct htm_id_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<50, 20, 5> insert_policy;
    };
    struct htm_mod_id_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<20, 20, 5> insert_policy;
    };

    template <class Fixture>
    using SBSimpleBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, SimpleBag>;

    template <class Fixture>
    using SBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag>;

    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, SBSimpleBasketQueue_HP )
    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, SBIdBasketQueue_HP)

    template <class Fixture>
    using HTMSBSimpleBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, SimpleBag, htm_traits>;

    template <class Fixture>
    using HTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, htm_id_traits>;

    template <class Fixture>
    using HTMSBModIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, ModIdBag, htm_mod_id_traits>;

    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, HTMSBSimpleBasketQueue_HP )
    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, HTMSBIdBasketQueue_HP)
    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, HTMSBModIdBasketQueue_HP)

    struct htm_id_stat_traits : public htm_id_traits
    {
        typedef cds::container::basket_queue::stat<> stat;
    };
    template <class Fixture>
    using HTMSBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, htm_id_stat_traits>;

    CDSSTRESS_Queue_F( simple_sb_queue_push_pop, HTMSBIdBasketQueue_HP_Stat)
#undef CDSSTRESS_Queue_F


} // namespace
