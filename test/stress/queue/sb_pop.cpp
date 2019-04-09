// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "queue_type.h"
#include <vector>
#include <algorithm>
#include <unordered_map>

#include <boost/optional.hpp>

#include <cds/container/sb_basket_queue.h>
#include <cds/container/wf_queue.h>
#include <cds/container/sb_block_basket_queue.h>
#include <cds/container/bags.h>

#include <cds/details/system_timer.h>

#include <cds_test/check_baskets.h>
#include <cds_test/topology.h>

// Multi-threaded queue test for pop operation
namespace {

    static size_t s_nThreadCount = 8;
    static size_t s_nQueueSize = 20000000 ;   // no more than 20 million records
    using cds_test::utils::topology::Topology;
    static boost::optional<Topology> s_Topology;

        struct SimpleValue {
            size_t    nNo;

            SimpleValue(): nNo(0) {}
            SimpleValue( size_t n ): nNo(n) {}
            size_t getNo() const { return  nNo; }
        };

    class sb_queue_pop: public cds_test::stress_fixture
    {
      public:
        struct value_type
        {
            size_t      nNo;

            value_type()
                : nNo( 0 )
            {}

            value_type( size_t n )
                : nNo( n )
            {}
        };
    protected:

        template <class Queue>
        class Consumer: public cds_test::thread
        {
            typedef cds_test::thread base_class;

        public:
            Consumer( cds_test::thread_pool& pool, Queue& queue )
                : base_class( pool )
                , m_Queue( queue )
                , m_arr( new uint8_t[ s_nQueueSize ])
                , m_nPopCount( 0 )
            {}

            Consumer( Consumer& src )
                : base_class( src )
                , m_Queue( src.m_Queue )
                , m_arr( new uint8_t[ s_nQueueSize ])
                , m_nPopCount( 0 )
            {}

            virtual thread * clone()
            {
                return new Consumer( *this );
            }

            virtual void test()
            {
                auto id_ = id();
                s_Topology->pin_thread(id_);
                memset( m_arr.get(), 0, sizeof( m_arr[0] ) * s_nQueueSize );
                typedef typename Queue::value_type value_type;
                value_type value;
                size_t nPopCount = 0;
                while ( m_Queue.pop( value, id_ )) {
                    ++m_arr[ value.nNo ];
                    ++nPopCount;
                }
                m_nPopCount = nPopCount;
                s_Topology->verify_pin(id_);
            }

        public:
            Queue&              m_Queue;
            std::unique_ptr< uint8_t[] > m_arr;
            size_t              m_nPopCount;
        };

    public:
        static void SetUpTestCase()
        {
            cds_test::config const& cfg = get_config( "queue_pop" );

            s_nThreadCount = cfg.get_size_t( "ThreadCount", s_nThreadCount );
            s_nQueueSize = cfg.get_size_t( "QueueSize", s_nQueueSize );

            if ( s_nThreadCount == 0 )
                s_nThreadCount = 1;
            if ( s_nQueueSize == 0 )
                s_nQueueSize = 1000;

            s_Topology = Topology(s_nThreadCount);

            std::cout << "[ STAT     ] ThreadCount = " << s_nThreadCount << std::endl;
            std::cout << "[ STAT     ] QueueSize = " << s_nQueueSize << std::endl;

            s_Topology = Topology(s_nThreadCount);

            std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        }

        //static void TearDownTestCase();

    protected:
        template <class Queue>
        void analyze( Queue& q )
        {
            cds_test::thread_pool& pool = get_pool();
            std::unique_ptr< uint8_t[] > arr( new uint8_t[s_nQueueSize] );
            memset(arr.get(), 0, sizeof(arr[0]) * s_nQueueSize );

            size_t nTotalPops = 0;
            for ( size_t i = 0; i < pool.size(); ++i ) {
                Consumer<Queue>& thread = static_cast<Consumer<Queue>&>(pool.get( i ));
                for ( size_t j = 0; j < s_nQueueSize; ++j )
                    arr[j] += thread.m_arr[j];
                nTotalPops += thread.m_nPopCount;
            }
            EXPECT_EQ( nTotalPops, s_nQueueSize );
            EXPECT_TRUE( q.empty());

            for ( size_t i = 0; i < s_nQueueSize; ++i ) {
                EXPECT_EQ( arr[i], 1 ) << "i=" << i;
            }
        }

        template <class Queue>
        void test( Queue& q )
        {
            cds_test::thread_pool& pool = get_pool();

            pool.add( new Consumer<Queue>( pool, q ), s_nThreadCount );

            for ( size_t i = 0; i < s_nQueueSize; ++i )
                q.push( i, 0 );

            propout() << std::make_pair( "thread_count", s_nThreadCount )
                << std::make_pair( "push_count", s_nQueueSize );

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair( "duration", duration );

            analyze( q );

            propout() << q.statistics();
        }
    };

#define CDSSTRESS_QUEUE_F( QueueType, HasBasket ) \
    TEST_F( sb_queue_pop, QueueType ) \
    { \
        QueueType q(s_nThreadCount); \
        test( q ); \
        QueueType::gc::force_dispose(); \
    }

    using namespace cds::container::bags;
    using value_type = sb_queue_pop::value_type;
    using gc_type = cds::gc::HP;

    using SBSimpleBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, SimpleBag>;
    using SBIdBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, IdBag>;
    using SBStackBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, StackBag>;

    struct htm_traits : cds::container::sb_basket_queue::traits {
      typedef cds::intrusive::htm_basket_queue::htm_insert<> insert_policy;
    };

    using HTMSBSimpleBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, SimpleBag, htm_traits>;
    using HTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, IdBag, htm_traits>;
    using HTMSBStackBasketQueue_HP = cds::container::SBBasketQueue<gc_type, value_type, StackBag, htm_traits>;

    static_assert(std::is_same<HTMSBSimpleBasketQueue_HP::insert_policy, htm_traits::insert_policy>::value, "Use htm");

    CDSSTRESS_QUEUE_F( SBSimpleBasketQueue_HP, std::true_type)
    //CDSSTRESS_QUEUE_F( SBIdBasketQueue_HP)
    //CDSSTRESS_QUEUE_F( SBStackBasketQueue_HP)

#ifdef CDS_HTM_SUPPORT
    //CDSSTRESS_QUEUE_F( HTMSBSimpleBasketQueue_HP)
    //CDSSTRESS_QUEUE_F( HTMSBIdBasketQueue_HP)
    //CDSSTRESS_QUEUE_F( HTMSBStackBasketQueue_HP)
#endif // CDS_HTM_SUPPORT

#undef CDSSTRESS_QUEUE_F

} // namespace
