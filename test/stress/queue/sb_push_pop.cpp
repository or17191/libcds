// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "queue_type.h"
#include "sb_adaptors.h"

#include <cds/container/bags.h>
#include <cds/container/ccqueue.h>
#include <cds/container/sb_basket_queue.h>
#include <cds/container/wf_queue.h>
#include <cds/details/memkind_allocator.h>

#include <algorithm>
#include <type_traits>
#include <vector>

#include <cds_test/check_baskets.h>
#include <cds_test/topology.h>

#include <boost/optional.hpp>

// Multi-threaded queue push/pop test
namespace {

using queue::push;
using queue::pop;
using queue::safe_add_pointer;
using cds_test::utils::topology::Topology;
static boost::optional<Topology> s_Topology;
static size_t s_nConsumerThreadCount = 4;
static size_t s_nProducerThreadCount = 4;
static size_t s_nQueueSize = 4000000;
static size_t s_nPreStoreSize = 0;
static size_t s_nHeavyValueSize = 100;

static std::atomic<size_t> s_nProducerDone(0);
static bool s_nPreStoreDone(false);

struct old_value
{
    size_t nNo;
    size_t nWriterNo;
};

struct writer_compare
{
    const std::less<size_t> cmp;
    using pair = std::pair<old_value *, size_t>;
    bool operator()(const pair &v, size_t writer) const { return cmp(v.first->nWriterNo, writer); }
    bool operator()(size_t writer, const pair &v) const { return cmp(writer, v.first->nWriterNo); }
    bool operator()(const pair &lhs, const pair &rhs) const { return cmp(lhs.first->nWriterNo, rhs.first->nWriterNo); }
};

template <class Queue>
size_t push_many(Queue &queue, const size_t first, const size_t last, const size_t producer,
                 typename safe_add_pointer<typename Queue::value_type>::type values)
{
    size_t failed = 0;
    for (size_t i = first; i < last; ++i, ++values) {
        while (!push(queue, values, producer)) {
            ++failed;
        }
    }
    return failed;
}

template <class Value>
void push_many_setup(const size_t first, const size_t last, const size_t producer,
                     Value *values)
{
    size_t failed = 0;
    for (size_t i = first; i < last; ++i, ++values) {
        values->nNo = i;
        values->nWriterNo = producer;
    }
}

template <class Value = old_value>
class sb_queue_push_pop : public cds_test::stress_fixture
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
    class Producer : public cds_test::thread
    {
        typedef cds_test::thread base_class;

    public:
        Producer(cds_test::thread_pool &pool, Queue &queue, size_t nStart, size_t nEnd)
            : base_class(pool, producer_thread), m_Queue(queue), m_nPushFailed(0), m_nStart(nStart), m_nEnd(nEnd)
        {
        }

        Producer(Producer &src)
            : base_class(src), m_Queue(src.m_Queue), m_nPushFailed(0), m_nStart(src.m_nStart), m_nEnd(src.m_nEnd)
        {
        }

        virtual thread *clone()
        {
            return new Producer(*this);
        }

        virtual void SetUp()
        {
            thread::SetUp();
            push_many_setup(m_nStart, m_nEnd, m_nWriterId, m_values);
        }

        virtual void test()
        {
            s_Topology->pin_thread(m_nThreadId);
            auto start = clock_type::now();
            m_nPushFailed += push_many(m_Queue, m_nStart, m_nEnd, m_nWriterId, m_values);
            auto end = clock_type::now();
            m_Duration = std::chrono::duration_cast<duration_type>(end - start);
            s_nProducerDone.fetch_add(1);
            s_Topology->verify_pin(m_nThreadId);
        }

    public:
        Queue &m_Queue;
        size_t m_nPushFailed;
        size_t const m_nStart;
        size_t const m_nEnd;
        size_t m_nWriterId;
        size_t m_nThreadId;
        duration_type m_Duration;
        typename safe_add_pointer<typename Queue::value_type>::type m_values{};
    };

    template <class Queue, class HasBasket>
    class Consumer : public cds_test::thread
    {
        typedef cds_test::thread base_class;

    public:
        Queue &m_Queue;
        size_t const m_nPushPerProducer;
        size_t m_nPopEmpty;
        size_t m_nPopped;
        size_t m_nBadWriter;
        size_t m_nReaderId;
        size_t m_nThreadId;
        duration_type m_Duration;

        typedef std::vector<std::pair<typename safe_add_pointer<typename Queue::value_type>::type, size_t>> popped_data;
        typedef std::vector<size_t>::iterator data_iterator;
        typedef std::vector<size_t>::const_iterator const_data_iterator;

        popped_data m_WriterData;

    private:
        void initPoppedData()
        {
            const size_t nProducerCount = s_nProducerThreadCount;
            m_WriterData.reserve(m_nPushPerProducer * nProducerCount);
        }

    public:
        Consumer(cds_test::thread_pool &pool, Queue &queue, size_t nPushPerProducer)
            : base_class(pool, consumer_thread), m_Queue(queue), m_nPushPerProducer(nPushPerProducer), m_nPopEmpty(0), m_nPopped(0), m_nBadWriter(0)
        {
            initPoppedData();
        }
        Consumer(Consumer &src)
            : base_class(src), m_Queue(src.m_Queue), m_nPushPerProducer(src.m_nPushPerProducer), m_nPopEmpty(0), m_nPopped(0), m_nBadWriter(0)
        {
            initPoppedData();
        }

        virtual thread *clone()
        {
            return new Consumer(*this);
        }

        virtual void test()
        {
            s_Topology->pin_thread(m_nThreadId);
            m_nPopEmpty = 0;
            m_nPopped = 0;
            m_nBadWriter = 0;
            const size_t nTotalWriters = s_nProducerThreadCount;
            value_type *v;
            bool writers_done = false;
            size_t basket;
            auto start = clock_type::now();
            while (true) {
                if (pop(m_Queue, v, m_nReaderId, basket, HasBasket{})) {
                    ++m_nPopped;
                    // TODO bad writer
                    m_WriterData.emplace_back(v, basket);
                    if (m_nPopped >= m_nPushPerProducer) {
                        break;
                    }
                } else {
                    ++m_nPopEmpty;
                    writers_done = s_nProducerDone.load() >= nTotalWriters;
                    if (writers_done) {
                        break;
                    }
                }
            }
            auto end = clock_type::now();
            m_Duration = std::chrono::duration_cast<duration_type>(end - start);
            s_Topology->verify_pin(m_nThreadId);
        }

        virtual void TearDown()
        {
            thread::TearDown();
            std::stable_sort(m_WriterData.begin(), m_WriterData.end(), writer_compare{});
            auto pos = std::lower_bound(m_WriterData.begin(), m_WriterData.end(),
                                        s_nProducerThreadCount, writer_compare{});
            m_nBadWriter = std::distance(pos, m_WriterData.end());
        }
    };

protected:
    static size_t s_nThreadPushCount;
    static size_t s_nThreadPreStoreSize;

protected:
    template <class It>
    void check_baskets(It first, It last, std::true_type)
    {
        using value_type = decltype(*first);
        auto checker = cds_test::BasketsChecker::make(first, last, [](const size_t &e) { return e; });
        EXPECT_EQ(0, checker.null_basket_count);
        EXPECT_GE(s_nProducerThreadCount, checker.distribution.rbegin()->first) << " allow at most one element per thread in each basket";
        propout() << std::make_pair("basket_distribution", checker.distribution_str());
        auto mean_std = checker.mean_std();
        propout() << std::make_pair("basket_mean", mean_std.first);
        propout() << std::make_pair("basket_std", mean_std.second);
    }

    template <class It>
    void check_baskets(It first, It last, std::false_type)
    {
    }

    template <class Queue, class HasBaskets>
    void analyze(Queue &q, HasBaskets, size_t /*nLeftOffset*/ = 0, size_t nRightOffset = 0)
    {
        cds_test::thread_pool &pool = get_pool();

        typedef Consumer<Queue, HasBaskets> consumer_type;
        typedef Producer<Queue> producer_type;

        size_t nPostTestPops = 0;
        typename consumer_type::popped_data post_pops;
        post_pops.reserve(s_nPreStoreSize);
        {
            value_type *v;
            cds::uuid_type basket;
            while (pop(q, v, 0, basket, HasBaskets{})) {
                ++nPostTestPops;
                post_pops.emplace_back(v, basket);
            }
            std::stable_sort(post_pops.begin(), post_pops.end(), writer_compare{});
            auto pos = std::lower_bound(post_pops.begin(), post_pops.end(),
                                        s_nProducerThreadCount, writer_compare{});
            auto post_bad_writer = std::distance(pos, post_pops.end());
            EXPECT_EQ(post_bad_writer, 0);
        }
        EXPECT_EQ(post_pops.size(), s_nPreStoreSize);

        size_t nTotalPops = 0;
        size_t nPopFalse = 0;
        size_t nPoppedItems = 0;
        size_t nPushFailed = 0;

        std::vector<consumer_type *> arrConsumer;
        std::vector<size_t> baskets;

        for (size_t i = 0; i < pool.size(); ++i) {
            cds_test::thread &thr = pool.get(i);
            if (thr.type() == consumer_thread) {
                consumer_type &consumer = static_cast<consumer_type &>(thr);
                nTotalPops += consumer.m_nPopped;
                nPopFalse += consumer.m_nPopEmpty;
                arrConsumer.push_back(&consumer);
                EXPECT_EQ(consumer.m_nBadWriter, 0u) << "consumer_thread_no " << i;
                nPoppedItems += consumer.m_WriterData.size();
            } else {
                assert(thr.type() == producer_thread);

                producer_type &producer = static_cast<producer_type &>(thr);
                nPushFailed += producer.m_nPushFailed;
                EXPECT_EQ(producer.m_nPushFailed, 0u) << "producer_thread_no " << i;
            }
        }
        EXPECT_EQ(nTotalPops, nPoppedItems);

        propout() << std::make_pair("empty_pops", nPopFalse);

        EXPECT_EQ(nTotalPops + nPostTestPops, s_nQueueSize + s_nPreStoreSize) << "nTotalPops=" << nTotalPops << ", nPostTestPops=" << nPostTestPops;
        {
            value_type *v;
            cds::uuid_type basket;
            EXPECT_FALSE(pop(q, v, 0, basket, HasBaskets{}));
        }

        // Test consistency of popped sequence
        for (size_t nWriter = 0; nWriter < s_nProducerThreadCount; ++nWriter) {
            std::vector<size_t> arrData;
            arrData.reserve(s_nThreadPushCount);
            for (size_t nReader = 0; nReader < arrConsumer.size(); ++nReader) {
                auto &consumer = *arrConsumer[nReader];
                auto rng = std::equal_range(consumer.m_WriterData.begin(), consumer.m_WriterData.end(),
                                            nWriter, writer_compare{});
                auto it = rng.first;
                auto itEnd = rng.second;
                if (it != itEnd) {
                    auto itPrev = it;
                    ASSERT_EQ(nWriter, it->first->nWriterNo);
                    for (++it; it != itEnd; ++it) {
                        ASSERT_LT(itPrev->first->nNo, it->first->nNo + nRightOffset) << "consumer=" << nReader << ", producer=" << nWriter;
                        itPrev = it;
                    }
                }

                for (it = rng.first; it != rng.second; ++it) {
                    arrData.push_back(it->first->nNo);
                    baskets.push_back(it->second);
                }
            }
            {
                auto rng = std::equal_range(post_pops.begin(), post_pops.end(), nWriter,
                                            writer_compare{});
                auto it = rng.first;
                auto itEnd = rng.second;
                if (it != itEnd) {
                    auto itPrev = it;
                    ASSERT_EQ(nWriter, it->first->nWriterNo);
                    for (++it; it != itEnd; ++it) {
                        ASSERT_LT(itPrev->first->nNo, it->first->nNo + nRightOffset) << ", producer=" << nWriter;
                        itPrev = it;
                    }
                }

                for (it = rng.first; it != rng.second; ++it) {
                    arrData.push_back(it->first->nNo);
                    baskets.push_back(it->second);
                }
            }

            std::sort(arrData.begin(), arrData.end());
            for (size_t i = 1; i < arrData.size(); ++i) {
                ASSERT_EQ(arrData[i - 1] + 1, arrData[i]) << "producer=" << nWriter;
            }

            EXPECT_EQ(arrData[0], 0u) << "producer=" << nWriter;
            EXPECT_EQ(arrData[arrData.size() - 1], s_nThreadPushCount + s_nThreadPreStoreSize - 1) << "producer=" << nWriter;
        }

        check_baskets(baskets.begin(), baskets.end(), HasBaskets{});
    }

    template <class Producer, class Consumer, class Pointer>
    static void setup_pool_ids(cds_test::thread_pool &pool, bool independent_ids, Pointer &ptr)
    {
        size_t writer_id = 0;
        size_t reader_id = 0;
        for (size_t i = 0; i < pool.size(); ++i) {
            cds_test::thread &thr = pool.get(i);
            if (thr.type() == consumer_thread) {
                Consumer &consumer = static_cast<Consumer &>(thr);
                if (independent_ids) {
                    consumer.m_nReaderId = reader_id++;
                } else {
                    consumer.m_nReaderId = writer_id++;
                }
            } else {
                assert(thr.type() == producer_thread);
                Producer &producer = static_cast<Producer &>(thr);
                producer.m_nWriterId = writer_id++;
                producer.m_values = ptr;
                ptr += (producer.m_nEnd - producer.m_nStart);
            }
        }
        size_t tid = 0;
        for (size_t i = 0; i < pool.size(); ++i) {
            cds_test::thread &thr = pool.get(i);
            if (thr.type() == producer_thread) {
                Producer &producer = static_cast<Producer &>(thr);
                producer.m_nThreadId = tid++;
            }
        }
        for (size_t i = 0; i < pool.size(); ++i) {
            cds_test::thread &thr = pool.get(i);
            if (thr.type() == consumer_thread) {
                Consumer &consumer = static_cast<Consumer &>(thr);
                consumer.m_nThreadId = tid++;
            }
        }
    }

    template <class Queue, class HasBaskets>
    void test_queue(Queue &q, typename safe_add_pointer<typename Queue::value_type>::type values, bool independent_ids, bool sequential_pre_store, HasBaskets)
    {
        typedef Consumer<Queue, HasBaskets> consumer_type;
        typedef Producer<Queue> producer_type;

        cds_test::thread_pool &pool = get_pool();

        if (sequential_pre_store) {
            std::cout << "[ STAT     ] Sequential pre store" << std::endl;
            for (size_t i = 0; i < s_nProducerThreadCount; ++i) {
                push_many_setup(0, s_nThreadPreStoreSize, i, values);
                size_t failed = push_many(q, 0, s_nThreadPreStoreSize, i, values);
                EXPECT_EQ(0, failed);
                values += s_nThreadPreStoreSize;
            }
        } else {
            std::cout << "[ STAT     ] Parallel pre store" << std::endl;
            pool.add(new producer_type(pool, q, 0, s_nThreadPreStoreSize), s_nProducerThreadCount);
            setup_pool_ids<producer_type, consumer_type>(pool, independent_ids, values);
            pool.run();
            pool.reset();
        }
        pool.add(new producer_type(pool, q, s_nThreadPreStoreSize, s_nThreadPushCount + s_nThreadPreStoreSize), s_nProducerThreadCount);
        pool.add(new consumer_type(pool, q, s_nThreadPushCount), s_nConsumerThreadCount);
        setup_pool_ids<producer_type, consumer_type>(pool, independent_ids, values);
        s_nPreStoreDone = true;
        s_nProducerDone.store(0);
        std::cout << "[ PROMPT   ] PID = " << getpid() << std::endl;
        std::chrono::milliseconds duration = pool.run();

        propout() << std::make_pair("producer_count", s_nProducerThreadCount)
                  << std::make_pair("consumer_count", s_nConsumerThreadCount)
                  << std::make_pair("push_count", s_nQueueSize)
                  << std::make_pair("pre_store_count", s_nPreStoreSize);

        propout() << std::make_pair("duration", duration);
        std::cout << "[ STAT     ] Duration = " << duration.count() << "ms" << std::endl;

        duration_type reader_duration{0};
        duration_type writer_duration{0};

        for (size_t i = 0; i < pool.size(); ++i) {
            cds_test::thread &thr = pool.get(i);
            if (thr.type() == consumer_thread) {
                consumer_type &consumer = static_cast<consumer_type &>(thr);
                reader_duration += consumer.m_Duration;
            } else {
                assert(thr.type() == producer_thread);
                producer_type &producer = static_cast<producer_type &>(thr);
                writer_duration += producer.m_Duration;
            }
        }
        auto ns_reader_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(reader_duration);
        auto ns_writer_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(writer_duration);
        double ns_reader_throughput = double(ns_reader_duration.count()) / (s_nQueueSize);
        double ns_writer_throughput = double(ns_writer_duration.count()) / (s_nQueueSize);

        propout() << std::make_pair("reader_throughput_nsop", ns_reader_throughput);
        propout() << std::make_pair("writer_throughput_nsop", ns_writer_throughput);
        std::cout << "[ STAT     ] Reader Throughput = " << ns_reader_throughput << "ns/op" << std::endl;
        std::cout << "[ STAT     ] Writer Throughput = " << ns_writer_throughput << "ns/op" << std::endl;
    }

    template <class Queue, class HasBaskets>
    void test(Queue &q, bool independent_ids, bool sequential_pre_store, HasBaskets hb)
    {
        cds::details::memkind_vector<typename std::remove_pointer<typename Queue::value_type>::type> values(s_nQueueSize + s_nPreStoreSize);
        test_queue(q, values.data(), independent_ids, sequential_pre_store, hb);
        analyze(q, hb);
        propout() << q.statistics();
    }

private:
    static void set_array_size(size_t size)
    {
        const bool tmp = fc_test::has_set_array_size<value_type>::value;
        set_array_size(size, std::integral_constant<bool, tmp>());
    }

    static void set_array_size(size_t size, std::true_type)
    {
        value_type::set_array_size(size);
    }

    static void set_array_size(size_t, std::false_type)
    {
    }

public:
    static void SetUpTestCase()
    {
        cds_test::config const &cfg = get_config("sb_queue_push_pop");

        s_nConsumerThreadCount = cfg.get_size_t("ConsumerCount", s_nConsumerThreadCount);
        s_nProducerThreadCount = cfg.get_size_t("ProducerCount", s_nProducerThreadCount);
        s_nQueueSize = cfg.get_size_t("QueueSize", s_nQueueSize);
        s_nPreStoreSize = cfg.get_size_t("PreStore", s_nPreStoreSize);
        s_nHeavyValueSize = cfg.get_size_t("HeavyValueSize", s_nHeavyValueSize);

        s_nQueueSize *= s_nProducerThreadCount;
        s_nPreStoreSize *= s_nProducerThreadCount;

        if (s_nConsumerThreadCount == 0u)
            s_nConsumerThreadCount = 1;
        if (s_nProducerThreadCount == 0u)
            s_nProducerThreadCount = 1;
        if (s_nQueueSize == 0u)
            s_nQueueSize = 1000;
        if (s_nHeavyValueSize == 0)
            s_nHeavyValueSize = 1;

        s_nThreadPushCount = s_nQueueSize / s_nProducerThreadCount;
        s_nQueueSize = s_nThreadPushCount * s_nProducerThreadCount;

        s_nThreadPreStoreSize = s_nPreStoreSize / s_nProducerThreadCount;
        s_nPreStoreSize = s_nThreadPreStoreSize * s_nProducerThreadCount;

        std::cout << "[ STAT     ] Producer = " << s_nProducerThreadCount << std::endl;
        std::cout << "[ STAT     ] Consumer = " << s_nConsumerThreadCount << std::endl;
        std::cout << "[ STAT     ] QueueSize = " << s_nQueueSize << std::endl;
        std::cout << "[ STAT     ] PreStoreSize = " << s_nPreStoreSize << std::endl;

        set_array_size(s_nHeavyValueSize);

        s_Topology = Topology(s_nProducerThreadCount + s_nConsumerThreadCount);

        std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        std::cout << "[ STAT     ] Socket boundary = " << s_Topology->socket_boundary() << std::endl;
    }

    //static void TearDownTestCase();
};
template <class Queue>
size_t sb_queue_push_pop<Queue>::s_nThreadPushCount;
template <class Queue>
size_t sb_queue_push_pop<Queue>::s_nThreadPreStoreSize;

using simple_sb_queue_push_pop = sb_queue_push_pop<>;

#undef CDSSTRESS_Queue_F
#define CDSSTRESS_Queue_F(test_fixture, type_name)                 \
    TEST_F(test_fixture, type_name)                                \
    {                                                              \
        typedef type_name<test_fixture> queue_type;                \
        ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount); \
        queue_type queue(s_nConsumerThreadCount);                  \
        test(queue, true, false, std::true_type{});                \
    }

#define CDSSTRESS_WFQueue_F(test_fixture, type_name)                                                      \
    TEST_F(test_fixture, type_name)                                                                       \
    {                                                                                                     \
        typedef type_name<test_fixture> queue_type;                                                       \
        ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount);                                        \
        queue_type queue(s_nConsumerThreadCount + s_nProducerThreadCount, s_Topology->socket_boundary()); \
        test(queue, false, true, std::false_type{});                                                      \
    }

template <class Fixture>
using WFQueue = cds::container::WFQueue<typename Fixture::value_type>;

template <class Fixture>
using CCQueue = cds::container::CCQueue<typename Fixture::value_type>;

struct numa_wf_queue : public cds::container::wf_queue::traits
{
    using numa_balance = std::true_type;
};

template <class Fixture>
using NUMAWFQueue = cds::container::WFQueue<typename Fixture::value_type, numa_wf_queue>;

CDSSTRESS_WFQueue_F(simple_sb_queue_push_pop, WFQueue)
    //CDSSTRESS_WFQueue_F( simple_sb_queue_push_pop, NUMAWFQueue)
    CDSSTRESS_WFQueue_F(simple_sb_queue_push_pop, CCQueue)

    // template <class Fixture>
    // using BasketWFQueue = cds::container::BasketWFQueue<typename Fixture::gc_type, typename Fixture::value_type>;

    // TEST_F( simple_sb_queue_push_pop, BasketWFQueue )
    // {
    //     typedef BasketWFQueue<simple_sb_queue_push_pop> queue_type;
    //     ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount);
    //     queue_type queue( s_nConsumerThreadCount + s_nProducerThreadCount);
    //     test( queue, false );
    // }

    struct stat_wf_queue : public cds::container::wf_queue::traits
{
    typedef cds::container::wf_queue::full_stat<> stat_type;
};

template <class Fixture>
using WFQueue_Stat = cds::container::WFQueue<typename Fixture::value_type, stat_wf_queue>;
CDSSTRESS_WFQueue_F(simple_sb_queue_push_pop, WFQueue_Stat)

    using namespace cds::container::bags;
namespace sb_basket_queue = cds::container::sb_basket_queue;
using sb_basket_queue::Constant;
using sb_basket_queue::Linear;

struct fast_cas_id_traits : cds::container::sb_basket_queue::traits
{
    typedef sb_basket_queue::atomics_insert<Constant<350>> insert_policy;
};
template <class Fixture>
using FastSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, fast_cas_id_traits>;
struct slow_cas_id_traits : cds::container::sb_basket_queue::traits
{
    typedef sb_basket_queue::atomics_insert<Constant<720>> insert_policy;
};
template <class Fixture>
using SlowSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, slow_cas_id_traits>;

struct fast_htm_id_traits : cds::container::sb_basket_queue::traits
{
    typedef sb_basket_queue::htm_insert<Constant<350>, Constant<30>> insert_policy;
};
template <class Fixture>
using FastHTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, fast_htm_id_traits>;
struct slow_htm_id_traits : cds::container::sb_basket_queue::traits
{
    typedef sb_basket_queue::htm_insert<Constant<720>, Constant<30>> insert_policy;
};
template <class Fixture>
using SlowHTMSBIdBasketQueue_HP = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, slow_htm_id_traits>;

struct stat_traits : public slow_cas_id_traits
{
    typedef cds::container::sb_basket_queue::stat<> stat;
};
struct htm_id_stat_traits : public slow_htm_id_traits
{
    typedef cds::container::sb_basket_queue::stat<> stat;
};

template <class Fixture>
using HTMSBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, htm_id_stat_traits>;
template <class Fixture>
using SBIdBasketQueue_HP_Stat = cds::container::SBBasketQueue<typename Fixture::gc_type, typename Fixture::value_type, IdBag, stat_traits>;

// CDSSTRESS_Queue_F( SBSimpleBasketQueue_HP)
//CDSSTRESS_Queue_F( simple_sb_queue_push_pop, FastSBIdBasketQueue_HP)
CDSSTRESS_Queue_F(simple_sb_queue_push_pop, SlowSBIdBasketQueue_HP)
    //CDSSTRESS_Queue_F( SBStackBasketQueue_HP)

    CDSSTRESS_Queue_F(simple_sb_queue_push_pop, SBIdBasketQueue_HP_Stat)

#ifdef CDS_HTM_SUPPORT
    //CDSSTRESS_Queue_F( HTMSBSimpleBasketQueue_HP)
    //CDSSTRESS_Queue_F( simple_sb_queue_push_pop, FastHTMSBIdBasketQueue_HP)
    CDSSTRESS_Queue_F(simple_sb_queue_push_pop, SlowHTMSBIdBasketQueue_HP)
        CDSSTRESS_Queue_F(simple_sb_queue_push_pop, HTMSBIdBasketQueue_HP_Stat)
//CDSSTRESS_Queue_F( HTMSBStackBasketQueue_HP)
#endif // CDS_HTM_SUPPORT

#undef CDSSTRESS_Queue_F

            struct vanilla_traits : cds::container::basket_queue::traits
{
    using allocator = cds::details::memkind_allocator<int>;
};
template <class Fixture>
using VanillaBasketQueue = cds::container::BasketQueue<typename Fixture::gc_type, typename Fixture::value_type *, vanilla_traits>;
TEST_F(simple_sb_queue_push_pop, VanillaBasketQueue)
{
    typedef VanillaBasketQueue<simple_sb_queue_push_pop> queue_type;
    ASSERT_EQ(s_nConsumerThreadCount, s_nProducerThreadCount);
    queue_type queue;
    test(queue, false, false, std::false_type{});
}

} // namespace
