// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <cds_test/stress_test.h>

#include <cds/sync/htm.h>
#include <cds/algo/atomic.h>

namespace {
    class counter_inc : public cds_test::stress_fixture {
      protected:
        static size_t s_nThreadCount;
        static size_t s_nIncrementCount;

        template <class IncrementPolicy>
        class Worker : public cds_test::thread {
            typedef cds_test::thread base_class;
            typedef typename IncrementPolicy::counter_type counter_type;

            counter_type &m_counter;

          public:
            size_t m_nSuccess = 0;

          public:
            Worker(cds_test::thread_pool &pool, counter_type &s)
                : base_class(pool), m_counter(s) {}

            Worker(Worker &src) : base_class(src), m_counter(src.m_counter) {}

            virtual thread *clone() { return new Worker(*this); }

            virtual void test() {
                auto& counter=m_counter;
                for (size_t pass = 0; pass < s_nIncrementCount; ++pass) {
                    auto res = IncrementPolicy::_(counter);
                    m_nSuccess += static_cast<bool>(res);
                }
            }
        };

      public:
        static void SetUpTestCase() {
            cds_test::config const &cfg = get_config("htm");

            s_nThreadCount = cfg.get_size_t("ThreadCount", s_nThreadCount);
            s_nIncrementCount =
                cfg.get_size_t("IncrementCount", s_nIncrementCount);

            if (s_nThreadCount == 0)
                s_nThreadCount = 1;
            if (s_nIncrementCount == 0)
                s_nIncrementCount = 1000;
        }
        // static void TearDownTestCase();

      protected:
        template <class IncrementPolicy>
        void test() {
            cds_test::thread_pool &pool = get_pool();

            typename IncrementPolicy::counter_type nTotal{0};
            typedef Worker<IncrementPolicy> worker_type;

            pool.add(new worker_type(pool, nTotal), s_nThreadCount);

            propout() << std::make_pair("work_thread", s_nThreadCount)
                      << std::make_pair("increment_count", s_nIncrementCount);

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair("duration", duration);

            std::cout << "[ MESSAGE  ] Total: " << nTotal << std::endl;

            // analyze result

            size_t nSuccess = 0;
            for (size_t threadNo = 0; threadNo < pool.size(); ++threadNo)
                nSuccess +=
                    static_cast<worker_type &>(pool.get(threadNo)).m_nSuccess;

            EXPECT_EQ(nSuccess, nTotal);
            EXPECT_NE(0, nTotal);

            propout() << std::make_pair("total", nSuccess);
        }
    };

    size_t counter_inc::s_nThreadCount = 4;
    size_t counter_inc::s_nIncrementCount = 100000;

    static void spin(size_t n) { for(volatile size_t i = 0; i < n; ++i) {} }

    struct HTMPolicy {
      typedef atomics::atomic_size_t counter_type;
      static bool _(counter_type& counter) {
        auto tmp = counter.load(atomics::memory_order_acquire);
        spin(1000);
        auto res = cds::sync::htm([&] {
          if (tmp != counter.load(atomics::memory_order_relaxed)) cds::sync::abort(0xff); 
          counter.store(tmp + 1, atomics::memory_order_relaxed);
        });
        return static_cast<bool>(res);
      }
    };

    struct CASPolicy {
      typedef atomics::atomic_size_t counter_type;
      static bool _(counter_type& counter) {
        auto old = counter.load(atomics::memory_order_acquire);
        spin(1000);
        return counter.compare_exchange_weak(old, old + 1, atomics::memory_order_release, atomics::memory_order_relaxed);
      }
    };

#ifdef CDS_HTM_SUPPORT
    TEST_F(counter_inc, htm) { test<HTMPolicy>(); }
    TEST_F(counter_inc, cas) { test<CASPolicy>(); }
#endif // CDS_HTM_SUPPORT
} // namespace
