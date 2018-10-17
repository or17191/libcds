// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <cds_test/stress_test.h>

#include <cds/compiler/defs.h>
#include <cds/sync/htm.h>

namespace {
    class htm : public cds_test::stress_fixture {
      protected:
        static size_t s_nThreadCount;
        static size_t s_nIncrementCount;

        class Worker : public cds_test::thread {
            typedef cds_test::thread base_class;

            size_t &m_counter;

          public:
            size_t m_nSuccess = 0;

          public:
            Worker(cds_test::thread_pool &pool, size_t &s)
                : base_class(pool), m_counter(s) {}

            Worker(Worker &src) : base_class(src), m_counter(src.m_counter) {}

            virtual thread *clone() { return new Worker(*this); }

            virtual void test() {
                for (size_t pass = 0; pass < s_nIncrementCount; ++pass) {
                    auto res = cds::sync::htm([this] { m_counter++; });
                    if (res) {
                        ++m_nSuccess;
                    }
                }
            }
        };

      public:
        static void SetUpTestCase() {
            cds_test::config const &cfg = get_config("free_list");

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
        void test() {
            constexpr_if(!cds::sync::RTM_ENABLED) {
                std::cout << "[ SKIPPED  ] RTM is not supported" << std::endl;
                return;
            }
            cds_test::thread_pool &pool = get_pool();

            size_t nTotal = 0;

            pool.add(new Worker(pool, nTotal), s_nThreadCount);

            propout() << std::make_pair("work_thread", s_nThreadCount)
                      << std::make_pair("increment_count", s_nIncrementCount);

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair("duration", duration);

            // analyze result

            size_t nSuccess = 0;
            for (size_t threadNo = 0; threadNo < pool.size(); ++threadNo)
                nSuccess +=
                    static_cast<Worker &>(pool.get(threadNo)).m_nSuccess;

            EXPECT_EQ(nSuccess, nTotal);
        }
    };

    size_t htm::s_nThreadCount = 4;
    size_t htm::s_nIncrementCount = 100000;

    TEST_F(htm, increment) { test(); }
} // namespace
