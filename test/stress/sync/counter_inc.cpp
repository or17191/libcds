// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <cds_test/stress_test.h>

#include <map>

#include <cds/sync/htm.h>
#include <cds/algo/atomic.h>
#include <cds/opt/options.h>
#include <boost/optional.hpp>

#include <cds_test/topology.h>

namespace {
    inline static void spin(size_t n, std::true_type) {
      volatile int x = 0;
      for(size_t i = 0; i < n; ++i) {
        x = 0;
      }
    }
    inline static void spin(size_t, std::false_type) { }

    template <class Value>
    struct alignas(cds::c_nCacheLineSize) PaddedValue{
      char pad1_[cds::c_nCacheLineSize];
      Value value;
      char pad2_[cds::c_nCacheLineSize - sizeof(Value)];

      PaddedValue() {
        static_assert(sizeof(Value) <= cds::c_nCacheLineSize, "");
        static_assert(sizeof(*this) == 2 * cds::c_nCacheLineSize, "");
        static_assert(alignof(*this) == cds::c_nCacheLineSize, "");
      }
    };

    using cds_test::utils::topology::Topology;
    class counter_inc : public cds_test::stress_fixture {
      protected:
        static size_t s_nThreadCount;
        static size_t s_nIncrementCount;
        static size_t s_nThreadIncrementCount;
        static boost::optional<Topology> s_Topology;

        template <class IncrementPolicy>
        class Worker : public cds_test::thread {
            typedef cds_test::thread base_class;
            typedef typename IncrementPolicy::counter_type counter_type;

            counter_type &m_counter;
            IncrementPolicy& m_increment;
            const Topology& m_Topology;

          public:
            intmax_t m_nSuccess = 0;
            bool m_good = true;

          public:
            Worker(cds_test::thread_pool &pool, counter_type &s, IncrementPolicy& increment,
                  const Topology& topology, int type = 0)
                : base_class(pool, type), m_counter(s), m_increment(increment), m_Topology(topology) {}

            Worker(Worker &src) : base_class(src), m_counter(src.m_counter), m_increment(src.m_increment),
                m_Topology(src.m_Topology) {}

            virtual thread *clone() { return new Worker(*this); }

            virtual void test() {
                auto id_ = id();
                m_Topology.pin_thread(id_);
                auto& counter=m_counter;
                for (size_t pass = 0; pass < s_nThreadIncrementCount; ++pass) {
                    auto tmp = counter.load(atomics::memory_order_acquire);
                    auto res = m_increment(counter, id_);
                    auto tmp2 = counter.load(atomics::memory_order_release);
                    if (!(tmp < tmp2)) {
                      m_good = false;
                    }
                    m_nSuccess += static_cast<bool>(res);
                    spin(1000, std::true_type{});
                }
                m_Topology.verify_pin(id_);
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

            std::cout << "[ STAT     ] ThreadCount = " << s_nThreadCount << std::endl;
            std::cout << "[ STAT     ] IncrementCount = " << s_nIncrementCount << std::endl;

            s_Topology = Topology(s_nThreadCount);

            std::cout << "[ STAT     ] Topology = " << *s_Topology << std::endl;
        }
        // static void TearDownTestCase();

        template <size_t i> struct tester;

      protected:
        template <class IncrementPolicy>
        void test() {
            // s_nThreadIncrementCount = s_nIncrementCount / s_nThreadCount;
            // s_nIncrementCount = s_nThreadIncrementCount * s_nThreadCount;
            s_nThreadIncrementCount = s_nIncrementCount;
            s_nIncrementCount = s_nThreadIncrementCount;

            cds_test::thread_pool &pool = get_pool();

            using counter_type = typename IncrementPolicy::counter_type;
            using worker_type = Worker<IncrementPolicy>;

            PaddedValue<counter_type> counter;
            auto& nTotal = counter.value;
            nTotal = 0;

            IncrementPolicy incrementer(s_nThreadCount);

            pool.add(new worker_type(pool, nTotal, incrementer, *s_Topology), s_nThreadCount);

            propout() << std::make_pair("work_thread", s_nThreadCount)
                      << std::make_pair("increment_count", s_nIncrementCount);

            using fsecs = std::chrono::duration<float, std::ratio<1, 1>>;

            std::chrono::milliseconds duration = pool.run();
            std::cout << "[ STAT     ] Duration = " << fsecs(duration).count() << "s" << std::endl;
            std::cout << "[ STAT     ] Total = " << nTotal << std::endl;

            propout() << std::make_pair("duration", duration)
              << std::make_pair("atomic_counter", nTotal.load(std::memory_order_relaxed));

            incrementer.out(*this);

            // analyze result

            size_t nSuccess = 0;
            bool passed = true;
            for (size_t threadNo = 0; threadNo < pool.size(); ++threadNo) {
              auto& thread = static_cast<worker_type &>(pool.get(threadNo));
              EXPECT_TRUE(thread.m_good);
              passed = passed && thread.m_good;
              nSuccess += thread.m_nSuccess;
            }

            EXPECT_EQ(nSuccess, nTotal);
            EXPECT_NE(0, nTotal);

            propout() << std::make_pair("all_consistent", passed);
            propout() << std::make_pair("thread_counter", nSuccess);
        }
    };

    size_t counter_inc::s_nThreadCount = 4;
    size_t counter_inc::s_nIncrementCount = 100000;
    size_t counter_inc::s_nThreadIncrementCount = 100000 / 4;
    boost::optional<Topology> counter_inc::s_Topology{};

    struct HTMPolicyBase {
      typedef decltype(_xbegin()) status_type;
      static constexpr status_type UNINIT_STATUS = 0xdeadbeef;
      using statistics_type = std::map<status_type, size_t>;
      std::vector<PaddedValue<statistics_type>> m_counters;

      HTMPolicyBase(size_t tnum) : m_counters(tnum) {
      }

      void update(status_type status, size_t tid) {
        m_counters[tid].value[status]++;
      }

      void out(counter_inc& tester) {
        statistics_type total;
        for(auto& c: m_counters) {
          for(auto& e: c.value) {
            total[e.first] += e.second;
          }
        }
        std::stringstream s;
        for(auto& e: total) {
          s << '(' << e.first << ',' << e.second << ')';
        }
        tester.propout() << std::make_pair("htm_status", s.str());
      }
    };

    struct HTMPolicy : HTMPolicyBase {
      typedef atomics::atomic_size_t counter_type;

      using HTMPolicyBase::HTMPolicyBase;

      bool operator()(counter_type& counter, size_t id) {
        auto prev = counter.load(atomics::memory_order_acquire);
        bool good = false;
        status_type status = UNINIT_STATUS;
        while(prev >= counter.load(atomics::memory_order_acquire)) {
          status = _xbegin();
          if(status == _XBEGIN_STARTED) {
            auto tmp = counter.load(atomics::memory_order_relaxed);
            if (tmp == prev) {
              counter.store(tmp + 1, atomics::memory_order_relaxed);
              good = true;
            }
            _xend();
            update(status, id);
            return good;
          }
        }
        update(status, id);
        return false;
      }


    };

    struct CASPolicy {

      CASPolicy(size_t) {}

      typedef atomics::atomic_size_t counter_type;
      bool operator()(counter_type& counter, size_t) const {
        auto old = counter.load(atomics::memory_order_acquire);
        auto ret = counter.compare_exchange_weak(old, old + 1, atomics::memory_order_release, atomics::memory_order_relaxed);
        // ret = ret || counter.compare_exchange_weak(old, old + 1, atomics::memory_order_release, atomics::memory_order_relaxed);
        return ret;
      }

      void out(counter_inc&) {}
    };

    struct HTMFullPolicy : HTMPolicyBase {
      typedef atomics::atomic_size_t counter_type;

      using HTMPolicyBase::HTMPolicyBase;

      bool operator()(counter_type& counter, size_t id) {
        status_type status = UNINIT_STATUS;
        while(true) {
          status = _xbegin();
          if(status == _XBEGIN_STARTED) {
            auto tmp = counter.load(atomics::memory_order_relaxed);
            counter.store(tmp + 1, atomics::memory_order_relaxed);
            _xend();
            update(status, id);
            return true;
          }
        }
        update(status, id);
        return false;
      }
    };

    struct HTMFullBadPolicy : HTMPolicyBase {
      typedef atomics::atomic_size_t counter_type;

      using HTMPolicyBase::HTMPolicyBase;

      bool operator()(counter_type& counter, size_t id) {
        status_type status = _xbegin();
        if (status == _XBEGIN_STARTED) {
          auto tmp = counter.load(atomics::memory_order_relaxed);
          counter.store(tmp + 1, atomics::memory_order_relaxed);
          _xend();
          update(status, id);
          return true;
        }
        update(status, id);
        return false;
      }
    };


    struct FAAPolicy {
      typedef atomics::atomic_size_t counter_type;

      FAAPolicy(size_t) {}

      bool operator()(counter_type& counter, size_t) const {
        counter.fetch_add(1, atomics::memory_order_release);
        return true; // FAA always works.
      }

      void out(counter_inc&) {}
    };

#ifdef CDS_HTM_SUPPORT
    TEST_F(counter_inc, htm) { test<HTMPolicy>(); }
    TEST_F(counter_inc, htm_full) { test<HTMFullPolicy>(); }
    TEST_F(counter_inc, htm_bad) { test<HTMFullBadPolicy>(); }
#endif // CDS_HTM_SUPPORT
    TEST_F(counter_inc, cas) { test<CASPolicy>(); }
    TEST_F(counter_inc, faa) { test<FAAPolicy>(); }

    class dual_counter_inc : public counter_inc {
      protected:
        enum {
          cas_worker,
          htm_worker
        };
        static void SetUpTestCase() {
            counter_inc::SetUpTestCase();
            s_nThreadCount = (s_nThreadCount / 2) * 2;
        }

        void test() {
            cds_test::thread_pool &pool = get_pool();

            typename atomics::atomic_size_t nTotal{0};

            CASPolicy cas_incrementer(s_nThreadCount);
            HTMPolicy htm_incrementer(s_nThreadCount);

            pool.add(new Worker<CASPolicy>(pool, nTotal, cas_incrementer, *s_Topology), s_nThreadCount / 2);
            pool.add(new Worker<HTMPolicy>(pool, nTotal, htm_incrementer, *s_Topology), s_nThreadCount / 2);

            propout() << std::make_pair("work_thread", s_nThreadCount)
                      << std::make_pair("increment_count", s_nIncrementCount);

            std::chrono::milliseconds duration = pool.run();

            propout() << std::make_pair("duration", duration)
              << std::make_pair("nTotal", nTotal.load(atomics::memory_order_relaxed));

            // analyze result

            size_t nSuccess = 0;
            for (size_t threadNo = 0; threadNo < pool.size(); ++threadNo) {
              auto& thr = pool.get(threadNo);
              if (thr.type() == cas_worker) {
                nSuccess +=
                    static_cast<Worker<CASPolicy> &>(pool.get(threadNo)).m_nSuccess;
              } else {
                nSuccess +=
                    static_cast<Worker<HTMPolicy> &>(pool.get(threadNo)).m_nSuccess;
              }
            }

            EXPECT_EQ(nSuccess, nTotal);
            EXPECT_NE(0, nTotal);

            propout() << std::make_pair("total", nSuccess);
        }
    };


#ifdef CDS_HTM_SUPPORT
    TEST_F(dual_counter_inc, dual) { test(); }
#endif // CDS_HTM_SUPPORT



} // namespace
