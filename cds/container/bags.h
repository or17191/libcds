#ifndef CDSLIB_CONTAINER_BAGS_H
#define CDSLIB_CONTAINER_BAGS_H

#include <algorithm>
#include <memory>
#include <array>

#include <cds/algo/atomic.h>
#include <cds/container/treiber_stack.h>
#include <cds/details/memkind_allocator.h>
#include <cds/sync/htm.h>
#include <cds/gc/hp.h>

namespace cds { namespace container {
    namespace bags {
        inline void delay(size_t s) {
          volatile int x;
          for(size_t i = 0; i < s; ++i) {
            x = 0;
          }
        }

        template <class T, size_t PADDING=1>
        struct PaddedValue
        {
            T value;
            char pad1_[PADDING * cds::c_nCacheLineSize - sizeof(value)];

            template <class... Args>
            explicit PaddedValue(Args&&... args) : value(std::forward<Args>(args)...) {}
        } __attribute__((aligned (PADDING * cds::c_nCacheLineSize)));

        template <class T>
        struct TwicePaddedValue
        {
            char pad1_[cds::c_nCacheLineSize];
            T value;
            char pad2_[cds::c_nCacheLineSize - sizeof(value)];

            template <class... Args>
            explicit TwicePaddedValue(Args&&... args) : value(std::forward<Args>(args)...) {}
        } __attribute__((aligned (2 * cds::c_nCacheLineSize)));

        template <class T>
        class SimpleBag
        {
        private:
            struct flagged_value {
              atomics::atomic_bool flag{false};
              T value;
            };
            using value_type = PaddedValue<flagged_value>;
            atomics::atomic_int m_pushes{0};
            char pad1_[cds::c_nCacheLineSize - sizeof(m_pushes)];
            atomics::atomic_int m_pops{0};
            char pad2_[cds::c_nCacheLineSize - sizeof(m_pops)];
            static constexpr int NO_SIZE = -1;
            atomics::atomic_int m_real_size{NO_SIZE};
            char pad3_[cds::c_nCacheLineSize - sizeof(m_real_size)];
            static constexpr size_t MAX_THREADS=40;
            std::array<value_type, MAX_THREADS> m_bag;
            size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            SimpleBag(size_t ids) : m_size(ids) {
              assert(m_size <= MAX_THREADS);
            }

            void reset(T& t, size_t /*id*/) {
              assert(m_pushes.load(atomics::memory_order_relaxed) == 1);
              assert(m_pops.load(atomics::memory_order_relaxed) == 0);
              assert(m_real_size.load(atomics::memory_order_relaxed) == NO_SIZE);
              auto& cell = m_bag[0].value;
              assert(cell.flag.load(atomics::memory_order_relaxed) == true);
              std::swap(t, cell.value);
              cell.flag.store(false, atomics::memory_order_relaxed);
              m_pushes.store(0, atomics::memory_order_relaxed);
            }

            template <class First>
            bool insert(T &t, size_t /*id*/, First)
            {
                auto idx = m_pushes.fetch_add(1, atomics::memory_order_acq_rel);
                assert(idx < m_size);
                int real_size = m_real_size.load(atomics::memory_order_acquire);
                if (real_size == NO_SIZE) {
                  auto pops = m_pops.load(atomics::memory_order_acquire);
                  if (pops != 0) {
                    // If pops happen, read until you have size. Otherwise, read once.
                    while((real_size = m_real_size.load(atomics::memory_order_acquire)) == NO_SIZE) {
                      delay(10);
                    }
                  }
                }
                if (real_size != NO_SIZE && real_size <= idx) {
                  return false;
                }
                auto& cell = m_bag[idx].value;
                assert(!cell.flag.load(atomics::memory_order_acquire));
                std::swap(t, cell.value);
                cell.flag.store(true, atomics::memory_order_release);
                return true;
            }
            bool extract(T &t, size_t /*id*/)
            {
                auto idx = m_pops.fetch_add(1, atomics::memory_order_acq_rel);
                int real_size;
                if (idx == 0) {
                  auto pushes = m_pushes.load(atomics::memory_order_acquire);
                  assert(pushes <= m_size);
                  m_real_size.store(pushes, atomics::memory_order_release);
                  real_size = pushes;
                } else {
                  while((real_size = m_real_size.load(atomics::memory_order_acquire)) == NO_SIZE) {
                    delay(10);
                  }
                }
                assert(real_size != NO_SIZE);
                if(idx >= real_size) {
                  return false;
                }
                auto& cell = m_bag[idx].value;
                while(!cell.flag.load(atomics::memory_order_acquire));
                std::swap(t, cell.value);
                return true;
            }

            bool empty() const {
              auto pushes = m_pushes.load(atomics::memory_order_acquire);
              auto real_size = m_real_size.load(atomics::memory_order_acquire);
              auto pops = m_pops.load(atomics::memory_order_acquire);
              if (real_size == NO_SIZE) {
                return pushes == 0;
              } else {
                return pops >= real_size;
              }
            }
            /*
            size_t size() const
            {
                auto size_ = m_counter.load(atomics::memory_order_acquire);
                assert(size_ >= 0);
                return size_;
            }*/
        };
        static constexpr int INSERT = 0;
        static constexpr int EXTRACT = 1;
        static constexpr int EMPTY = 2;

        template <class T>
        class IdBag
        {
        private:
            struct value
            {
                std::atomic<int> flag{INSERT};
                T value{};
            };
            using value_type = PaddedValue<value>;
            static constexpr size_t MAX_THREADS=40;
            std::array<value_type, MAX_THREADS> m_bag;
            // TwicePaddedValue<std::atomic<int>> status{0};
            TwicePaddedValue<std::atomic<int>> status{INSERT};
            const size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            IdBag(size_t ids) : m_size(ids)
            {
                assert(m_size <= MAX_THREADS);
            }
            static int attempt_pop(T& t, value& v) {
              // int flag = v.flag.load(std::memory_order_relaxed);
              // if (flag == EMPTY) {
              //   return flag;
              // }
              int flag = v.flag.exchange(EMPTY, std::memory_order_release);
              // We still get a lot of EMPTYs here
              if(flag == EXTRACT) {
                t = std::move(v.value);
                return flag;
              }
              return flag;
            }
            bool extract(T &t, size_t id) {
              const size_t size = m_size;
              size_t index;
              while((index = status.value.fetch_add(1, std::memory_order_acquire)) < size) {
                if(attempt_pop(t, m_bag[index].value) == EXTRACT) {
                  return true;
                }
              }
              return false;
            }
            template <class First>
            bool insert(T &t, size_t id, First)
            {
                assert(id < m_size);
                auto &v = m_bag[id].value;
                int flag = INSERT;
                v.value = std::move(t);
                bool ret = v.flag.compare_exchange_strong(flag, EXTRACT, std::memory_order_acq_rel);
                if(ret) {
                  return true;
                } else {
                  t = std::move(v.value);
                  return false;
                }
            }
            /*
            template <class First>
            bool insert(T &t, const size_t id, First)
            {
                assert(id < m_size);
                auto &v = m_bag[id].value;
                int ret;
                while(true) {
                  if ((ret = _xbegin()) == _XBEGIN_STARTED) {
                    auto flag = v.flag.load(std::memory_order_relaxed);
                    if (flag != INSERT) {
                      _xabort(0x1);
                    }
                    v.flag.store(EXTRACT, std::memory_order_relaxed);
                    v.value = std::move(t);
                    _xend();
                    return true;
                  } else if (ret & _XABORT_EXPLICIT) {
                    break;
                  } else if (ret & _XABORT_CONFLICT) {
                    auto flag = v.flag.load(std::memory_order_relaxed);
                    if (flag != INSERT) {
                      break;
                    }
                  }
                }
                return false;
            }
            bool extract(T &t, size_t id) {
              const size_t size = m_size;
              int current_status = status.value.load(std::memory_order_acquire);
              if(current_status == EMPTY) {
                return false;
              }
              thread_local value_type* last_pos = nullptr;
              thread_local size_t last_i;
              const auto last = std::next(m_bag.data(), size);
              const auto first = m_bag.data();
              value_type* pos;
              size_t i;
              if(last_pos >= first && last_pos < last) {
                pos = last_pos;
                i = last_i;
              } else {
                pos = std::next(first, id);
                i = 0;
              }
              const size_t checkpoint = size >> 1;
              volatile size_t x;
              for(size_t current_i = 0; i < size; ++current_i, ++i, ++pos) {
                if(cds_unlikely(pos == last)) {
                  pos = first;
                }
                x %= size; // Adding improves timings...
                if(cds_unlikely(current_i == checkpoint)) {
                  current_i = 0;
                  if (status.value.load(std::memory_order_acquire) == EMPTY) {
                    last_pos = nullptr;
                    return false;
                  }
                }
                int result = attempt_pop(t, pos->value);
                if(result == EXTRACT) {
                  last_pos = pos;
                  last_i = i;
                  return true;
                }
              }
              current_status = status.value.load(std::memory_order_acquire);
              if(current_status != EMPTY) {
                status.value.store(EMPTY, std::memory_order_release);
              }
              last_pos = nullptr;
              return false;
            }
            */
            bool empty() const {
              return status.value.load(std::memory_order_acquire) >= m_size;
              //return status.value.load(std::memory_order_acquire) == EMPTY;
            }
        };

        template <class T>
        class ModIdBag
        {
        private:
            static constexpr int INSERT = 0;
            static constexpr int EXTRACT = 1;
            static constexpr int EMPTY = 2;
            static constexpr int INSERTING = 3;
            struct value
            {
                std::atomic<int> flag{INSERT};
                T value{};
            };
            using value_type = PaddedValue<value>;
            static constexpr size_t MAX_THREADS=20;
            std::array<value_type, MAX_THREADS> m_bag;
            PaddedValue<std::atomic<int>> status;
            size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            ModIdBag(size_t ids) : m_size((ids + 1) / 2)
            {
                assert(m_size <= MAX_THREADS);
                status.value.store(INSERT, std::memory_order_relaxed);
            }
            template <class First>
            bool insert(T &t, size_t id, First)
            {
                if (id >= m_size) id -= m_size;
                assert(id < m_size);
                auto &v = m_bag[id].value;
                int old_flag = v.flag.load(std::memory_order_relaxed);
                if(old_flag != INSERT ||
                   !v.flag.compare_exchange_strong(old_flag, INSERTING, std::memory_order_release,
                      std::memory_order_relaxed)) {
                  // This scenario is neglegible
                  return false;
                }
                std::swap(t, v.value);
                old_flag = v.flag.load(std::memory_order_relaxed);
                if(old_flag != INSERTING ||
                   !v.flag.compare_exchange_strong(old_flag, EXTRACT, std::memory_order_release,
                      std::memory_order_relaxed)) {
                  // This scenario is neglegible
                  std::swap(t, v.value);
                  return false;
                }
                return true;
            }
            bool extract(T &t, size_t id)
            {
                if (id >= m_size) id -= m_size;
                int current_status = status.value.load(std::memory_order_acquire);
                if(current_status == EMPTY) {
                  return false;
                }
                auto pos = std::next(m_bag.begin(), id);
                auto last = std::next(m_bag.begin(), m_size);
                for(size_t i = 0; i < m_size; ++i, ++pos) {
                  if(pos == last) {
                    pos = m_bag.begin();
                  }
                  if(i % (m_size / 2) == 0 ) {
                    if (status.value.load(std::memory_order_acquire) == EMPTY) {
                      return false;
                    }
                  }
                  auto& value = pos->value;
                  if(value.flag.load(std::memory_order_relaxed) == EMPTY) {
                    // This is pretty significant
                    continue;
                  }
                  auto flag = value.flag.exchange(EMPTY, std::memory_order_acquire);
                  // We still get a lot of EMPTYs here
                  if(flag == EXTRACT) {
                    std::swap(t, value.value);
                    return true;
                  }
                }
                current_status = status.value.load(std::memory_order_relaxed);
                if(current_status == EMPTY) {
                  return false;
                }
                status.value.store(EMPTY, std::memory_order_release);
                return false;
            }
            bool empty() const {
              return status.value.load(std::memory_order_acquire) == EMPTY;
            }
        };

        template <class T>
        class StackBag
        {
        private:

            struct inner_traits : treiber_stack::traits {
              using allocator = cds::details::memkind_allocator<T>;
            };
            using bag_type = TreiberStack<cds::gc::HP, T, inner_traits>;
            bag_type m_bag;

        public:
            StackBag(size_t /*ids*/) {}
            static constexpr const size_t c_nHazardPtrCount = bag_type::c_nHazardPtrCount; ///< Count of hazard pointer required for the algorithm

            template <class First>
            bool insert(T &t, size_t /*id*/, First)
            {
                return m_bag.push(std::move(t));
            }
            bool extract(T &t, size_t /*id*/)
            {
                return m_bag.pop(t);
            }

            void reset(T &t, size_t /*id*/)
            {
                auto res = m_bag.pop(t);
                (void)res;
                assert(res);
            }

            bool empty() const
            {
                return m_bag.empty();
            }
            size_t size() const
            {
                return m_bag.size();
            }
        };

    } // namespace bags
}}    // namespace cds::container

#endif //#ifndef CDSLIB_CONTAINER_BAGS_H
