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

        template <class T>
        struct PaddedValue
        {
            T value;
            char pad1_[cds::c_nCacheLineSize - sizeof(value)];
        };

        template <class T>
        class SimpleBag
        {
        private:
            struct flagged_value {
              T value;
              atomics::atomic_bool flag{false};
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

            bool insert(T &t, size_t /*id*/)
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

        template <class T>
        class IdBag
        {
        private:
            static constexpr int INSERT = 0;
            static constexpr int EXTRACT = 1;
            static constexpr int EMPTY = 2;
            struct value
            {
                T value{};
                std::atomic<int> flag{INSERT};
            };
            using value_type = PaddedValue<value>;
            static constexpr size_t MAX_THREADS=40;
            std::array<value_type, MAX_THREADS> m_bag;
            PaddedValue<std::atomic<int>> status;
            size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            IdBag(size_t ids) : m_size(ids)
            {
                assert(m_size <= MAX_THREADS);
                status.value.store(INSERT, std::memory_order_relaxed);
            }
            bool insert(T &t, size_t id)
            {
                assert(id < m_size);
                auto &v = m_bag[id].value;
                // if(status.value.load(std::memory_order_acquire) != INSERT) {
                //   return false;
                // }
                // int old_flag = v.flag.load(std::memory_order_relaxed);
                // if(old_flag != INSERT) {
                //   return false;
                // }
                std::swap(t, v.value);
                // if(!v.flag.compare_exchange_strong(old_flag, EXTRACT, std::memory_order_release,
                //       std::memory_order_relaxed)) {
                //   std::swap(t, v.value);
                //   return false;
                // }
                v.flag.store(EXTRACT, std::memory_order_relaxed);
                return true;
            }
            bool extract(T &t, size_t id)
            {
                int current_status = status.value.load(std::memory_order_acquire);
                if(current_status == INSERT) {
                  if(status.value.compare_exchange_strong(current_status,
                        EXTRACT, std::memory_order_acquire)) {
                    current_status = EXTRACT;
                  }
                }
                if(current_status == EMPTY) {
                  return false;
                }
                assert(current_status != INSERT);
                auto pos = std::next(m_bag.begin(), id);
                auto last = std::next(m_bag.begin(), m_size);
                for(size_t i = 0; i < m_size; ++i, ++pos) {
                  if(pos == last) {
                    pos = m_bag.begin();
                  }
                  auto& value = pos->value;
                  if(value.flag.load(std::memory_order_relaxed) != EXTRACT) {
                    continue;
                  }
                  auto flag = value.flag.exchange(EMPTY, std::memory_order_acquire);
                  if(flag == EXTRACT) {
                    std::swap(t, value.value);
                    return true;
                  }
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

            bool insert(T &t, size_t /*id*/)
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
