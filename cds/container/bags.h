#ifndef CDSLIB_CONTAINER_BAGS_H
#define CDSLIB_CONTAINER_BAGS_H

#include <algorithm>
#include <memory>
#include <array>

#include <cds/algo/atomic.h>
#include <cds/container/treiber_stack.h>
#include <cds/details/memkind_allocator.h>
#include <cds/gc/hp.h>

namespace cds { namespace container {
    namespace bags {
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
            bool insert(T &t, size_t /*id*/)
            {
                auto idx = m_pushes.fetch_add(1, atomics::memory_order_acq_rel);
                assert(idx < m_size);
                auto real_size = m_real_size.load(atomics::memory_order_acquire);
                if (real_size != NO_SIZE && real_size <= idx) {
                  return false;
                }
                auto& cell = m_bag[idx].value;
                assert(!cell.flag.load(atomics::memory_order_acquire));
                std::swap(t, cell.value);
                cell.flag.store(true, atomics::memory_order_release);
                return true;
            }
            bool extract(T &t)
            {
                int real_size = m_real_size.load(atomics::memory_order_acquire);
                if (real_size == NO_SIZE) {
                  auto pushes = m_pushes.load(atomics::memory_order_acquire);
                  assert(pushes <= m_size);
                  if(m_real_size.compare_exchange_strong(real_size, pushes, atomics::memory_order_acq_rel)) {
                    real_size = pushes;
                  }
                }
                auto idx = m_pops.fetch_add(1, atomics::memory_order_acquire);
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
            struct value
            {
                T value;
                bool flag;
            };
            using value_type = PaddedValue<value>;
            static constexpr size_t MAX_THREADS=40;
            std::array<value_type, MAX_THREADS> m_bag;
            size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            IdBag(size_t ids) : m_size(ids)
            {
                assert(m_size <= MAX_THREADS);
                for (size_t i = 0; i < m_size; ++i) {
                    m_bag[i].value.flag = false;
                }
            }
            bool insert(T &t, size_t id)
            {
                assert(id < m_size);
                auto &v = m_bag[id];
                assert(v.value.flag == false);
                v.value.flag = true;
                std::swap(t, v.value.value);
                return true;
            }
            bool extract(T &t)
            {
                auto first = m_bag.begin();
                auto last = m_bag.end();
                auto it = std::find_if(first, last, [](const value_type &e) { return e.value.flag; });
                if (it == last) {
                    return false;
                }
                it->value.flag = false;
                std::swap(t, it->value.value);
                return true;
            }

            bool empty() const
            {
                return std::none_of(m_bag.begin(), m_bag.end(),
                                    [](const value_type &v) { return v.value.flag; });
            }
            size_t size() const
            {
                return std::count_if(m_bag.begin(), m_bag.end(),
                                     [](const value_type &v) { return v.value.flag; });
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
            bool extract(T &t)
            {
                return m_bag.pop(t);
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
