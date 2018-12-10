#ifndef CDSLIB_CONTAINER_BAGS_H
#define CDSLIB_CONTAINER_BAGS_H

#include <algorithm>
#include <memory>

#include <cds/algo/atomic.h>
#include <cds/container/treiber_stack.h>
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
            using value_type = PaddedValue<T>;
            atomics::atomic_int m_counter;
            char pad2_[cds::c_nCacheLineSize - sizeof(m_counter)];
            std::unique_ptr<value_type[]> m_bag;
            size_t m_size;

        public:
            SimpleBag(size_t ids) : m_bag(new value_type[ids]()), m_counter(0), m_size(ids) {}
            bool insert(T &t, size_t /*id*/)
            {
                auto idx = m_counter.fetch_add(1, atomics::memory_order_relaxed);
                assert(idx < m_size);
                std::swap(t, m_bag[idx].value);
                return true;
            }
            bool extract(T &t)
            {
                auto idx = m_counter.fetch_sub(1, atomics::memory_order_relaxed) - 1;
                if (idx < 0) {
                    // Yes, there is a race here.
                    return false;
                }
                std::swap(t, m_bag[idx].value);
                return true;
            }

            bool empty() const { return m_counter.load(atomics::memory_order_acquire) <= 0; }
            size_t size() const
            {
                auto size_ = m_counter.load(atomics::memory_order_acquire);
                assert(size_ >= 0);
                return size_;
            }
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
            std::unique_ptr<value_type[]> m_bag;
            size_t m_size;

        public:
            IdBag(size_t ids) : m_bag(new value_type[ids]()), m_size(ids)
            {
                for (size_t i = 0; i < m_size; ++i) {
                    m_bag[i].value.flag = false;
                }
            }
            bool insert(T &t, size_t id)
            {
                assert(id < m_size);
                auto &v = m_bag[id];
                v.value.flag = true;
                std::swap(t, v.value.value);
                return true;
            }
            bool extract(T &t)
            {
                auto first = m_bag.get();
                auto last = std::next(first, m_size);
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
                return std::none_of(m_bag.get(), std::next(m_bag.get(), m_size),
                                    [](const value_type &v) { return v.value.flag; });
            }
            size_t size() const
            {
                return std::count_if(m_bag.get(), std::next(m_bag.get(), m_size),
                                     [](const value_type &v) { return v.value.flag; });
            }
        };

        template <class T>
        class StackBag
        {
        private:
            TreiberStack<cds::gc::HP, T> m_bag;

        public:
            StackBag(size_t /*ids*/) {}

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