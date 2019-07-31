#ifndef CDSLIB_CONTAINER_BAGS_H
#define CDSLIB_CONTAINER_BAGS_H

#include <algorithm>
#include <memory>
#include <array>

#include <cds/algo/atomic.h>
#include <cds/container/treiber_stack.h>
#include <cds/details/memkind_allocator.h>
#include <cds/details/marked_ptr.h>
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

        /*
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

            void reset(T& t, size_t ) {
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
            bool insert(T &t, size_t , First)
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
            bool extract(T &t, size_t )
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
            size_t size() const
            {
                auto size_ = m_counter.load(atomics::memory_order_acquire);
                assert(size_ >= 0);
                return size_;
            }
        };
        */

        template <class T>
        class IdBag
        {
        private:
            static_assert(std::is_pointer<T>::value, "");
            using ptr_t = cds::details::marked_ptr<typename std::remove_pointer<T>::type, 1>;
            using atomic_ptr_t = std::atomic<ptr_t>;

            using value_type = PaddedValue<atomic_ptr_t>;
            static constexpr size_t MAX_THREADS=44;
            std::array<value_type, MAX_THREADS> m_bag;
            TwicePaddedValue<std::atomic<size_t>> counter{0};
            TwicePaddedValue<std::atomic<bool>> exhaust{false};

        protected:
            const size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            IdBag(size_t ids) : m_size(ids)
            {
                assert(m_size <= MAX_THREADS);
            }
            void unsafe_insert(T t, size_t id) {
              auto& v = m_bag[id].value;
              v.store(ptr_t{t}, std::memory_order_relaxed);
            }
            void unsafe_extract(size_t id) {
              auto& v = m_bag[id].value;
              v.store(ptr_t{nullptr, 0}, std::memory_order_relaxed);
            }
            static ptr_t attempt_pop(atomic_ptr_t& v) {
              return v.exchange(ptr_t{nullptr, 1}, std::memory_order_acq_rel);
            }
            bool extract(T &t, size_t) {
              const size_t size = m_size;
              size_t index;
              ptr_t ptr;
              while((index = counter.value.fetch_add(1, std::memory_order_acq_rel)) < size) {
                if(index == size - 1) {
                  exhaust.value.store(true, std::memory_order_relaxed);
                  std::atomic_thread_fence(std::memory_order_seq_cst);
                }
                ptr = attempt_pop(m_bag[index].value);
                if(ptr.ptr() != nullptr) {
                  t = ptr.ptr();
                  return true;
                }
              }
              return false;
            }
            template <class First>
            bool insert(T t, size_t id, First) {
                assert(id < m_size);
                auto &v = m_bag[id].value;
                ptr_t old{nullptr, 0};
                return v.compare_exchange_strong(old, ptr_t{t}, std::memory_order_acq_rel);
            }
            bool empty() const {
              return exhaust.value.load(std::memory_order_acquire);
            }
            /*
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
        };

        template <class T>
        class HalfIdBag: public IdBag<T>
        {
          private:
            using base_type = IdBag<T>;

            size_t adjust_id(size_t id) {
              return (id < base_type::m_size) ? id : (id - base_type::m_size);
            }
          public:
            HalfIdBag(size_t ids) : base_type(ids / 2) {}

            void unsafe_insert(T t, size_t id) {
              base_type::unsafe_insert(t, adjust_id(id));
            }
            void unsafe_extract(size_t id) {
              base_type::unsafe_extract(adjust_id(id));
            }
            template <class First>
            bool insert(T t, size_t id, First)
            {
                return base_type::insert(t, adjust_id(id), First{});
            }

        };

        template <class T>
        class RandomIdBag
        {
        private:
            static_assert(std::is_pointer<T>::value, "");
            using ptr_t = cds::details::marked_ptr<typename std::remove_pointer<T>::type, 1>;
            using atomic_ptr_t = std::atomic<ptr_t>;

            using value_type = PaddedValue<atomic_ptr_t>;
            static constexpr size_t MAX_THREADS=44;
            std::array<value_type, MAX_THREADS> m_bag;
            TwicePaddedValue<std::atomic<size_t>> counter{0};
            TwicePaddedValue<std::atomic<bool>> exhaust{false};

        protected:
            const size_t m_size;

        public:
            static constexpr const size_t c_nHazardPtrCount = 0; ///< Count of hazard pointer required for the algorithm
            RandomIdBag(size_t ids) : m_size(ids)
            {
                assert(m_size <= MAX_THREADS);
            }
            void unsafe_insert(T t, size_t id) {
              auto& v = m_bag[id].value;
              v.store(ptr_t{t}, std::memory_order_relaxed);
            }
            void unsafe_extract(size_t id) {
              auto& v = m_bag[id].value;
              v.store(ptr_t{nullptr, 0}, std::memory_order_relaxed);
            }
            static ptr_t attempt_pop(atomic_ptr_t& v) {
              ptr_t empty{nullptr, 1};
              ptr_t value = v.load(std::memory_order_acquire);
              if(value == empty) {
                return empty;
              }
              return v.exchange(empty, std::memory_order_acq_rel);
            }
            static size_t seed() {
              return std::random_device{}();
            }
            bool extract(T &t, size_t) {
              thread_local std::minstd_rand rnd(seed());
              size_t size = m_size;
              std::uniform_int_distribution<> dist(0, size - 1);
              size_t index;
              ptr_t ptr;
              for(size_t i = 0; i < 3; ++i) {
                index = dist(rnd);
                ptr = attempt_pop(m_bag[index].value);
                if (ptr.ptr() != nullptr) {
                  t = ptr.ptr();
                  return true;
                }
              }
              if(empty()) {
                return false;
              }
              size >>= 1;
              for(index = 0; index < size; ++index) {
                ptr = attempt_pop(m_bag[index].value);
                if(ptr.ptr() != nullptr) {
                  t = ptr.ptr();
                  return true;
                }
              }
              if(empty()) {
                return false;
              }
              for(; index < size << 1; ++index) {
                ptr = attempt_pop(m_bag[index].value);
                if(ptr.ptr() != nullptr) {
                  t = ptr.ptr();
                  return true;
                }
              }
              if(empty()) {
                return false;
              }
              exhaust.value.store(true, std::memory_order_relaxed);
              std::atomic_thread_fence(std::memory_order_seq_cst);
              return false;
            }
            template <class First>
            bool insert(T t, size_t id, First) {
                assert(id < m_size);
                auto &v = m_bag[id].value;
                ptr_t old{nullptr, 0};
                return v.compare_exchange_strong(old, ptr_t{t}, std::memory_order_acq_rel);
            }
            bool empty() const {
              return exhaust.value.load(std::memory_order_acquire);
            }
        };

        /*
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
            StackBag(size_t ) {}
            static constexpr const size_t c_nHazardPtrCount = bag_type::c_nHazardPtrCount; ///< Count of hazard pointer required for the algorithm

            template <class First>
            bool insert(T &t, size_t , First)
            {
                return m_bag.push(std::move(t));
            }
            bool extract(T &t, size_t )
            {
                return m_bag.pop(t);
            }

            void reset(T &t, size_t )
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
        */

    } // namespace bags
}}    // namespace cds::container

#endif //#ifndef CDSLIB_CONTAINER_BAGS_H
