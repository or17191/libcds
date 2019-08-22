// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_SB_BASKET_QUEUE_H
#define CDSLIB_CONTAINER_SB_BASKET_QUEUE_H

#include <memory>

#include <cds/algo/uuid.h>
#include <cds/details/memkind_allocator.h>
#include <cds/intrusive/basket_queue.h>
#include <cds/sync/htm.h>

namespace cds { namespace container {

    /// BasketQueue related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace sb_basket_queue {

        /// Atomics based insert policy

        template <size_t C>
        struct Constant
        {
            size_t operator()(size_t) const { return C; }
        };
        template <int A = 1, int B = 0>
        struct Linear
        {
            size_t operator()(size_t x) const { return A * x + B; }
        };
        template <class Latency = Linear<10>>
        struct atomics_insert
        {
            static inline void delay(size_t s)
            {
                volatile int x;
                for (size_t i = 0; i < s; ++i) {
                    x = 0;
                }
            }

            size_t m_latency;

            atomics_insert(size_t threads = 1) : m_latency(Latency{}(threads)) {}

            enum class InsertResult : uint8_t { NOT_NULL = 0,
                                                FAILED_INSERT = 1,
                                                SUCCESSFUL_INSERT = 2,
                                                RETRY = 3 };

            template <class MemoryModel, class MarkedPtr>
            InsertResult _(MarkedPtr old_node, MarkedPtr new_node, MarkedPtr &next_value) const
            {
                new_node->m_pNext.store(MarkedPtr{}, MemoryModel::memory_order_relaxed);
                auto &old = old_node->m_pNext;
                MarkedPtr pNext = old.load(MemoryModel::memory_order_acquire);
                if (pNext.ptr() != nullptr) {
                    next_value = pNext;
                    return InsertResult::FAILED_INSERT;
                }
                delay(m_latency);
                bool res = old.compare_exchange_strong(pNext, new_node, MemoryModel::memory_order_release, MemoryModel::memory_order_relaxed);
                if (!res) {
                    next_value = pNext;
                    return InsertResult::FAILED_INSERT;
                } else {
                    return InsertResult::SUCCESSFUL_INSERT;
                }
            }
        };

        template <class Latency = Linear<10>, class FinalLatency = Constant<30>>
        struct htm_insert : atomics_insert<>
        {
            static constexpr bool IS_HTM = true;

            size_t m_latency;
            size_t m_final_latency;

            htm_insert(size_t threads = 1) : m_latency(Latency{}(threads)),
                                             m_final_latency(FinalLatency{}(threads)) {}

            template <class MemoryModel, class MarkedPtr>
            InsertResult _(MarkedPtr old_node, const MarkedPtr new_node, MarkedPtr &new_value) const
            {
                auto &old = old_node->m_pNext;
                int ret;
                MarkedPtr pNext;
                constexpr size_t MAX_TXN = 5;
                for (int i = 0; i < MAX_TXN; ++i) {
                    if ((ret = _xbegin()) == _XBEGIN_STARTED) {
                        if (_xbegin() == _XBEGIN_STARTED) {
                            pNext = old.load(std::memory_order_relaxed);
                            if (pNext.ptr() != nullptr) {
                                _xabort(0x01);
                            }
                            delay(m_latency);
                            _xend();
                        }
                        old.store(new_node, std::memory_order_relaxed);
                        _xend();
                        return InsertResult::SUCCESSFUL_INSERT;
                    }
                    if (ret == 0) {
                        // Relatively uncommon
                        continue;
                    }
                    if ((ret & _XABORT_EXPLICIT) != 0) {
                        assert(_XABORT_CODE(ret) == 0x01);
                        new_value = old.load(std::memory_order_acquire);
                        return InsertResult::FAILED_INSERT;
                    }
                    const bool is_conflict = (ret & _XABORT_CONFLICT) != 0;
                    const bool is_nested = (ret & _XABORT_NESTED) != 0;
                    if (is_conflict && is_nested) {
                        delay(m_final_latency);
                        auto value = old.load(std::memory_order_acquire);
                        if (value.ptr() != nullptr) {
                            new_value = value;
                            return InsertResult::FAILED_INSERT;
                        }
                        //return InsertResult::RETRY;
                    }
                }
                pNext = old.load(MemoryModel::memory_order_acquire);
                if (pNext.ptr() != nullptr) {
                    new_value = pNext;
                    return InsertResult::FAILED_INSERT;
                }
                bool res = old.compare_exchange_strong(pNext, new_node,
                                                       MemoryModel::memory_order_release, MemoryModel::memory_order_relaxed);
                if (!res) {
                    new_value = pNext;
                    return InsertResult::FAILED_INSERT;
                } else {
                    return InsertResult::SUCCESSFUL_INSERT;
                }
            }
        };

        class non_atomic_counter
        {
        public:
            non_atomic_counter() = default;
            void reset() { m_inner = 0; }
            size_t get() const { return m_inner; }
            non_atomic_counter &operator++()
            {
                ++m_inner;
                return *this;
            }
            non_atomic_counter &operator+=(size_t other)
            {
                m_inner += other;
                return *this;
            }

        private:
            size_t m_inner{0};
        };
        template <typename Counter = non_atomic_counter>
        struct stat : public intrusive::basket_queue::stat<Counter>
        {
            using base_type = intrusive::basket_queue::stat<Counter>;
            using counter_type = typename base_type::counter_type;
            counter_type m_FalseExtract; ///< Count the number of times an extract failed
            counter_type m_FreeNode;     ///< Count the number of times an extract failed
            counter_type m_SameNodeExtract;
            counter_type m_RetryInsert;
            counter_type m_NullBasket;

            void onFalseExtract() { ++m_FalseExtract; }
            void onFreeNode() { ++m_FreeNode; }
            void onSameNodeExtract() { ++m_SameNodeExtract; }
            void onRetryInsert() { ++m_RetryInsert; }
            void onNullBasket() { ++m_NullBasket; }

            base_type &base() { return static_cast<base_type &>(*this); }
            const base_type &base() const { return static_cast<const base_type &>(*this); }

            //@cond
            void reset()
            {
                base_type::reset();
                m_FalseExtract.reset();
                m_FreeNode.reset();
                m_SameNodeExtract.reset();
                m_RetryInsert.reset();
                m_NullBasket.reset();
            }

            stat &operator+=(stat const &s)
            {
                base_type::operator+=(s);
                m_FalseExtract += s.m_FalseExtract.get();
                m_FreeNode += s.m_FreeNode.get();
                m_SameNodeExtract += s.m_SameNodeExtract.get();
                m_RetryInsert += s.m_RetryInsert.get();
                m_NullBasket += s.m_NullBasket.get();
                return *this;
            }
            //@endcond
        };

        /// Dummy BasketQueue statistics - no counting is performed, no overhead. Support interface like \p basket_queue::stat
        struct empty_stat : intrusive::basket_queue::empty_stat
        {
            //@cond
            void onFalseExtract() const {}
            void onFreeNode() const {}
            void onSameNodeExtract() const {}
            void onRetryInsert() const {}
            void onNullBasket() const {}

            empty_stat &operator+=(empty_stat const &)
            {
                return *this;
            }
            //@endcond
        };

        /// BasketQueue default type traits
        struct traits : intrusive::basket_queue::traits
        {
            typedef empty_stat stat;
            /// Node allocator
            typedef cds::details::memkind_allocator<int> allocator;

            typedef atomics_insert<> insert_policy;
        };

        /// Metafunction converting option list to \p basket_queue::traits
        /**
            Supported \p Options are:
            - \p opt::hook - hook used. Possible hooks are: \p basket_queue::base_hook, \p basket_queue::member_hook, \p basket_queue::traits_hook.
                If the option is not specified, \p %basket_queue::base_hook<> is used.
            - \p opt::back_off - back-off strategy used, default is \p cds::backoff::empty.
            - \p opt::disposer - the functor used for dispose removed items. Default is \p opt::v::empty_disposer. This option is used
                when dequeuing.
            - \p opt::link_checker - the type of node's link fields checking. Default is \p opt::debug_check_link
            - \p opt::item_counter - the type of item counting feature. Default is \p cds::atomicity::empty_item_counter (item counting disabled)
                To enable item counting use \p cds::atomicity::item_counter
            - \p opt::stat - the type to gather internal statistics.
                Possible statistics types are: \p basket_queue::stat, \p basket_queue::empty_stat, user-provided class that supports \p %basket_queue::stat interface.
                Default is \p %basket_queue::empty_stat (internal statistics disabled).
            - \p opt::padding - padding for internal critical atomic data. Default is \p opt::cache_line_padding
            - \p opt::memory_model - C++ memory ordering model. Can be \p opt::v::relaxed_ordering (relaxed memory model, the default)
                or \p opt::v::sequential_consistent (sequentially consisnent memory model).

            Example: declare \p %BasketQueue with item counting and internal statistics
            \code
            typedef cds::intrusive::BasketQueue< cds::gc::HP, Foo,
                typename cds::intrusive::basket_queue::make_traits<
                    cds::intrusive::opt:hook< cds::intrusive::basket_queue::base_hook< cds::opt::gc<cds:gc::HP> >>,
                    cds::opt::item_counte< cds::atomicity::item_counter >,
                    cds::opt::stat< cds::intrusive::basket_queue::stat<> >
                >::type
            > myQueue;
            \endcode
        */
        template <typename... Options>
        struct make_traits
        {
#ifdef CDS_DOXYGEN_INVOKED
            typedef implementation_defined type; ///< Metafunction result
#else
            typedef typename cds::opt::make_options<
                typename cds::opt::find_type_traits<traits, Options...>::type, Options...>::type type;
#endif
        };
    } // namespace sb_basket_queue

    //@cond
    namespace details {
        template <typename GC, typename Bag, typename Traits>
        struct make_sb_basket_queue
        {
            typedef GC gc;
            typedef Bag bag_type;
            typedef Traits traits;

            struct node_type
            {
                typedef GC gc;
                typedef cds::details::marked_ptr<node_type, 1> marked_ptr;                     ///< marked pointer
                typedef typename gc::template atomic_marked_ptr<marked_ptr> atomic_marked_ptr; ///< atomic marked pointer specific for GC

                char pad_1[cds::c_nCacheLineSize];
                atomic_marked_ptr m_pNext __attribute__((aligned(cds::c_nCacheLineSize))){marked_ptr{}}; ///< pointer to the next node in the container
                uuid_type m_basket_id{0};
                char pad1[2 * cds::c_nCacheLineSize - sizeof(m_pNext) - sizeof(m_basket_id)];
                //std::atomic<bool> deleted __attribute__((aligned(cds::c_nCacheLineSize))){false};
                bag_type m_bag __attribute__((aligned(cds::c_nCacheLineSize)));

                node_type(size_t size)
                    : m_bag(size)
                {
                }
            };

            typedef typename std::allocator_traits<typename traits::allocator>::template rebind_alloc<node_type> allocator_type;
            //typedef typename traits::allocator::template rebind<node_type>::other allocator_type;
            typedef cds::details::Allocator<node_type, allocator_type> cxx_allocator;

            struct node_deallocator
            {
                void operator()(node_type *pNode)
                {
                    cxx_allocator().Delete(pNode);
                }
            };

            typedef node_deallocator disposer;
            typedef typename intrusive::single_link::get_link_checker<node_type, traits::link_checker>::type link_checker; ///< link checker
        };
    } // namespace details
    //@endcond

    template <typename GC, typename T, template <class> class Bag, typename Traits = sb_basket_queue::traits>
    class SBBasketQueue
    {
        //@cond
        typedef details::make_sb_basket_queue<GC, Bag<T *>, Traits> maker;
        //@endcond

    public:
        /// Rebind template arguments
        template <typename GC2, typename T2, template <class> class Bag2, typename Traits2>
        struct rebind
        {
            typedef SBBasketQueue<GC2, T2, Bag2, Traits2> other; ///< Rebinding result
        };

    public:
        typedef GC gc;         ///< Garbage collector
        typedef T value_type;  ///< Type of value to be stored in the queue
        typedef Traits traits; ///< Queue's traits

        typedef typename traits::back_off back_off;            ///< Back-off strategy used
        typedef typename maker::allocator_type allocator_type; ///< Allocator type used for allocate/deallocate the nodes
        typedef typename traits::item_counter item_counter;    ///< Item counting policy used
        typedef typename traits::stat stat;                    ///< Internal statistics policy used
        typedef typename traits::memory_model memory_model;    ///< Memory ordering. See cds::opt::memory_model option

        typedef typename traits::insert_policy insert_policy;

        static constexpr size_t c_nHazardPtrCount = 0;

    protected:
        typedef typename maker::node_type node_type; ///< queue node type (derived from intrusive::basket_queue::node)

        //@cond
        typedef typename maker::cxx_allocator cxx_allocator;
        typedef typename maker::node_deallocator node_deallocator; // deallocate node
        typedef typename maker::link_checker link_checker;
        typedef typename node_type::marked_ptr marked_ptr;
        typedef typename node_type::atomic_marked_ptr atomic_marked_ptr;

        //@endcond

        //@cond
        atomic_marked_ptr m_pTail __attribute__((aligned(2 * cds::c_nCacheLineSize))){&m_Dummy};     ///< Queue's tail pointer (aligned)
        atomic_marked_ptr m_pHead __attribute__((aligned(2 * cds::c_nCacheLineSize))){&m_Dummy};     ///< Queue's head pointer (aligned)
        atomic_marked_ptr m_pCapacity __attribute__((aligned(2 * cds::c_nCacheLineSize))){&m_Dummy}; ///< Queue's tail pointer (aligned)
        node_type m_Dummy __attribute__((aligned(2 * cds::c_nCacheLineSize)));                       ///< dummy node
        typename opt::details::apply_padding<node_type, traits::padding>::padding_type pad3_;
        item_counter m_ItemCounter; ///< Item counter
        stat m_Stat;                ///< Internal statistics
        size_t const m_nMaxHops = 3;
        size_t const m_ids;
        const typename traits::insert_policy m_insert_policy;
        //@endcond

    protected:
        ///@cond
        node_type *alloc_node()
        {
            return cxx_allocator().New(m_ids);
        }
        static void free_node(node_type *p)
        {
            node_deallocator()(p);
        }

        struct node_disposer
        {
            void operator()(node_type *pNode)
            {
                free_node(pNode);
            }
        };
        typedef std::unique_ptr<node_type, node_disposer> scoped_node_ptr;
        //@endcond

    private:
        struct thread_data
        {
            scoped_node_ptr node{nullptr};
            size_t last_node{0};
            stat stats;
        } __attribute__((aligned(cds::c_nCacheLineSize)));
        std::unique_ptr<thread_data[]> m_nodes_cache;
        struct hazard_node
        {
            std::atomic<int> value{-1};
        } __attribute__((aligned(cds::c_nCacheLineSize)));
        std::vector<hazard_node> m_thread_hazard;
        bool stat_copied = false;

        struct dequeue_result
        {
            T *value;
            uuid_type basket_id;
        };

    public:
        /// Initializes empty queue
        SBBasketQueue(size_t ids)
            : m_Dummy(ids), m_ids(ids), m_insert_policy(ids),
              m_nodes_cache(new thread_data[2 * m_ids]()),
              m_thread_hazard(ids * 2)
        {
            // m_Dummy.m_basket_id = static_cast<cds::uuid_type>(-1);
            m_Dummy.m_basket_id = 1;
            m_Dummy.m_pNext.store(marked_ptr(nullptr), std::memory_order_relaxed);
        }

        /// Destructor clears the queue
        ~SBBasketQueue()
        {
            clear();

            node_type *pHead = m_pHead.load(memory_model::memory_order_relaxed).ptr();
            assert(pHead != nullptr);

            {
                node_type *pNext = pHead->m_pNext.load(memory_model::memory_order_relaxed).ptr();
                while (pNext) {
                    node_type *p = pNext;
                    pNext = pNext->m_pNext.load(memory_model::memory_order_relaxed).ptr();
                    p->m_pNext.store(marked_ptr(), memory_model::memory_order_relaxed);
                    dispose_node(p);
                }
                pHead->m_pNext.store(marked_ptr(), memory_model::memory_order_relaxed);
                //m_pTail.store( marked_ptr( pHead ), memory_model::memory_order_relaxed );
            }

            m_pHead.store(marked_ptr(nullptr), memory_model::memory_order_relaxed);
            m_pTail.store(marked_ptr(nullptr), memory_model::memory_order_relaxed);
            m_pCapacity.store(marked_ptr(nullptr), memory_model::memory_order_relaxed);

            dispose_node(pHead);
        }

        /// Enqueues \p val value into the queue.
        /**
            The function makes queue node in dynamic memory calling copy constructor for \p val
            and then it calls \p intrusive::BasketQueue::enqueue().
            Returns \p true if success, \p false otherwise.
        */
        bool enqueue(T *val, const size_t id)
        {
            auto &tcache = m_nodes_cache[id];
            if (do_enqueue(tcache, val, id)) {
                return true;
            }
            return false;
        }

        template <class... Args>
        bool push(Args &&... args)
        {
            return enqueue(std::forward<Args>(args)...);
        }

        /// Dequeues a value from the queue
        /**
            If queue is not empty, the function returns \p true, \p dest contains copy of
            dequeued value. The assignment operator for \p value_type is invoked.
            If queue is empty, the function returns \p false, \p dest is unchanged.
        */
        bool dequeue(T *&dest, const size_t tid, uuid_type *basket_id = nullptr)
        {

            dequeue_result res;
            if (do_dequeue(res, true, tid)) {
                // TSan finds a race between this read of \p src and node_type constructor
                // I think, it is wrong
                CDS_TSAN_ANNOTATE_IGNORE_READS_BEGIN;
                dest = res.value;
                if (basket_id != nullptr) {
                    *basket_id = res.basket_id;
                }
                CDS_TSAN_ANNOTATE_IGNORE_READS_END;
                return true;
            }
            return false;
        }

        /// Synonym for \p dequeue() function
        template <class... Args>
        bool pop(Args &&... args)
        {
            return dequeue(std::forward<Args>(args)...);
        }

        bool empty()
        {
            dequeue_result res;
            return !do_dequeue(res, false, 0);
        }
        void clear()
        {
            dequeue_result res;
            while (do_dequeue(res, true, 0))
                ;
        }

        /// Returns queue's item count
        /** \copydetails cds::intrusive::BasketQueue::size()
        */
        size_t size() const
        {
            return m_ItemCounter.value();
        }

        /// Returns reference to internal statistics
        const stat &statistics()
        {
            if (!stat_copied) {
                stat_copied = true;
                for (size_t i = 0; i < m_ids * 2; ++i) {
                    m_Stat += m_nodes_cache[i].stats;
                }
            }
            return m_Stat;
        }

    private:
        static bool is_empty(marked_ptr p)
        {
            return p->m_bag.empty();
        }

        marked_ptr protect(atomic_marked_ptr &p, size_t id)
        {
            auto &haz = m_thread_hazard[id].value;
            marked_ptr p1, p2;
            p1 = p.load(std::memory_order_acquire);
            while (true) {
                haz.store(p1->m_basket_id, std::memory_order_seq_cst);
                p2 = p.load(std::memory_order_acquire);
                if (p1 == p2) {
                    break;
                }
                p1 = p2;
            }
            return p1;
        }

        void release(size_t id)
        {
            auto &haz = m_thread_hazard[id].value;
            haz.store(-1, std::memory_order_release);
        }

        static bool allowd_to_advance(std::atomic<int> &haz, int new_id)
        {
            int old_id = haz.load(std::memory_order_acquire);
            return old_id <= new_id && old_id != -1;
        }

        marked_ptr assign(marked_ptr p, size_t id)
        {
            auto &haz = m_thread_hazard[id].value;
            int new_id = p->m_basket_id;
            assert(allowd_to_advance(haz, new_id));
            haz.store(new_id, std::memory_order_acq_rel);
            return p;
        }

        marked_ptr assign(atomic_marked_ptr &p, size_t id)
        {
            auto v = p.load(std::memory_order_acquire);
            return assign(v, id);
        }

        bool do_enqueue(thread_data &th, T *val, const size_t id)
        {
            using InsertResult = typename insert_policy::InsertResult;
            auto &tstat = m_nodes_cache[id].stats;
            auto &node_ptr = th.node;

            back_off bkoff;

            marked_ptr t = protect(m_pTail, id);

            while (true) {
                if (!node_ptr) {
                    node_ptr.reset(alloc_node());
                    assert(node_ptr.get() != nullptr);
                    link_checker::is_empty(node_ptr.get());
                    // node_ptr->m_basket_id = uuid();
                }

                marked_ptr pNext{};
                node_ptr->m_basket_id = t->m_basket_id + 1;
                node_ptr->m_pNext.store(marked_ptr{}, std::memory_order_relaxed);
                node_ptr->m_bag.unsafe_insert(val, id);
                typename insert_policy::InsertResult res;
                int i = 0;
                pNext = node_ptr->m_pNext.load(std::memory_order_acquire);
                if (pNext.ptr() != nullptr) {
                    res = InsertResult::NOT_NULL;
                } else {
                    // do {
                    res = m_insert_policy.template _<memory_model>(t, marked_ptr(node_ptr.get()), pNext);
                    //  if(res == InsertResult::RETRY) {
                    //    tstat.onRetryInsert();
                    //  } else {
                    //    if( i > 0 && res == InsertResult::NOT_NULL) {
                    //      res = InsertResult::FAILED_INSERT;
                    //    }
                    //    break;
                    //  }
                    //  ++i;
                    //} while(true);
                }

                if (res == InsertResult::SUCCESSFUL_INSERT) {
                    auto node = node_ptr.release();
                    auto copy_t = t;
                    if (!m_pTail.compare_exchange_strong(copy_t, marked_ptr(node), memory_model::memory_order_release, atomics::memory_order_relaxed))
                        tstat.onAdvanceTailFailed();
                    // if(cds_unlikely(th.last_node == node->m_basket_id)) {
                    //   std::stringstream s;
                    //   s << "My bag " << std::hex << th.last_node << ' ' << node->m_basket_id << ' ' << id;
                    //   throw std::logic_error(s.str());
                    // };
                    //th.last_node = node->m_basket_id;
                    break;
                } else if (res == insert_policy::InsertResult::FAILED_INSERT) {
                    node_ptr->m_bag.unsafe_extract(id);
                    // Try adding to basket
                    tstat.onTryAddBasket();

                    // add to the basket
                    bkoff();
                    // if(cds_unlikely(th.last_node == node->m_basket_id)) {
                    //   std::stringstream s;
                    //   s << "Other bag " << std::hex << th.last_node << ' ' << node->m_basket_id << ' ' << id;
                    //   throw std::logic_error(s.str());
                    // }
                    if (cds_likely(pNext->m_bag.insert(val, id, std::false_type{}))) {
                        tstat.onAddBasket();
                        // th.last_node = node->m_basket_id;
                        break;
                    }
                }

                {
                    marked_ptr pNext;
                    while ((pNext = t->m_pNext.load(std::memory_order_acquire)).ptr() != nullptr) {
                        t = pNext;
                    }
                    advance_node(m_pTail, t);
                }

                tstat.onEnqueueRace();
            }

            ++m_ItemCounter;
            tstat.onEnqueue();

            release(id);

            return true;
        }

        static bool txn_cas(atomic_marked_ptr &ptr, marked_ptr old_value, marked_ptr new_value)
        {
            int ret;
            while (ptr.load(std::memory_order_relaxed) == old_value) {
                if ((ret = _xbegin()) == _XBEGIN_STARTED) {
                    if (ptr.load(std::memory_order_relaxed) != old_value) {
                        _xabort(0x1);
                    }
                    ptr.store(new_value, std::memory_order_relaxed);
                    _xend();
                    return true;
                }
                if (ret & _XABORT_EXPLICIT) {
                    return false;
                }
            }
            return false;
        }

        static bool atomic_cas(atomic_marked_ptr &ptr, marked_ptr old_value, marked_ptr new_value)
        {
            if (ptr.load(std::memory_order_relaxed) != old_value) {
                return false;
            }
            return ptr.compare_exchange_strong(old_value, new_value,
                                               memory_model::memory_order_acq_rel, atomics::memory_order_acquire);
        }

        static marked_ptr txn_test_and_set(atomic_marked_ptr &ptr, marked_ptr new_value)
        {
            int ret;
            marked_ptr old_value;
            while (true) {
                if ((ret = _xbegin()) == _XBEGIN_STARTED) {
                    old_value = ptr.load(std::memory_order_relaxed);
                    if (cds_likely(old_value == new_value)) {
                        _xabort(0x1);
                    }
                    ptr.store(new_value, std::memory_order_relaxed);
                    _xend();
                    return old_value;
                }
                if (ret & _XABORT_EXPLICIT) {
                    return new_value;
                }
            }
            return new_value;
        }

        static marked_ptr atomic_test_and_set(atomic_marked_ptr &ptr, marked_ptr new_value)
        {
            marked_ptr old_value = ptr.load(std::memory_order_relaxed);
            if (old_value == new_value) {
                return new_value;
            }
            old_value = ptr.exchange(new_value, std::memory_order_acquire);
            return old_value;
        }

        bool advance_node(atomic_marked_ptr &ptr, marked_ptr newValue)
        {
            int ret;
            int new_id = newValue->m_basket_id;
            marked_ptr oldValue;
            while (true) {
                if ((ret = _xbegin()) == _XBEGIN_STARTED) {
                    oldValue = ptr.load(std::memory_order_relaxed);
                    if (oldValue->m_basket_id >= new_id) {
                        _xabort(0x1);
                    }
                    ptr.store(newValue, std::memory_order_relaxed);
                    _xend();
                    return true;
                }
                if (ret & _XABORT_EXPLICIT) {
                    return false;
                }
            }
            return false;
        }

        void free_chain(size_t id)
        {
            auto &tstat = m_nodes_cache[id + m_ids].stats;
            marked_ptr head = txn_test_and_set(m_pCapacity, marked_ptr(nullptr));
            if (!head.ptr()) {
                return;
            }
            int min_id = std::accumulate(m_thread_hazard.begin(), m_thread_hazard.end(),
                                         std::numeric_limits<int>::max(),
                                         [](int state, hazard_node &element) -> int {
                                             int v = element.value.load(std::memory_order_acquire);
                                             return (v != -1 && v < state) ? v : state;
                                         });
            assert(min_id != -1);
            typename maker::disposer disposer;
            while (head->m_basket_id < min_id) {
                auto tmp = head->m_pNext.load(std::memory_order_relaxed);
                assert(is_empty(head));
                node_type *p = head.ptr();
                if (p != &m_Dummy) {
                    clear_links(p);
                    disposer(p);
                    tstat.onFreeNode();
                }
                head = tmp;
            }
            m_pCapacity.store(head, std::memory_order_release);
        }

        void dispose_node(node_type *p)
        {
            using disposer = typename maker::disposer;
            if (p != &m_Dummy) {
                struct internal_disposer
                {
                    void operator()(node_type *p)
                    {
                        assert(p != nullptr);
                        clear_links(p);
                        disposer()(p);
                    }
                };
                gc::template retire<internal_disposer>(p);
            }
        }

        bool do_dequeue(dequeue_result &res, bool bDeque, const size_t id)
        {
            // Note:
            // If bDeque == false then the function is called from empty method and no real dequeuing operation is performed
            auto &tcache = m_nodes_cache[id + m_ids];
            auto &tstat = tcache.stats;

            back_off bkoff;

            marked_ptr h = protect(m_pHead, id + m_ids);
            marked_ptr iter(h);
            marked_ptr pNext = iter->m_pNext.load(std::memory_order_acquire);

            size_t hops = 0;

            auto loop_iteration = [&iter, &pNext, &hops] {
                iter = pNext;
                pNext = iter->m_pNext.load(std::memory_order_acquire);
                ++hops;
            };

            while (true) {
                while (pNext.ptr() && is_empty(iter)) {
                    loop_iteration();
                }

                if (pNext.ptr() == nullptr && is_empty(iter)) {
                    pNext = iter->m_pNext.load(std::memory_order_acquire);
                    if (pNext.ptr()) {
                        loop_iteration();
                        continue;
                    }
                    if (hops >= m_nMaxHops && advance_node(m_pHead, iter)) {
                        free_chain(id);
                    }
                    tstat.onEmptyDequeue();
                    release(id + m_ids);
                    return false;
                }

                if (!bDeque) {
                    // Not sure how thread safe that is
                    // res.basket_id = pNext->m_basket_id;
                    release(id + m_ids);
                    return !iter->m_bag.empty();
                }
                if (iter->m_bag.extract(res.value, id)) {
                    if (hops >= m_nMaxHops && advance_node(m_pHead, iter)) {
                        free_chain(id);
                    }
                    res.basket_id = iter->m_basket_id;
                    if (res.basket_id == tcache.last_node) {
                        tstat.onSameNodeExtract();
                    }
                    tcache.last_node = res.basket_id;
                    break;
                } else {
                    if (pNext.ptr() == nullptr) {
                        pNext = iter->m_pNext.load(std::memory_order_acquire);
                    }
                    if (pNext.ptr() == nullptr) {
                        if (hops >= m_nMaxHops && advance_node(m_pHead, iter)) {
                            free_chain(id);
                        }
                        tstat.onEmptyDequeue();
                        release(id + m_ids);
                        return false;
                    }
                    tstat.onFalseExtract();
                    loop_iteration();
                }

                if (bDeque)
                    tstat.onDequeueRace();
                bkoff();
            }

            if (bDeque) {
                --m_ItemCounter;
                tstat.onDequeue();
            }

            release(id + m_ids);
            return true;
        }

        static void clear_links(node_type *pNode)
        {
            pNode->m_pNext.store(marked_ptr(nullptr), memory_model::memory_order_release);
        }
    };

}} // namespace cds::container

#endif // #ifndef CDSLIB_CONTAINER_SB_BASKET_QUEUE_H
