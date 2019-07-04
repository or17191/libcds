// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_SB_BASKET_QUEUE_H
#define CDSLIB_CONTAINER_SB_BASKET_QUEUE_H

#include <memory>

#include <cds/container/basket_queue.h>
#include <cds/container/details/base.h>
#include <cds/intrusive/basket_queue.h>

namespace cds { namespace container {

    /// BasketQueue related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace sb_basket_queue {

        /// Internal statistics
        template <typename Counter = cds::intrusive::basket_queue::stat<>::counter_type>
        using stat = cds::container::basket_queue::stat<Counter>;

        /// Dummy internal statistics
        typedef cds::container::basket_queue::empty_stat empty_stat;

        /// BasketQueue default type traits
        typedef cds::container::basket_queue::traits traits;

        template <typename... Options>
        using make_traits = cds::container::basket_queue::make_traits<Options...>;

    } // namespace sb_basket_queue

    //@cond
    namespace details {
        template <typename GC, typename Bag, typename Traits>
        struct make_sb_basket_queue
        {
            typedef GC gc;
            typedef Bag bag_type;
            typedef Traits traits;

            struct node_type : public intrusive::basket_queue::node<gc>
            {
                std::atomic<bool> deleted __attribute__((aligned(cds::c_nCacheLineSize))){false};
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

            struct intrusive_traits : public traits
            {
                typedef cds::intrusive::basket_queue::base_hook<opt::gc<gc>> hook;
                typedef node_deallocator disposer;
                static constexpr const cds::intrusive::opt::link_check_type link_checker = cds::intrusive::basket_queue::traits::link_checker;
            };

            typedef cds::intrusive::BasketQueue<gc, node_type, intrusive_traits> type;
            static constexpr const size_t c_nHazardPtrCount = type::c_nHazardPtrCount + bag_type::c_nHazardPtrCount; ///< Count of hazard pointer required for the algorithm
        };
    } // namespace details
    //@endcond

    template <typename GC, typename T, template <class> class Bag, typename Traits = basket_queue::traits>
    class SBBasketQueue
    {
        //@cond
        typedef details::make_sb_basket_queue<GC, Bag<T>, Traits> maker;
        typedef typename maker::type base_class;
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

        typedef typename base_class::back_off back_off;         ///< Back-off strategy used
        typedef typename maker::allocator_type allocator_type;  ///< Allocator type used for allocate/deallocate the nodes
        typedef typename base_class::item_counter item_counter; ///< Item counting policy used
        typedef typename base_class::stat stat;                 ///< Internal statistics policy used
        typedef typename base_class::memory_model memory_model; ///< Memory ordering. See cds::opt::memory_model option

        static constexpr const size_t c_nHazardPtrCount = maker::c_nHazardPtrCount; ///< Count of hazard pointer required for the algorithm

        typedef typename base_class::insert_policy insert_policy;

    protected:
        typedef typename maker::node_type node_type;           ///< queue node type (derived from intrusive::basket_queue::node)
        typedef typename base_class::node_type base_node_type; ///< queue node type (derived from intrusive::basket_queue::node)

        //@cond
        typedef typename maker::cxx_allocator cxx_allocator;
        typedef typename maker::node_deallocator node_deallocator; // deallocate node
        typedef typename base_class::node_traits node_traits;
        typedef typename base_class::link_checker link_checker;
        typedef typename base_node_type::marked_ptr marked_ptr;
        typedef typename base_node_type::atomic_marked_ptr atomic_marked_ptr;
        //@endcond

        struct dequeue_result
        {
            typename gc::template GuardArray<3> guards;
            value_type value;
            uuid_type basket_id;
        };

        //@cond
        atomic_marked_ptr m_pHead{&m_Dummy}; ///< Queue's head pointer (aligned)
        typename opt::details::apply_padding<atomic_marked_ptr, traits::padding>::padding_type pad1_;
        atomic_marked_ptr m_pTail{&m_Dummy}; ///< Queue's tail pointer (aligned)
        typename opt::details::apply_padding<atomic_marked_ptr, traits::padding>::padding_type pad2_;
        atomic_marked_ptr m_pCapacity{&m_Dummy}; ///< Queue's tail pointer (aligned)
        typename opt::details::apply_padding<atomic_marked_ptr, traits::padding>::padding_type pad4_;
        node_type m_Dummy; ///< dummy node
        typename opt::details::apply_padding<node_type, traits::padding>::padding_type pad3_;
        item_counter m_ItemCounter; ///< Item counter
        stat m_Stat;                ///< Internal statistics
        size_t const m_nMaxHops = 3;
        size_t const m_ids;
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
            cds::uuid_type last_node{0};
            char pad1_[cds::c_nCacheLineSize - sizeof(scoped_node_ptr) - sizeof(cds::uuid_type)];
        } __attribute__((aligned (cds::c_nCacheLineSize)));
        std::unique_ptr<thread_data[]> m_nodes_cache;
        struct padded_stat {
          stat value;
        } __attribute__((aligned (cds::c_nCacheLineSize)));
        std::vector<padded_stat> m_thread_stat;
        struct hazard_node {
          std::atomic<int> value{-1};
        } __attribute__((aligned (cds::c_nCacheLineSize)));
        std::vector<hazard_node> m_thread_hazard;
        bool stat_copied = false;

    public:
        /// Initializes empty queue
        SBBasketQueue(size_t ids)
            : m_Dummy(ids), m_ids(ids),
              m_nodes_cache(new thread_data[m_ids]()),
              m_thread_stat(ids * 2),
              m_thread_hazard(ids * 2)
        {
          // m_Dummy.m_basket_id = static_cast<cds::uuid_type>(-1);
          m_Dummy.m_basket_id = 1;
        }

        /// Destructor clears the queue
        ~SBBasketQueue()
        {
            clear();

            base_node_type *pHead = m_pHead.load(memory_model::memory_order_relaxed).ptr();
            assert(pHead != nullptr);

            {
                base_node_type *pNext = pHead->m_pNext.load(memory_model::memory_order_relaxed).ptr();
                while (pNext) {
                    base_node_type *p = pNext;
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
        template <class Arg>
        bool enqueue(Arg &&val, size_t id)
        {
            auto &p = m_nodes_cache[id];
            if (do_enqueue(p, std::forward<Arg>(val), id)) {
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
        bool dequeue(value_type &dest, size_t tid, uuid_type *basket_id = nullptr)
        {

            dequeue_result res;
            if (do_dequeue(res, true, tid)) {
                // TSan finds a race between this read of \p src and node_type constructor
                // I think, it is wrong
                CDS_TSAN_ANNOTATE_IGNORE_READS_BEGIN;
                dest = std::move(res.value);
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
            if(!stat_copied) {
              stat_copied = true;
              for(auto& e: m_thread_stat) {
                m_Stat += e.value;
              }
            }
            return m_Stat;
        }

    private:
        static bool is_deleted(marked_ptr p) {
          auto node_ptr = node_traits::to_value_ptr(p.ptr());
          return node_ptr->deleted.load(memory_model::memory_order_acquire);
        }
        static bool make_deleted(marked_ptr p) {
          auto node_ptr = node_traits::to_value_ptr(p.ptr());
          bool status = node_ptr->deleted.load(std::memory_order_relaxed);
          if(status != false) {
            return false;
          }
          return node_ptr->deleted.compare_exchange_strong(status, true, memory_model::memory_order_release, memory_model::memory_order_relaxed);
        }
        
        marked_ptr protect(atomic_marked_ptr& p, size_t id) {
          auto& haz = m_thread_hazard[id].value;
          marked_ptr p1, p2;
          p1 = p.load(std::memory_order_acquire);
          while(true) {
            haz.store(p1->m_basket_id, std::memory_order_seq_cst);
            p2 = p.load(std::memory_order_acquire);
            if(p1 == p2) {
              break;
            }
            p1 = p2;
          }
          return p1;
        }

        void release(size_t id) {
          auto& haz = m_thread_hazard[id].value;
          haz.store(-1, std::memory_order_release);
        }

        marked_ptr assign(marked_ptr p, size_t id) {
          auto& haz = m_thread_hazard[id].value;
          auto new_id = p->m_basket_id;
          assert(haz.load(std::memory_order_relaxed) <= new_id && haz.load(std::memory_order_relaxed) != -1);
          haz.store(new_id, std::memory_order_acq_rel);
          return p;
        }

        marked_ptr assign(atomic_marked_ptr& p, size_t id) {
          auto v = p.load(std::memory_order_acquire);
          return assign(v, id);
        }

        template <class Arg>
        bool do_enqueue(thread_data &th, Arg &&tmp_val, size_t id)
        {
            auto& tstat = m_thread_stat[id].value;
            auto& node_ptr = th.node;
            value_type val(std::forward<Arg>(tmp_val));
            base_node_type* pNew{nullptr};

            back_off bkoff;

            protect(m_pTail, id);

            while (true) {
                if(!node_ptr) {
                  node_ptr.reset(alloc_node());
                  assert(node_ptr.get() != nullptr);
                  // node_ptr->m_basket_id = uuid();
                }
                if(!pNew) {
                  pNew = node_traits::to_node_ptr(node_ptr.get());
                  link_checker::is_empty(pNew);
                }

                const marked_ptr t = assign(m_pTail, id);

                marked_ptr pNext{};
                pNew->m_basket_id = t->m_basket_id + 1;
                const auto res = insert_policy::template _<memory_model>(t, marked_ptr(pNew), pNext, m_ids);

                if ( res == insert_policy::InsertResult::SUCCESSFUL_INSERT) {
                    node_ptr.release();
                    auto node = node_traits::to_value_ptr(t.ptr());
                    auto copy_t = t;
                    if (!m_pTail.compare_exchange_strong(copy_t, marked_ptr(pNew), memory_model::memory_order_release, atomics::memory_order_relaxed))
                        tstat.onAdvanceTailFailed();
                    // if(cds_unlikely(th.last_node == node->m_basket_id)) {
                    //   std::stringstream s;
                    //   s << "My bag " << std::hex << th.last_node << ' ' << node->m_basket_id << ' ' << id;
                    //   throw std::logic_error(s.str());
                    // };
                    pNew = nullptr; // Need to do this after we update node_ptr
                    if (cds_likely(node->m_bag.insert(val, id, std::true_type{}))) {
                        th.last_node = node->m_basket_id;
                        break;
                    } else {
                        continue;
                    }
                } else if ( res == insert_policy::InsertResult::FAILED_INSERT ) {
                    // Try adding to basket
                    tstat.onTryAddBasket();

                    // add to the basket
                    bkoff();
                    auto node = node_traits::to_value_ptr(t.ptr());
                    // if(cds_unlikely(th.last_node == node->m_basket_id)) {
                    //   std::stringstream s;
                    //   s << "Other bag " << std::hex << th.last_node << ' ' << node->m_basket_id << ' ' << id;
                    //   throw std::logic_error(s.str());
                    // }
                    if (cds_likely(node->m_bag.insert(val, id, std::false_type{}))) {
                        tstat.onAddBasket();
                        // th.last_node = node->m_basket_id;
                        break;
                    } else {
                        continue;
                    }
                } else {
                    // Tail is misplaced, advance it
                    if (m_pTail.load(memory_model::memory_order_acquire) != t) {
                        tstat.onEnqueueRace();
                        bkoff();
                        continue;
                    }
                    pNext = t;

                    marked_ptr p;
                    bool bTailOk = true;
                    while ((p = pNext->m_pNext.load(memory_model::memory_order_acquire)).ptr() != nullptr) {
                        bTailOk = m_pTail.load(memory_model::memory_order_relaxed) == t;
                        if (!bTailOk)
                            break;

                        if (pNext->m_pNext.load(memory_model::memory_order_relaxed) != p)
                            continue;
                        pNext = p;
                    }
                    auto copy_t = t;
                    if (!bTailOk || !m_pTail.compare_exchange_weak(copy_t, marked_ptr(pNext.ptr()), memory_model::memory_order_release, atomics::memory_order_relaxed))
                        tstat.onAdvanceTailFailed();

                    tstat.onBadTail();
                }

                tstat.onEnqueueRace();
            }

            ++m_ItemCounter;
            tstat.onEnqueue();

            release(id);

            return true;
        }

        void free_chain(marked_ptr head, marked_ptr newHead, size_t id)
        {
            // "head" and "newHead" are guarded
            if (!m_pHead.compare_exchange_strong(head, marked_ptr(newHead.ptr()), memory_model::memory_order_release, atomics::memory_order_acquire)) {
              assign(head, id + m_ids);
              return;
            }
            assign(newHead, id + m_ids);
            head = m_pCapacity.load(std::memory_order_relaxed);
            if(head == nullptr) {
              return;
            }
            head = m_pCapacity.exchange(marked_ptr{nullptr}, std::memory_order_acquire);
            if(head == nullptr) {
              return;
            }
            int min_id = std::accumulate(m_thread_hazard.begin(), m_thread_hazard.end(),
                std::numeric_limits<int>::max(),
                [](int state, hazard_node& element) -> int{
              int v = element.value.load(std::memory_order_acquire);
              return (v != -1 && v < state) ? v : state;
            });
            assert(min_id != -1);
            int current_id = head->m_basket_id;
            typename base_class::disposer disposer;
            size_t free_count = 0;
            while(current_id < min_id && head.ptr() != newHead.ptr()) {
              auto tmp = head->m_pNext.load(std::memory_order_relaxed);
              assert(is_deleted(head));
              node_type* p = node_traits::to_value_ptr(head.ptr());
              if(p != &m_Dummy) {
                clear_links(p);
                disposer(p);
                free_count++;
              }
              head = tmp;
            }
            m_pCapacity.store(head, std::memory_order_release);
        }

        void dispose_node(base_node_type *p)
        {
            using disposer = typename base_class::disposer;
            if (p != &m_Dummy) {
                struct internal_disposer
                {
                    void operator()(node_type *p)
                    {
                        assert(p != nullptr);
                        clear_links(node_traits::to_node_ptr(p));
                        disposer()(p);
                    }
                };
                gc::template retire<internal_disposer>(node_traits::to_value_ptr(p));
            }
        }

        bool do_dequeue(dequeue_result &res, bool bDeque, size_t id)
        {
            // Note:
            // If bDeque == false then the function is called from empty method and no real dequeuing operation is performed
            auto& tstat = m_thread_stat[id + m_ids].value;

            back_off bkoff;

            marked_ptr h;
            marked_ptr pNext;

            h = protect(m_pHead, id + m_ids);

            while (true) {
                // h = node_traits::to_value_ptr(m_pHead.load(std::memory_order_acquire).ptr());
                pNext = node_traits::to_value_ptr(h->m_pNext.load(std::memory_order_acquire).ptr());

                marked_ptr iter(h);
                size_t hops = 0;

                while (pNext.ptr() && is_deleted(iter)) {
                    iter = pNext;
                    pNext = pNext->m_pNext.load(std::memory_order_acquire);
                    ++hops;
                }

                if (pNext.ptr() == nullptr) {
                    if (hops >= m_nMaxHops) {
                      free_chain(h, iter, id);
                    }
                    tstat.onEmptyDequeue();
                    release(id + m_ids);
                    return false;
                } else if (!is_deleted(iter)) {
                    auto value_node = node_traits::to_value_ptr(*iter.ptr());
                    if (bDeque) {
                        if (value_node->m_bag.extract(res.value, id)) {
                            if (hops >= m_nMaxHops) {
                              free_chain(h, iter, id);
                            }
                            //res.basket_id = value_node->m_basket_id;
                            break;
                        } else {
                            // empty node, mark it as deleted.
                            if (make_deleted(iter)) {
                                free_chain(h, pNext, id);
                            }
                            h = assign(pNext, id + m_ids);
                        }
                    } else {
                        // Not sure how thread safe that is
                        // res.basket_id = pNext->m_basket_id;
                        release(id + m_ids);
                        return !value_node->m_bag.empty();
                    }
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

        static void clear_links(base_node_type *pNode)
        {
            pNode->m_pNext.store(marked_ptr(nullptr), memory_model::memory_order_release);
        }
    };

}} // namespace cds::container

#endif // #ifndef CDSLIB_CONTAINER_SB_BASKET_QUEUE_H
