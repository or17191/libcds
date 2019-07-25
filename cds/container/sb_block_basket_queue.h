
// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_SB_BLOCK_BASKET_QUEUE_H
#define CDSLIB_CONTAINER_SB_BLOCK_BASKET_QUEUE_H

#include <memory>

#include <cds/container/details/base.h>
#include <cds/container/sb_basket_queue.h>
#include <cds/details/marked_ptr.h>

namespace cds { namespace container {

    /// BasketQueue related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace sb_block_basket_queue {

        struct traits
        {
            /// Node allocator
            typedef CDS_DEFAULT_ALLOCATOR       node_allocator;
            typedef CDS_DEFAULT_ALLOCATOR       value_allocator;

            /// Back-off strategy
            typedef cds::backoff::empty         back_off;

            /// Item counting feature; by default, disabled. Use \p cds::atomicity::item_counter to enable item counting
            typedef atomicity::empty_item_counter   item_counter;

            /// Internal statistics (by default, disabled)
            /**
                Possible option value are: \p basket_queue::stat, \p basket_queue::empty_stat (the default),
                user-provided class that supports \p %basket_queue::stat interface.
            */
            typedef cds::container::wf_queue::empty_stat         stat;
            static constexpr const opt::link_check_type link_checker = opt::debug_check_link;

            /// C++ memory ordering model
            /**
                Can be \p opt::v::relaxed_ordering (relaxed memory model, the default)
                or \p opt::v::sequential_consistent (sequentially consisnent memory model).
            */
            typedef opt::v::relaxed_ordering    memory_model;

            typedef sb_basket_queue::atomics_insert<>    insert_policy;

            /// Padding for internal critical atomic data. Default is \p opt::cache_line_padding
            enum { padding = opt::cache_line_padding };

            enum { node_size = (1 << 10) - 2 };
            enum { max_spin = 100 };
            enum { max_patience = 10} ;
            
            struct max_grabage {
              size_t operator()(size_t n) const { return n * 2; }
            };
        };

        template <typename... Options>
        struct make_traits {
#   ifdef CDS_DOXYGEN_INVOKED
            typedef implementation_defined type;   ///< Metafunction result
#   else
            typedef typename cds::opt::make_options<
                typename cds::opt::find_type_traits< traits, Options... >::type
                , Options...
            >::type type;
#   endif
        };
        
    } // namespace sb_block_basket_queue

    //@cond
    namespace details {
        template <typename GC, typename T, typename Traits>
        struct make_sb_block_basket_queue
        {
            typedef GC gc;
            typedef T value_type;
            typedef Traits traits;
            typedef intmax_t id_t;
            typedef atomics::atomic<id_t> atomic_id_t;

            typedef typename cds::details::marked_ptr<value_type, 1> value_ptr;
            typedef typename gc::template atomic_marked_ptr<value_ptr> atomic_value_ptr;

            template <class MarkedPtr>
            static MarkedPtr bot() { return MarkedPtr{nullptr, 0}; }

            template <class MarkedPtr>
            static MarkedPtr top() { return MarkedPtr{nullptr, 1}; }

            template <class MarkedPtr>
            static bool is_bot(const MarkedPtr& p) { return p.ptr() == nullptr && p.bits() == 0; }

            template <class MarkedPtr>
            static bool is_top(const MarkedPtr& p) { return p.ptr() == nullptr && p.bits() == 1; }

            struct enq_t {
              atomic_id_t id;
              typename opt::details::apply_padding< decltype(id), traits::padding >::padding_type pad1_;
              atomic_value_ptr val;
              typename opt::details::apply_padding< decltype(val), traits::padding >::padding_type pad2_;

              enq_t(id_t i, value_ptr v): id(i), val(v) {}
            };

            typedef cds::details::marked_ptr<enq_t, 1> enq_ptr_t;

            struct deq_t {
              atomic_id_t id;
              typename opt::details::apply_padding< decltype(id), traits::padding >::padding_type pad1_;
              atomic_id_t idx;
              typename opt::details::apply_padding< decltype(idx), traits::padding >::padding_type pad2_;

              deq_t(id_t i, id_t ix): id(i), idx(ix) {}
            };

            typedef cds::details::marked_ptr<deq_t, 1> deq_ptr_t;

            typedef typename std::allocator_traits<typename traits::value_allocator>::template rebind_alloc<T> value_allocator_type;
            //typedef typename traits::allocator::template rebind<node_type>::other allocator_type;
            typedef cds::details::Allocator<value_type, value_allocator_type> value_cxx_allocator;

            struct value_deallocator
            {
                void operator()(T *pValue)
                {
                    value_cxx_allocator().Delete(pValue);
                }
            };


            struct cell_t {
              atomic_value_ptr val{bot<value_ptr>()};
              typename opt::details::apply_padding< decltype(val), traits::padding >::padding_type pad1_;
              typename gc::template atomic_marked_ptr<enq_ptr_t> enq{bot<enq_ptr_t>()};
              typename opt::details::apply_padding< decltype(enq), traits::padding >::padding_type pad2_;
              typename gc::template atomic_marked_ptr<deq_ptr_t> deq{bot<deq_ptr_t>()};
              typename opt::details::apply_padding< decltype(deq), traits::padding >::padding_type pad3_;
              size_t basket_id{0};

              cell_t() = default;
            };

            struct node_type;

            typedef cds::details::marked_ptr<node_type, 1> node_ptr;
            typedef typename gc::template atomic_marked_ptr<node_ptr> atomic_node_ptr;

            struct node_type
            {
                atomic_node_ptr next{nullptr};
                typename opt::details::apply_padding< decltype(next), traits::padding >::padding_type pad1_;
                id_t id{0};
                typename opt::details::apply_padding< decltype(id), traits::padding >::padding_type pad2_;
                std::array<cell_t, traits::node_size> cells{};
                typename opt::details::apply_padding< decltype(cells), traits::padding >::padding_type pad3_;

                node_type() = default;
            };


            typedef typename std::allocator_traits<typename traits::node_allocator>::template rebind_alloc<node_type> node_allocator_type;
            //typedef typename traits::allocator::template rebind<node_type>::other allocator_type;
            typedef cds::details::Allocator<node_type, node_allocator_type> node_cxx_allocator;

            struct node_deallocator
            {
                void operator()(node_type *pNode)
                {
                    node_cxx_allocator().Delete(pNode);
                }
            };

            struct handle_type {
              enq_t Er{0, bot<value_ptr>()};
              typename opt::details::apply_padding< decltype(Er), traits::padding >::padding_type pad1_;
              deq_t Dr{0, -1};
              typename opt::details::apply_padding< decltype(Dr), traits::padding >::padding_type pad2_;
              handle_type* Eh{nullptr};
              typename opt::details::apply_padding< decltype(Eh), traits::padding >::padding_type pad3_;
              std::unique_ptr<node_type, node_deallocator> spare{nullptr};
              typename opt::details::apply_padding< decltype(spare), traits::padding >::padding_type pad4_;
              atomic_id_t hzd_node_id{-1};
              typename opt::details::apply_padding< decltype(hzd_node_id), traits::padding >::padding_type pad5_;

              handle_type * next{nullptr};
              atomic_node_ptr Ep{nullptr};
              id_t enq_node_id{-1};
              atomic_node_ptr Dp{nullptr};
              id_t deq_node_id{-1};
              id_t Ei{0};
              handle_type * Dh{nullptr};
              size_t delay{0};
              size_t id{0};

              handle_type() = default;
              handle_type(const handle_type&) = delete;
            };
        };
    } // namespace details
    //@endcond

    template <typename GC, typename T, typename Traits = sb_block_basket_queue::traits>
    class SBBlockBasketQueue : details::make_sb_block_basket_queue<GC, T, Traits>
    {
    private:
        typedef typename details::make_sb_block_basket_queue<GC, T, Traits> maker;
    public:
        /// Rebind template arguments
        template <typename GC2, typename T2, typename Traits2>
        struct rebind
        {
            typedef SBBlockBasketQueue<GC2, T2, Traits2> other; ///< Rebinding result
        };

    public:
        typedef GC gc;         ///< Garbage collector
        typedef T value_type;  ///< Type of value to be stored in the queue
        typedef Traits traits; ///< Queue's traits

        typedef typename traits::back_off back_off;         ///< Back-off strategy used
        typedef typename traits::item_counter item_counter; ///< Item counting policy used
        typedef typename traits::stat stat;                 ///< Internal statistics policy used
        typedef typename traits::memory_model memory_model; ///< Memory ordering. See cds::opt::memory_model option
        typedef typename traits::insert_policy insert_policy;
        typedef typename maker::id_t id_t;
        typedef typename maker::atomic_id_t atomic_id_t;

        static constexpr const size_t c_nHazardPtrCount = 2; ///< Count of hazard pointer required for the algorithm

    protected:
        typedef typename maker::node_type node_type;           ///< queue node type (derived from intrusive::basket_queue::node)
        typedef typename maker::node_ptr node_ptr;           ///< queue node type (derived from intrusive::basket_queue::node)
        typedef typename maker::handle_type handle_type;           

        typedef typename maker::value_ptr value_ptr;
        typedef typename maker::deq_ptr_t deq_ptr_t;
        typedef typename maker::enq_ptr_t enq_ptr_t;

        //@cond
        atomic_id_t Ei; ///< Queue's head pointer (aligned)
        typename opt::details::apply_padding<decltype(Ei), traits::padding>::padding_type pad1_;
        atomic_id_t Di; ///< Queue's tail pointer (aligned)
        typename opt::details::apply_padding<decltype(Di), traits::padding>::padding_type pad2_;
        atomic_id_t Hi; ///< dummy node
        typename opt::details::apply_padding<decltype(Hi), traits::padding>::padding_type pad3_;
        typename maker::atomic_node_ptr Hp;
        typename opt::details::apply_padding<decltype(Hp), traits::padding>::padding_type pad4_;
        std::unique_ptr<handle_type[]> m_handle;
        item_counter m_ItemCounter; ///< Item counter
        stat m_Stat;                ///< Internal statistics
        size_t const nprocs;
        //@endcond

    protected:
        ///@cond
        node_type *alloc_node()
        {
            return typename maker::node_cxx_allocator{}.New();
        }
        static void free_node(node_type *p)
        {
            typename maker::node_deallocator{}(p);
        }

        struct node_disposer
        {
            void operator()(node_type *pNode)
            {
                free_node(pNode);
            }
        };
        typedef std::unique_ptr<node_type, node_disposer> scoped_node_ptr;
        typedef std::unique_ptr<value_type, typename maker::value_deallocator> scoped_value_ptr;
        //@endcond
        
    public:
        /// Initializes empty queue
        SBBlockBasketQueue(size_t nprocs)
          : Ei(1), Di(1), Hi(0), Hp(node_ptr(alloc_node())), 
            m_handle(), nprocs(nprocs)
        {
          m_handle.reset(new handle_type[nprocs]{});
          for(size_t i = 0; i < nprocs; ++i) {
            // This code is originally written to be performed concurrently by
            // each thread.
            auto& handle = m_handle[i];
            handle.Ep = Hp.load(memory_model::memory_order_relaxed);
            handle.enq_node_id = handle.Ep.load(memory_model::memory_order_relaxed)->id;
            handle.Dp = Hp.load(memory_model::memory_order_relaxed);
            handle.deq_node_id = handle.Dp.load(memory_model::memory_order_relaxed)->id;
            handle.spare.reset(alloc_node());
            if (i == 0) {
              handle.next = std::addressof(handle);
            } else {
              handle.next = std::addressof(m_handle[i - 1]);
            }
            handle.Eh = handle.next;
            handle.Dh = handle.next;
            handle.id = i;
          }
        }

        /// Destructor clears the queue
        ~SBBlockBasketQueue()
        {
          clear(0);
          cleanup(m_handle[0]);
          auto pos = Hp.load(memory_model::memory_order_relaxed);
          while(pos != nullptr) {
            auto tmp = pos->next.load(memory_model::memory_order_relaxed);
            typename maker::node_deallocator{}(pos.ptr());
            pos = tmp;
          }
        }

        /// Enqueues \p val value into the queue.
        /**
            The function makes queue node in dynamic memory calling copy constructor for \p val
            and then it calls \p intrusive::BasketQueue::enqueue().
            Returns \p true if success, \p false otherwise.
        */
        template <class Arg>
        bool enqueue(Arg &&val, size_t tid)
        {
            scoped_value_ptr value{typename maker::value_cxx_allocator().MoveNew(std::forward<Arg>(val))};
            auto& handle = m_handle[tid];
            handle.hzd_node_id.store(handle.enq_node_id, memory_model::memory_order_acquire);
            id_t id;
            for(size_t i = 0; i < traits::max_patience; ++i) {
              id = enq_fast(handle, value);
              if (id < 0) {
                break; 
              }
            }
            if(id >= 0) {
              std::terminate();
              enq_slow(handle, value, id);
            }
            handle.enq_node_id = handle.Ep.load(memory_model::memory_order_relaxed)->id;
            handle.hzd_node_id.store(-1, memory_model::memory_order_release);
            ++m_ItemCounter;
            return true;
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
        bool dequeue(value_type &dest, size_t tid, size_t* basket_id = nullptr)
        {
            auto& handle = m_handle[tid];
            handle.hzd_node_id.store(handle.deq_node_id, memory_model::memory_order_acquire);

            auto v = maker::template top<value_ptr>();
            id_t id = 0;
            for(size_t i = 0 ; i < 2 * nprocs; ++i) { // TODO quickfix, to skip empty cells
              v= deq_fast(handle, id, basket_id);
              if (!maker::is_top(v)) {
                break;
              }
            }
            if (maker::is_top(v)) {
              std::terminate();
              v = deq_slow(handle, id);
            } else {
              m_Stat.onFastDequeue();
            }


            if (!maker::is_bot(v)) {
              help_deq(handle, handle.Dh);
              handle.Dh = handle.Dh->next;
            } else {
              m_Stat.onEmpty();
            }

            handle.deq_node_id = handle.Dp.load(memory_model::memory_order_relaxed)->id;
            handle.hzd_node_id.store(-1, memory_model::memory_order_release);
           
            if (!handle.spare) {
              cleanup(handle);
              handle.spare.reset(alloc_node());
            }
            if (maker::is_bot(v)) {
              return false;
            } else {
              std::swap(dest, *v.ptr());
              typename maker::value_deallocator{}(v.ptr());
              --m_ItemCounter;
              return true;
            }
        }

        /// Synonym for \p dequeue() function
        template <class... Args>
        bool pop(Args &&... args)
        {
            return dequeue(std::forward<Args>(args)...);
        }

        void clear(size_t id)
        {
            value_type v;
            while (dequeue(v, id))
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
        const stat &statistics() const
        {
            return m_Stat;
        }

    private:
        id_t enq_fast(handle_type& handle, scoped_value_ptr& val) {
          auto i = allocate_basket(handle);
          auto cell = find_cell(handle.Ep, i, handle);
          auto cv = maker::template bot<value_ptr>();
          if (cell->val.compare_exchange_strong(cv, value_ptr(val.get()))) {
            cell->basket_id = i - handle.id; // TODO this is a race
            val.release();
            m_Stat.onFastEnqueue();
            return -1;
          } else {
            return i;
          }
        }

        typename maker::cell_t * find_cell(node_ptr &ptr, id_t i, handle_type& handle) {
          auto curr = ptr;
          id_t j;
          for(j = curr->id; j < i / traits::node_size; ++j) {
            auto next = curr->next.load(memory_model::memory_order_relaxed);
            if (next == nullptr) {
              auto tmp = handle.spare.get();
              if (!tmp) {
                handle.spare.reset(alloc_node());
                tmp = handle.spare.get();
              }
              tmp->id = j + 1;
              if(curr->next.compare_exchange_strong(next, node_ptr(tmp), memory_model::memory_order_release,
                    memory_model::memory_order_acquire)) {
                next = tmp;
                handle.spare.release();
              }
            }
            curr = next;
          }
          ptr = curr;
          return std::addressof(curr->cells[i % traits::node_size]);
        }

        typename maker::cell_t * find_cell(typename maker::atomic_node_ptr& ptr, id_t i, handle_type& handle) {
          auto tmp = ptr.load(memory_model::memory_order_relaxed);
          auto ret = find_cell(tmp, i, handle);
          ptr.store(tmp, memory_model::memory_order_relaxed);
          return ret;
        }

        void enq_slow(handle_type& handle, scoped_value_ptr& val, id_t id) {
          enq_ptr_t enq{std::addressof(handle.Er)};
          enq->val = value_ptr(val.get());
          enq->id.store(id, memory_model::memory_order_release);
          auto tail = handle.Ep.load(memory_model::memory_order_relaxed);
          id_t i;
          typename maker::cell_t * c = nullptr;
          do {
            i = allocate_basket(handle);
            c = find_cell(tail, i, handle);
            enq_ptr_t ce{nullptr, 0}; // BOT
            if(c->enq.compare_exchange_strong(ce, enq, memory_model::memory_order_seq_cst)
                && !maker::is_top(c->val.load(memory_model::memory_order_relaxed))) {
              if(enq->id.compare_exchange_strong(id, -i, memory_model::memory_order_relaxed)) {
                id = -i;
              }
              break;
            }
          } while (enq->id > 0);

          id = -enq->id;
          c = find_cell(handle.Ep, id, handle);
          swing_ei(handle, id);
          c->val.store(value_ptr(val.release()), memory_model::memory_order_relaxed);
          m_Stat.onSlowEnqueue();
        }

        value_ptr deq_fast(handle_type& handle, id_t& id, size_t * basket_id = nullptr) {
          auto i = Di.fetch_add(1, memory_model::memory_order_seq_cst);
          auto c = find_cell(handle.Dp, i, handle);
          auto v = help_enq(handle, c, i);
          auto cd = maker::template bot<deq_ptr_t>();
          if(maker::is_bot(v)) { return maker::template bot<value_ptr>(); }
          if(!maker::is_top(v) && c->deq.compare_exchange_strong(cd, maker::template top<deq_ptr_t>(), memory_model::memory_order_relaxed)) {
            if (basket_id) {
              *basket_id = c->basket_id;
            }
            return v;
          }
          id = i;
          return maker::template top<value_ptr>();
        }

        value_ptr deq_slow(handle_type& handle, id_t id) {
          auto& deq = handle.Dr;
          deq.id.store(id, memory_model::memory_order_release);
          deq.idx.store(id, memory_model::memory_order_release);
          help_deq(handle, std::addressof(handle)); // TODO ?
          auto i = -deq.idx;
          auto c = find_cell(handle.Dp, i, handle);
          auto val = c->val.load(memory_model::memory_order_relaxed);
          m_Stat.onSlowDequeue();
          return maker::is_top(val) ? maker::template bot<value_ptr>() : val;
        }

        value_ptr help_enq(handle_type& handle, typename maker::cell_t* c, id_t i) {
          auto v = spin(c->val);
          if (v.ptr() || (maker::is_bot(v) && !c->val.compare_exchange_strong(v, maker::template top<value_ptr>(), memory_model::memory_order_seq_cst) && !maker::is_top(v))) {
            return v;
          }

          auto e = c->enq.load(memory_model::memory_order_relaxed);
          if (maker::is_bot(e)) {
            handle_type* ph;
            enq_ptr_t pe;
            id_t id;
            ph = handle.Eh;
            pe = std::addressof(ph->Er);
            id = pe->id;

            if(handle.Ei != 0 && handle.Ei != id) {
              handle.Ei = 0;
              handle.Eh = ph->next;
              ph = handle.Eh;
              pe = enq_ptr_t(std::addressof(ph->Er));
              id = pe->id;
            }
            if(id > 0 && id <= i && !c->enq.compare_exchange_strong(e, pe, memory_model::memory_order_relaxed, memory_model::memory_order_relaxed)) {
              handle.Ei = id;
            } else {
              handle.Eh = ph->next;
            }
            if (maker::is_bot(e) && c->enq.compare_exchange_strong(e, maker::template top<enq_ptr_t>(), memory_model::memory_order_relaxed)) {
              e = maker::template top<enq_ptr_t>();
            }
          }

          if (maker::is_top(e)) {
            return Ei.load(memory_model::memory_order_relaxed) <= i ? maker::template bot<value_ptr>() : maker::template top<value_ptr>();
          }

          auto ei = e->id.load(memory_model::memory_order_acquire);
          auto ev = e->val.load(memory_model::memory_order_acquire);
          if (ei > i) {
            if (maker::is_top(ev) && Ei.load(memory_model::memory_order_relaxed) <= i) {
              return maker::template bot<value_ptr>();
            }
          } else {
            if ((ei > 0 && e->id.compare_exchange_strong(ei, -i, memory_model::memory_order_relaxed))
                || (ei == -i && maker::is_top(c->val.load(memory_model::memory_order_relaxed)))) {
              swing_ei(handle, ei);
              c->val.store(ev, memory_model::memory_order_relaxed);
            }
          }
          return c->val.load(memory_model::memory_order_relaxed);
        }
       

        void help_deq(handle_type& handle, handle_type * ph) {
          auto deq = std::addressof(ph->Dr);
          auto idx = deq->idx.load(memory_model::memory_order_acquire);
          auto id = deq->id.load(memory_model::memory_order_relaxed);
          
          if (idx < id) {
            return;
          }

          auto Dp = ph->Dp.load(memory_model::memory_order_relaxed);
          {
            handle.hzd_node_id.store(ph->hzd_node_id.load(memory_model::memory_order_relaxed), memory_model::memory_order_relaxed);
            atomics::atomic_thread_fence(memory_model::memory_order_seq_cst);
            idx = deq->idx.load(memory_model::memory_order_relaxed);
            auto i = id + 1;
            auto old = id;
            auto new_ = 0;
            while(true) {
              auto h = Dp;
              for(; idx == old && new_ == 0; ++i) {
                auto c = find_cell(h, i, handle);
                auto di = Di.load(memory_model::memory_order_relaxed);
                while(di <= i && !Di.compare_exchange_strong(di, i + 1));
                auto v = help_enq(handle, c, i);
                if(maker::is_bot(v) || (!maker::is_top(v) && maker::is_bot(c->deq.load(memory_model::memory_order_relaxed)))){
                  new_ = i;
                } else {
                  idx = deq->idx.load(memory_model::memory_order_acquire);
                }
              }

              if (new_ != 0) {
                if(deq->idx.compare_exchange_strong(idx, new_, memory_model::memory_order_release,
                      memory_model::memory_order_acquire)) {
                  idx = new_;
                }
                if (idx >= new_) {
                  new_ = 0;
                }
              }

              if (idx < 0 || deq->id.load(memory_model::memory_order_relaxed) != id) {
                break;
              }

              auto c = find_cell(Dp, idx, handle);
              auto cd = maker::template bot<deq_ptr_t>();
              if(maker::is_top(c->val.load(memory_model::memory_order_relaxed)) ||
                  c->deq.compare_exchange_strong(cd, deq_ptr_t(deq), memory_model::memory_order_relaxed) ||
                  cd == deq) {
                deq->idx.compare_exchange_strong(idx, -idx, memory_model::memory_order_relaxed);
                break;
              }

              old = idx;
              if (idx >= i) {
                i = idx + 1;
              }
            }
          }
        }

        void cleanup(handle_type& handle) {
          auto oid = Hi.load(memory_model::memory_order_acquire);
          auto new_ = handle.Dp.load(memory_model::memory_order_relaxed);
          if (oid == -1) return;
          if (new_->id - oid < typename traits::max_grabage{}(nprocs)) return;
          if (!Hi.compare_exchange_strong(oid, -1,
                memory_model::memory_order_acquire,
                memory_model::memory_order_relaxed)) return;

          auto old = Hp.load(memory_model::memory_order_relaxed);
          auto ph = std::addressof(handle);
          std::unique_ptr<handle_type*[]> phs(new handle_type*[nprocs]); 
          int i = 0;

          do {
            new_ = check(ph->hzd_node_id, new_, old);
            new_ = update(ph->Ep, new_, ph->hzd_node_id, old);
            new_ = update(ph->Dp, new_, ph->hzd_node_id, old);

            phs[i++] = ph;
            ph = ph->next;
          } while (new_->id > oid && ph != std::addressof(handle));

          while(new_->id > oid && --i >= 0) {
            new_ = check(phs[i]->hzd_node_id, new_, old);
          }

          auto nid = new_->id;

          if (nid <= oid) {
            Hi.store(oid, memory_model::memory_order_release);
          } else {
            Hp = new_;
            Hi.store(nid, memory_model::memory_order_release);

            while(old != new_) {
              auto tmp = old->next.load(memory_model::memory_order_relaxed);
              typename maker::node_deallocator{}(old.ptr());
              old = tmp;
            }
          }
        }

        node_ptr check(atomic_id_t& p_hzd_node_id, node_ptr cur, node_ptr old) {
          auto hzd_node_id = p_hzd_node_id.load(memory_model::memory_order_acquire);
          if(hzd_node_id < cur->id) {
            auto tmp = old;
            while(tmp->id < hzd_node_id) {
              tmp = tmp->next;
            }
            cur = tmp;
          }
          return cur;
        }

        node_ptr update(typename maker::atomic_node_ptr& pPn, node_ptr cur, atomic_id_t& p_hzd_node_id, node_ptr old) {
          auto ptr = pPn.load(memory_model::memory_order_acquire);
          if(ptr->id < cur->id) {
            if(!pPn.compare_exchange_strong(ptr, cur, memory_model::memory_order_seq_cst)) {
              if(ptr->id < cur->id) {
                cur = ptr;
              }
            }
            cur = check(p_hzd_node_id, cur, old);
          }

          return cur;
        }

        value_ptr spin(typename maker::atomic_value_ptr& p) {
            auto v = p.load(memory_model::memory_order_relaxed);

            for(size_t i = 0; i < traits::max_spin; ++i) {
              if (v.ptr() != nullptr) {
                break;
              }
              v = p.load(memory_model::memory_order_relaxed);
              // PAUSE
            }

            return v;
        }

        void swing_ei(const handle_type& handle, id_t id) {
          // id += (nprocs - handle.id); // Round to bucket
          id += 1;
          auto ei = Ei.load(memory_model::memory_order_relaxed);
          while(ei < id && insert_policy::template _<memory_model>(Ei, ei, id));
        }

        id_t allocate_basket(const handle_type& handle) {
          auto ei = Ei.load(memory_model::memory_order_acquire);
          if(insert_policy::template _<memory_model>(Ei, ei, id_t(ei + nprocs))) {
            return ei + handle.id;
          } else {
            return ei - nprocs + handle.id;
          }
        }
    };

    

}} // namespace cds::container

#endif // #ifndef CDSLIB_CONTAINER_SB_BLOCK_BASKET_QUEUE_H
