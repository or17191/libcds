// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
#define CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H

#include <type_traits>
#include <cds/intrusive/details/single_link_struct.h>
#include <cds/intrusive/basket_queue.h>
#include <cds/details/marked_ptr.h>
#include <cds/sync/htm.h>

namespace cds { namespace intrusive {

    template <typename GC, typename T, typename Traits = basket_queue::traits >
    class HTMBasketQueue : public BasketQueue<GC, T, Traits>
    {
    private:
        typedef BasketQueue<GC, T, Traits> base_type;
    public:
        typedef typename base_type::gc gc;          ///< Garbage collector
        typedef typename base_type::value_type value_type;  ///< type of value stored in the queue
        typedef typename base_type::traits traits;  ///< Queue traits
        typedef typename base_type::hook hook;       ///< hook type
        typedef typename base_type::node_type node_type;  ///< node type
        typedef typename base_type::disposer disposer;   ///< disposer used
        typedef typename base_type::node_traits node_traits;   ///< node traits
        typedef typename base_type::link_checker link_checker;   ///< link checker

        typedef typename base_type::back_off back_off;     ///< back-off strategy
        typedef typename base_type::item_counter item_counter; ///< Item counting policy used
        typedef typename base_type::stat stat;         ///< Internal statistics policy used
        typedef typename base_type::memory_model memory_model; ///< Memory ordering. See cds::opt::memory_model option

        /// Rebind template arguments
        template <typename GC2, typename T2, typename Traits2>
        struct rebind {
            typedef HTMBasketQueue< GC2, T2, Traits2> other   ;   ///< Rebinding result
        };

        static constexpr const size_t c_nHazardPtrCount = 6 ; ///< Count of hazard pointer required for the algorithm

    protected:
        //@cond
        typedef typename node_type::marked_ptr   marked_ptr;
        typedef typename node_type::atomic_marked_ptr atomic_marked_ptr;

        // GC and node_type::gc must be the same
        static_assert( std::is_same<gc, typename node_type::gc>::value, "GC and node_type::gc must be the same");
        //@endcond

        //@cond
        using base_type::m_pHead;           ///< Queue's head pointer (aligned)
        using base_type::m_pTail;           ///< Queue's tail pointer (aligned)
        using base_type::m_Dummy;           ///< dummy node
        using base_type::m_ItemCounter;   ///< Item counter
        using base_type::m_Stat;           ///< Internal statistics
        using base_type::m_nMaxHops;
        //@endcond

    public:
        /// Initializes empty queue
        using base_type::base_type;

        /// Enqueues \p val value into the queue.
        /** @anchor cds_intrusive_BasketQueue_enqueue
            The function always returns \p true.
        */
        bool enqueue( value_type& val )
        {
            node_type * pNew = node_traits::to_node_ptr( val );
            link_checker::is_empty( pNew );

            typename gc::Guard guard;
            typename gc::Guard gNext;
            back_off bkoff;

            marked_ptr t;
            while ( true ) {
                t = guard.protect( m_pTail, []( marked_ptr p ) -> value_type * { return node_traits::to_value_ptr( p.ptr());});

                marked_ptr pNext = t->m_pNext.load(memory_model::memory_order_relaxed );

                if ( pNext.ptr() == nullptr ) {
                    pNew->m_pNext.store( marked_ptr(), memory_model::memory_order_relaxed );
                    auto marked_new = marked_ptr(pNew);
                    auto transaction = [&] {
                      if (t->m_pNext.load(memory_model::memory_order_relaxed) != pNext) { 
                        _xabort(0xff); 
                        } 
                        t->m_pNext.store(marked_new, memory_model::memory_order_relaxed); 
                    };
                    if(sync::htm(transaction)) {
                        if ( !m_pTail.compare_exchange_strong( t, marked_ptr(pNew), memory_model::memory_order_release, atomics::memory_order_relaxed ))
                            m_Stat.onAdvanceTailFailed();
                        break;
                    }

                    // Try adding to basket
                    m_Stat.onTryAddBasket();

                    // Reread tail next
                try_again:
                    pNext = gNext.protect( t->m_pNext, []( marked_ptr p ) -> value_type * { return node_traits::to_value_ptr( p.ptr());});

                    // add to the basket
                    if ( m_pTail.load( memory_model::memory_order_relaxed ) == t
                         && t->m_pNext.load( memory_model::memory_order_relaxed) == pNext
                         && !pNext.bits())
                    {
                        bkoff();
                        pNew->m_pNext.store( pNext, memory_model::memory_order_relaxed );
                        if ( t->m_pNext.compare_exchange_weak( pNext, marked_ptr( pNew ), memory_model::memory_order_release, atomics::memory_order_relaxed )) {
                            m_Stat.onAddBasket();
                            break;
                        }
                        goto try_again;
                    }
                }
                else {
                    // Tail is misplaced, advance it

                    typename gc::template GuardArray<2> g;
                    g.assign( 0, node_traits::to_value_ptr( pNext.ptr()));
                    if ( m_pTail.load( memory_model::memory_order_acquire ) != t
                      || t->m_pNext.load( memory_model::memory_order_relaxed ) != pNext )
                    {
                        m_Stat.onEnqueueRace();
                        bkoff();
                        continue;
                    }

                    marked_ptr p;
                    bool bTailOk = true;
                    while ( (p = pNext->m_pNext.load( memory_model::memory_order_acquire )).ptr() != nullptr )
                    {
                        bTailOk = m_pTail.load( memory_model::memory_order_relaxed ) == t;
                        if ( !bTailOk )
                            break;

                        g.assign( 1, node_traits::to_value_ptr( p.ptr()));
                        if ( pNext->m_pNext.load( memory_model::memory_order_relaxed ) != p )
                            continue;
                        pNext = p;
                        g.assign( 0, g.template get<value_type>( 1 ));
                    }
                    if ( !bTailOk || !m_pTail.compare_exchange_weak( t, marked_ptr( pNext.ptr()), memory_model::memory_order_release, atomics::memory_order_relaxed ))
                        m_Stat.onAdvanceTailFailed();

                    m_Stat.onBadTail();
                }

                m_Stat.onEnqueueRace();
            }

            ++m_ItemCounter;
            m_Stat.onEnqueue();

            return true;
        }

        /// Synonym for \p enqueue() function
        bool push( value_type& val )
        {
            return enqueue( val );
        }

    };

}} // namespace cds::intrusive

#endif // #ifndef CDSLIB_INTRUSIVE_HTM_BASKET_QUEUE_H
