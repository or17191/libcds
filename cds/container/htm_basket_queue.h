// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_HTM_BASKET_QUEUE_H
#define CDSLIB_CONTAINER_HTM_BASKET_QUEUE_H

#include <cds/intrusive/htm_basket_queue.h>
#include <cds/container/basket_queue.h>
#include <cds/container/details/base.h>
#include <memory>

namespace cds { namespace container {

    /// HTMBasketQueue related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace htm_basket_queue {

        /// Internal statistics
        template <typename Counter = cds::intrusive::basket_queue::stat<>::counter_type >
        using stat = cds::intrusive::basket_queue::stat< Counter >;

        /// Dummy internal statistics
        typedef cds::intrusive::basket_queue::empty_stat empty_stat;

        /// HTMBasketQueue default type traits
        struct traits
        {
            /// Node allocator
            typedef CDS_DEFAULT_ALLOCATOR       allocator;

            /// Back-off strategy
            typedef cds::backoff::empty         back_off;

            /// Item counting feature; by default, disabled. Use \p cds::atomicity::item_counter to enable item counting
            typedef atomicity::empty_item_counter   item_counter;

            /// Internal statistics (by default, disabled)
            /**
                Possible option value are: \p basket_queue::stat, \p basket_queue::empty_stat (the default),
                user-provided class that supports \p %basket_queue::stat interface.
            */
            typedef htm_basket_queue::empty_stat         stat;

            /// C++ memory ordering model
            /**
                Can be \p opt::v::relaxed_ordering (relaxed memory model, the default)
                or \p opt::v::sequential_consistent (sequentially consisnent memory model).
            */
            typedef opt::v::relaxed_ordering    memory_model;
            typedef cds::intrusive::htm_basket_queue::htm_insert insert_policy;

            /// Padding for internal critical atomic data. Default is \p opt::cache_line_padding
            enum { padding = opt::cache_line_padding };
        };

        /// Metafunction converting option list to \p basket_queue::traits
        /**
            Supported \p Options are:
            - \p opt::allocator - allocator (like \p std::allocator) used for allocating queue nodes. Default is \ref CDS_DEFAULT_ALLOCATOR
            - \p opt::back_off - back-off strategy used, default is \p cds::backoff::empty.
            - \p opt::item_counter - the type of item counting feature. Default is \p cds::atomicity::empty_item_counter (item counting disabled)
                To enable item counting use \p cds::atomicity::item_counter
            - \ opt::stat - the type to gather internal statistics.
                Possible statistics types are: \p basket_queue::stat, \p basket_queue::empty_stat, user-provided class that supports \p %basket_queue::stat interface.
                Default is \p %basket_queue::empty_stat.
            - \p opt::padding - padding for internal critical atomic data. Default is \p opt::cache_line_padding
            - \p opt::memory_model - C++ memory ordering model. Can be \p opt::v::relaxed_ordering (relaxed memory model, the default)
                or \p opt::v::sequential_consistent (sequentially consisnent memory model).

            Example: declare \p %HTMBasketQueue with item counting and internal statistics
            \code
            typedef cds::container::HTMBasketQueue< cds::gc::HP, Foo,
                typename cds::container::basket_queue::make_traits<
                    cds::opt::item_counte< cds::atomicity::item_counter >,
                    cds::opt::stat< cds::intrusive::basket_queue::stat<> >
                >::type
            > myQueue;
            \endcode
        */
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
    } // namespace htm_basket_queue

    //@cond
    namespace details {
        template <typename GC, typename T, typename Traits>
        struct make_htm_basket_queue
        {
            typedef GC gc;
            typedef T value_type;
            typedef Traits traits;

            struct node_type: public intrusive::basket_queue::node< gc >
            {
                value_type  m_value;

                node_type( const value_type& val )
                    : m_value( val )
                {}
                template <typename... Args>
                node_type( Args&&... args )
                    : m_value( std::forward<Args>(args)...)
                {}
            };

            typedef typename std::allocator_traits< typename traits::allocator >::template rebind_alloc< node_type > allocator_type;
            //typedef typename traits::allocator::template rebind<node_type>::other allocator_type;
            typedef cds::details::Allocator< node_type, allocator_type >           cxx_allocator;

            struct node_deallocator
            {
                void operator ()( node_type * pNode )
                {
                    cxx_allocator().Delete( pNode );
                }
            };

            struct intrusive_traits : public traits
            {
                typedef cds::intrusive::basket_queue::base_hook< opt::gc<gc> > hook;
                typedef node_deallocator disposer;
                static constexpr const cds::intrusive::opt::link_check_type link_checker = cds::intrusive::basket_queue::traits::link_checker;
            };

            typedef cds::intrusive::HTMBasketQueue< gc, node_type, intrusive_traits > type;
        };
    }

    template <typename GC, typename T, typename Traits = htm_basket_queue::traits >
    class HTMBasketQueue: public cds::container::BasketQueue<GC, T, Traits> {
    private:
      typedef BasketQueue<GC, T, Traits> base_type;
    public:
      using base_type::base_type;
    };

}}  // namespace cds::container

#endif  // #ifndef CDSLIB_CONTAINER_HTM_BASKET_QUEUE_H
