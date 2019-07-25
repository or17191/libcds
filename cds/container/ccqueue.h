
// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_CC_QUEUE_H
#define CDSLIB_CONTAINER_CC_QUEUE_H

#include <memory>

#include <cds/container/details/base.h>
#include <cds/container/wf_primitives.h>
#include <cds/gc/nogc.h>
#include <memkind.h>

#define EMPTY ((void*)-1)
#define CCSYNCH_WAIT  0x0
#define CCSYNCH_READY 0x1
#define CCSYNCH_DONE  0x3

namespace cds { namespace container {

    namespace ccqueue {
      struct empty_stat {};
      struct traits {
        using stat_type = empty_stat;
        using item_counter = atomicity::empty_item_counter;
        using numa_balance = std::false_type;
      };
      
      template <typename... Options>
      struct make_traits {
          typedef typename cds::opt::make_options<
              typename cds::opt::find_type_traits< traits, Options... >::type
              , Options...
          >::type type;
      };


      template <class T, class Traits = traits>
      class CCQueue {
        public:
          using stat_type = typename Traits::stat_type;
        private:
          typedef struct _ccsynch_node_t {
            struct _ccsynch_node_t * volatile next CACHE_ALIGNED;
            void * volatile data;
            int volatile status CACHE_ALIGNED;
          } ccsynch_node_t;

          typedef struct _ccsynch_handle_t {
            struct _ccsynch_node_t * next;
          } ccsynch_handle_t;

          typedef struct _ccsynch_t {
            struct _ccsynch_node_t * volatile tail DOUBLE_CACHE_ALIGNED;
          } ccsynch_t;


          static inline
          void ccsynch_apply(ccsynch_t * synch, ccsynch_handle_t * handle,
              void (*apply)(void *, void *), void * state, void * data)
          {
            ccsynch_node_t * next = handle->next;
            next->next = NULL;
            next->status = CCSYNCH_WAIT;

            ccsynch_node_t * curr = SWAPra(&synch->tail, next);
            handle->next = curr;

            int status = ACQUIRE(&curr->status);

            if (status == CCSYNCH_WAIT) {
              curr->data = data;
              RELEASE(&curr->next, next);

              do {
                PAUSE();
                status = ACQUIRE(&curr->status);
              } while (status == CCSYNCH_WAIT);
            }

            if (status != CCSYNCH_DONE) {
              apply(state, data);

              curr = next;
              next = ACQUIRE(&curr->next);

              int count = 0;
              const int CCSYNCH_HELP_BOUND = 256;

              while (next && count++ < CCSYNCH_HELP_BOUND) {
                apply(state, curr->data);
                RELEASE(&curr->status, CCSYNCH_DONE);

                curr = next;
                next = ACQUIRE(&curr->next);
              }

              RELEASE(&curr->status, CCSYNCH_READY);
            }
          }

          static ccsynch_node_t* allocate_ccsynch_node() {
            void * ptr;
            int ret = memkind_posix_memalign(MEMKIND_HUGETLB, &ptr, cds::c_nCacheLineSize, sizeof(ccsynch_node_t));
            if (ret != 0) {
              fprintf(stderr, strerror(ret));
              abort();
            }

            return (ccsynch_node_t*)ptr;
          }

          static inline void ccsynch_init(ccsynch_t * synch)
          {
            ccsynch_node_t * node = allocate_ccsynch_node();
            node->next = NULL;
            node->status = CCSYNCH_READY;

            synch->tail = node;
          }

          static inline void ccsynch_handle_init(ccsynch_handle_t * handle)
          {
            handle->next = allocate_ccsynch_node();
          }

          typedef struct _node_t {
            struct _node_t * next CACHE_ALIGNED;
            void * volatile data;
          } node_t;

          typedef struct _queue_t {
            ccsynch_t enq DOUBLE_CACHE_ALIGNED;
            ccsynch_t deq DOUBLE_CACHE_ALIGNED;
            node_t * head DOUBLE_CACHE_ALIGNED;
            node_t * tail DOUBLE_CACHE_ALIGNED;
          } queue_t DOUBLE_CACHE_ALIGNED;

          typedef struct _handle_t {
            ccsynch_handle_t enq;
            ccsynch_handle_t deq;
            node_t * next;
          } handle_t DOUBLE_CACHE_ALIGNED;

          static inline
          void serialEnqueue(void * state, void * data)
          {
            node_t * volatile * tail = (node_t **) state;
            node_t * node = (node_t *) data;

            (*tail)->next = node;
            *tail = node;
          }

          static inline
          void serialDequeue(void * state, void * data)
          {
            node_t * volatile * head = (node_t **) state;
            node_t ** ptr = (node_t **) data;

            node_t * node = *head;
            node_t * next = node->next;

            if (next) {
              node->data = next->data;
              *head = next;
            } else {
              node = (node_t *) -1;
            }

            *ptr = node;
          }

          static inline node_t * allocate_node() {
            void * ptr;
            int ret = memkind_posix_memalign(MEMKIND_HUGETLB, &ptr, cds::c_nCacheLineSize, sizeof(node_t));
            if (ret != 0) {
              fprintf(stderr, strerror(ret));
              abort();
            }

            return (node_t*)ptr;
          }

          void queue_init(queue_t * queue, int nprocs)
          {
            ccsynch_init(&queue->enq);
            ccsynch_init(&queue->deq);

            node_t * dummy = allocate_node();
            dummy->data = 0;
            dummy->next = NULL;

            queue->head = dummy;
            queue->tail = dummy;
          }

          void queue_register(queue_t * queue, handle_t * handle, int id)
          {
            ccsynch_handle_init(&handle->enq);
            ccsynch_handle_init(&handle->deq);

            handle->next = allocate_node();
          }

          void enqueue(queue_t * queue, handle_t * handle, void * data)
          {
            node_t * node = handle->next;

            if (node) handle->next = NULL;
            else node = allocate_node();

            node->data = data;
            node->next = NULL;

            ccsynch_apply(&queue->enq, &handle->enq, &serialEnqueue, &queue->tail, node);
          }

          void * dequeue(queue_t * queue, handle_t * handle)
          {
            node_t * node;
            ccsynch_apply(&queue->deq, &handle->deq, &serialDequeue, &queue->head, &node);

            void * data;

            if (node == (void *) -1) {
              data = (void *) -1;
            } else {
              data = node->data;
              if (handle->next) memkind_free(MEMKIND_HUGETLB, node);
              else handle->next = node;
            }

            return data;
          }

          void queue_free(int id, int nprocs) {}

          queue_t m_internal_queue DOUBLE_CACHE_ALIGNED;
          std::atomic<ssize_t> m_active_socket DOUBLE_CACHE_ALIGNED {-1};
          size_t m_size DOUBLE_CACHE_ALIGNED;
          size_t m_socket_boundary DOUBLE_CACHE_ALIGNED;
          std::vector<handle_t> m_handlers DOUBLE_CACHE_ALIGNED;
          empty_stat m_stats;
        public:
          using value_type = T;
          using item_counter = typename Traits::item_counter;
          using gc = cds::gc::nogc;
          CCQueue(size_t ids, ssize_t socket_boundary = -1)
              : m_size(ids), m_socket_boundary(socket_boundary < 0 ? ids : socket_boundary), m_handlers(ids)
          {
            queue_init(&m_internal_queue, m_size);
            for(size_t i = 0; i < m_size; ++i) {
              queue_register(&m_internal_queue, &m_handlers[i], i);
            }
          }

          /// Returns reference to internal statistics
          const stat_type &statistics() 
          {
              return m_stats;
          }

          static inline uint64_t read_tsc() {
              unsigned upper, lower;
              __asm__ __volatile__ ("rdtsc" : "=a"(lower), "=d"(upper));
              return ((uint64_t)lower)|(((uint64_t)upper)<<32 );
          }

          size_t get_socket(size_t id) const {
            if (id < m_socket_boundary) {
              return 0;
            } else { 
              return 1;
            }
          }

          void wait_for_socket(size_t id, std::true_type) {
            ssize_t socket = get_socket(id);
            ssize_t current = m_active_socket.load(std::memory_order_acquire);

            if(cds_likely(current == socket)) {
              return;
            }

            using namespace std::chrono;
            constexpr microseconds USEC_TIMOUT(100);
            constexpr uint64_t WAIT_TIMEOUT = (duration_cast<nanoseconds>(USEC_TIMOUT).count() * 22) / 10;
            uint64_t w = read_tsc() + WAIT_TIMEOUT;

            while (current != -1 && current != socket && read_tsc() < w) {
              current = m_active_socket.load(std::memory_order_acquire);
            }

            if(current != socket) {
              m_active_socket.compare_exchange_strong(current, socket, std::memory_order_acq_rel, std::memory_order_relaxed);
            }
          }

          void wait_for_socket(size_t id, std::false_type) {}

          bool enqueue(T* val, size_t id)
          {
              wait_for_socket(id, typename Traits::numa_balance{});
              enqueue(&m_internal_queue, &m_handlers[id], val);
              return true;
          }

          bool dequeue(T* &dest, size_t tid)
          {
              wait_for_socket(tid, typename Traits::numa_balance{});
              dest = reinterpret_cast<T*>(dequeue(&m_internal_queue, &m_handlers[tid]));
              return static_cast<void*>(dest) != EMPTY;
          }

          ~CCQueue() {
            clear(0);
          }

          void clear(size_t id) {
            T* dest;
            while(dequeue(dest, id)) {}
          }

          template <class... Args>
          bool pop(Args &&... args)
          {
              return dequeue(std::forward<Args>(args)...);
          }

          template <class... Args>
          bool push(Args &&... args)
          {
              return enqueue(std::forward<Args>(args)...);
          }

          size_t size() const {
            return 0;
          }

      };
    }

    using ccqueue::CCQueue;

}} // namespace cds::container

#undef EMPTY
#undef CCSYNCH_READY
#undef CCSYNCH_WAIT
#undef CCSYNCH_DONE

#endif // #ifndef CDSLIB_CONTAINER_CC_QUEUE_H
