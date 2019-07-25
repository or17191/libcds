
// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSLIB_CONTAINER_WF_QUEUE_H
#define CDSLIB_CONTAINER_WF_QUEUE_H

#include <memory>

#include <cds/container/details/base.h>
#include <cds/gc/nogc.h>
#include <memkind.h>

#if defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ > 7
/**
 * An atomic fetch-and-add.
 */
#define FAA(ptr, val) __atomic_fetch_add(ptr, val, __ATOMIC_RELAXED)
/**
 * An atomic fetch-and-add that also ensures sequential consistency.
 */
#define FAAcs(ptr, val) __atomic_fetch_add(ptr, val, __ATOMIC_SEQ_CST)

/**
 * An atomic compare-and-swap.
 */
#define CAS(ptr, cmp, val) __atomic_compare_exchange_n(ptr, cmp, val, 0, \
    __ATOMIC_RELAXED, __ATOMIC_RELAXED)
/**
 * An atomic compare-and-swap that also ensures sequential consistency.
 */
#define CAScs(ptr, cmp, val) __atomic_compare_exchange_n(ptr, cmp, val, 0, \
    __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)
/**
 * An atomic compare-and-swap that ensures release semantic when succeed
 * or acquire semantic when failed.
 */
#define CASra(ptr, cmp, val) __atomic_compare_exchange_n(ptr, cmp, val, 0, \
    __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)
/**
 * An atomic compare-and-swap that ensures acquire semantic when succeed
 * or relaxed semantic when failed.
 */
#define CASa(ptr, cmp, val) __atomic_compare_exchange_n(ptr, cmp, val, 0, \
    __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)

/**
 * An atomic swap.
 */
#define SWAP(ptr, val) __atomic_exchange_n(ptr, val, __ATOMIC_RELAXED)

/**
 * An atomic swap that ensures acquire release semantics.
 */
#define SWAPra(ptr, val) __atomic_exchange_n(ptr, val, __ATOMIC_ACQ_REL)

/**
 * A memory fence to ensure sequential consistency.
 */
#define FENCE() __atomic_thread_fence(__ATOMIC_SEQ_CST)

/**
 * An atomic store.
 */
#define STORE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELAXED)

/**
 * A store with a preceding release fence to ensure all previous load
 * and stores completes before the current store is visiable.
 */
#define RELEASE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELEASE)

/**
 * A load with a following acquire fence to ensure no following load and
 * stores can start before the current load completes.
 */
#define ACQUIRE(ptr) __atomic_load_n(ptr, __ATOMIC_ACQUIRE)

#else /** Non-GCC or old GCC. */
#if defined(__x86_64__) || defined(_M_X64_)

#define FAA __sync_fetch_and_add
#define FAAcs __sync_fetch_and_add

static inline int
_compare_and_swap(void ** ptr, void ** expected, void * desired) {
  void * oldval = *expected;
  void * newval = __sync_val_compare_and_swap(ptr, oldval, desired);

  if (newval == oldval) {
    return 1;
  } else {
    *expected = newval;
    return 0;
  }
}
#define CAS(ptr, expected, desired) \
  _compare_and_swap((void **) (ptr), (void **) (expected), (void *) (desired))
#define CAScs CAS
#define CASra CAS
#define CASa  CAS

#define SWAP __sync_lock_test_and_set
#define SWAPra SWAP

#define ACQUIRE(p) ({ \
  __typeof__(*(p)) __ret = *p; \
  __asm__("":::"memory"); \
  __ret; \
})

#define RELEASE(p, v) do {\
  __asm__("":::"memory"); \
  *p = v; \
} while (0)
#define FENCE() __sync_synchronize()

#endif
#endif

#if defined(__x86_64__) || defined(_M_X64_)
#define PAUSE() __asm__ ("pause")

#else
#define PAUSE()
#endif

#define CACHE_ALIGNED __attribute__((aligned(::cds::c_nCacheLineSize)))
#define DOUBLE_CACHE_ALIGNED __attribute__((aligned(2 * ::cds::c_nCacheLineSize)))
#define BOT ((void*)0)
#define TOP ((void*)-1)

namespace cds { namespace container {

    /// BasketQueue related definitions
    /** @ingroup cds_nonintrusive_helper
    */
    namespace wf_queue {
      template <typename Counter = cds::atomicity::event_counter >
      struct full_stat
      {
          typedef Counter counter_type;   

          counter_type m_SlowEnqueue;    
          counter_type m_SlowDequeue;    
          counter_type m_FastEnqueue;     
          counter_type m_FastDequeue;     
          counter_type m_Empty;

          void onSlowEnqueue() { ++m_SlowEnqueue; }    
          void onSlowDequeue() { ++m_SlowDequeue; }    
          void onFastEnqueue() { ++m_FastEnqueue; }     
          void onFastDequeue() { ++m_FastDequeue; }     
          void onEmpty() { ++m_Empty; }


          //@cond
          void reset()
          {
              m_SlowEnqueue.reset();    
              m_SlowDequeue.reset();    
              m_FastEnqueue.reset();     
              m_FastDequeue.reset();     
              m_Empty.reset();
          }

          full_stat& operator +=( full_stat const& s )
          {
              m_SlowEnqueue += s.m_SlowEnqueue.get();
              m_SlowDequeue += s.m_SlowDequeue.get();
              m_FastEnqueue += s.m_FastEnqueue.get();
              m_FastDequeue += s.m_FastDequeue.get();
              m_Empty += s.m_Empty.get();
              return *this;
          }
          //@endcond
      };

      struct empty_stat
      {

          void onSlowEnqueue() { }    
          void onSlowDequeue() { }    
          void onFastEnqueue() { }     
          void onFastDequeue() { }     
          void onEmpty() { }


          //@cond
          void reset()
          {
          }

          empty_stat& operator +=( empty_stat const& s )
          {
              return *this;
          }
          //@endcond
      };

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

      static constexpr void* EMPTY = ((void*) 0);
      static constexpr size_t WFQUEUE_NODE_SIZE = (1 << 10) - 2;
      static constexpr size_t PAGE_SIZE = 4096;

      template <class T, class Traits = traits>
      class WFQueue {
        public:
          using stat_type = typename Traits::stat_type;
        private:
          struct enq_t {
            long volatile id;
            void * volatile val;
          } CACHE_ALIGNED;

          struct deq_t {
            long volatile id;
            long volatile idx;
          } CACHE_ALIGNED;

          struct cell_t {
            void * volatile val;
            enq_t * volatile enq;
            deq_t * volatile deq;
            void * pad[5];
          };

          struct node_t {
            node_t * volatile next CACHE_ALIGNED;
            long id CACHE_ALIGNED;
            struct cell_t cells[WFQUEUE_NODE_SIZE] CACHE_ALIGNED;
          };

          struct queue_t {
            /**
             * Index of the next position for enqueue.
             */
            volatile long Ei DOUBLE_CACHE_ALIGNED;

            /**
             * Index of the next position for dequeue.
             */
            volatile long Di DOUBLE_CACHE_ALIGNED;

            /**
             * Index of the head of the queue.
             */
            volatile long Hi DOUBLE_CACHE_ALIGNED;

            /**
             * Pointer to the head node of the queue.
             */
            struct node_t * volatile Hp;

            /**
             * Number of processors.
             */
            long nprocs;
            stat_type stat;
          } DOUBLE_CACHE_ALIGNED ;

          struct handle_t {
            /**
             * Pointer to the next handle.
             */
            handle_t * next;

            /**
             * Hazard pointer.
             */
            //struct _node_t * volatile Hp;
            unsigned long volatile hzd_node_id;

            /**
             * Pointer to the node for enqueue.
             */
            node_t * volatile Ep;
            unsigned long enq_node_id;

            /**
             * Pointer to the node for dequeue.
             */
            node_t * volatile Dp;
            unsigned long deq_node_id;

            /**
             * Enqueue request.
             */
            enq_t Er CACHE_ALIGNED;

            /**
             * Dequeue request.
             */
            deq_t Dr CACHE_ALIGNED;

            /**
             * Handle of the next enqueuer to help.
             */
            handle_t * Eh CACHE_ALIGNED;

            long Ei;

            /**
             * Handle of the next dequeuer to help.
             */
            handle_t * Dh;

            /**
             * Pointer to a spare node to use, to speedup adding a new node.
             */
            node_t * spare CACHE_ALIGNED;

            /**
             * Count the delay rounds of helping another dequeuer.
             */
            int delay;

            stat_type stat;
          };
          
          static constexpr inline size_t MAX_GARBAGE(size_t n) { return 2 * n; }

          static constexpr size_t MAX_SPIN = 100;
          static constexpr size_t MAX_PATIENCE = 10;

          static inline void *spin(void *volatile *p) {
              int patience = MAX_SPIN;
              void *v = *p;

              while (!v && patience-- > 0) {
                  v = *p;
                  PAUSE();
              }

              return v;
          }

          static inline void * align_malloc(size_t align, size_t size)
          {
            void * ptr;

            int ret = memkind_posix_memalign(MEMKIND_HUGETLB, &ptr, align, size);
            if (ret != 0) {
              fprintf(stderr, strerror(ret));
              abort();
            }

            return ptr;
          }

          static inline node_t *new_node() {
              node_t *n = reinterpret_cast<node_t*>(align_malloc(PAGE_SIZE, sizeof(node_t)));
              memset(n, 0, sizeof(node_t));
              return n;
          }

          static node_t *check(unsigned long volatile *p_hzd_node_id, node_t *cur,
                               node_t *old) {
              unsigned long hzd_node_id = ACQUIRE(p_hzd_node_id);

              if (hzd_node_id < cur->id) {
                  node_t *tmp = old;
                  while (tmp->id < hzd_node_id) {
                      tmp = tmp->next;
                  }
                  cur = tmp;
              }

              return cur;
          }

          static node_t *update(node_t *volatile *pPn, node_t *cur,
                                unsigned long volatile *p_hzd_node_id, node_t *old) {
              node_t *ptr = ACQUIRE(pPn);

              if (ptr->id < cur->id) {
                  if (!CAScs(pPn, &ptr, cur)) {
                      if (ptr->id < cur->id) cur = ptr;
                  }

                  cur = check(p_hzd_node_id, cur, old);
              }

              return cur;
          }

          static void cleanup(queue_t *q, handle_t *th) {
              long oid = ACQUIRE(&q->Hi);
              node_t *new_node = th->Dp;

              if (oid == -1) return;
              if (new_node->id - oid < MAX_GARBAGE(q->nprocs)) return;
              if (!CASa(&q->Hi, &oid, -1)) return;

              node_t *old = q->Hp;
              handle_t *ph = th;
              handle_t *phs[q->nprocs];
              int i = 0;

              do {
                  new_node = check(&ph->hzd_node_id, new_node, old);
                  new_node = update(&ph->Ep, new_node, &ph->hzd_node_id, old);
                  new_node = update(&ph->Dp, new_node, &ph->hzd_node_id, old);

                  phs[i++] = ph;
                  ph = ph->next;
              } while (new_node->id > oid && ph != th);

              while (new_node->id > oid && --i >= 0) {
                  new_node = check(&phs[i]->hzd_node_id, new_node, old);
              }

              long nid = new_node->id;

              if (nid <= oid) {
                  RELEASE(&q->Hi, oid);
              } else {
                  q->Hp = new_node;
                  RELEASE(&q->Hi, nid);

                  while (old != new_node) {
                      node_t *tmp = old->next;
                      memkind_free(MEMKIND_HUGETLB, old);
                      old = tmp;
                  }
              }
          }

          static cell_t *find_cell(node_t *volatile *ptr, long i, handle_t *th) {
              node_t *curr = *ptr;

              long j;
              for (j = curr->id; j < i / WFQUEUE_NODE_SIZE; ++j) {
                  node_t *next = curr->next;

                  if (next == NULL) {
                      node_t *temp = th->spare;

                      if (!temp) {
                          temp = new_node();
                          th->spare = temp;
                      }

                      temp->id = j + 1;

                      if (CASra(&curr->next, &next, temp)) {
                          next = temp;
                          th->spare = NULL;
                      }
                  }

                  curr = next;
              }

              *ptr = curr;
              return &curr->cells[i % WFQUEUE_NODE_SIZE];
          }

          static int enq_fast(queue_t *q, handle_t *th, void *v, long *id) {
              long i = FAAcs(&q->Ei, 1);
              cell_t *c = find_cell(&th->Ep, i, th);
              void *cv = BOT;

              if (CAS(&c->val, &cv, v)) {
                  th->stat.onFastEnqueue();
                  return 1;
              } else {
                  *id = i;
                  return 0;
              }
          }

          static void enq_slow(queue_t *q, handle_t *th, void *v, long id) {
              enq_t *enq = &th->Er;
              enq->val = v;
              RELEASE(&enq->id, id);

              node_t *tail = th->Ep;
              long i;
              cell_t *c;

              do {
                  i = FAA(&q->Ei, 1);
                  c = find_cell(&tail, i, th);
                  enq_t *ce = reinterpret_cast<enq_t*>(BOT);

                  if (CAScs(&c->enq, &ce, enq) && c->val != TOP) {
                      if (CAS(&enq->id, &id, -i)) id = -i;
                      break;
                  }
              } while (enq->id > 0);

              id = -enq->id;
              c = find_cell(&th->Ep, id, th);
              if (id > i) {
                  long Ei = q->Ei;
                  while (Ei <= id && !CAS(&q->Ei, &Ei, id + 1))
                      ;
              }
              c->val = v;

              th->stat.onSlowEnqueue();
          }

          static void enqueue(queue_t *q, handle_t *th, void *v) {
              th->hzd_node_id = th->enq_node_id;

              long id;
              int p = MAX_PATIENCE;
              while (!enq_fast(q, th, v, &id) && p-- > 0)
                  ;
              if (p < 0) enq_slow(q, th, v, id);

              th->enq_node_id = th->Ep->id;
              RELEASE(&th->hzd_node_id, -1);
          }

          static void *help_enq(queue_t *q, handle_t *th, cell_t *c, long i) {
              void *v = spin(&c->val);

              if ((v != TOP && v != BOT) ||
                  (v == BOT && !CAScs(&c->val, &v, TOP) && v != TOP)) {
                  return v;
              }

              enq_t *e = c->enq;

              if (e == BOT) {
                  handle_t *ph;
                  enq_t *pe;
                  long id;
                  ph = th->Eh, pe = &ph->Er, id = pe->id;

                  if (th->Ei != 0 && th->Ei != id) {
                      th->Ei = 0;
                      th->Eh = ph->next;
                      ph = th->Eh, pe = &ph->Er, id = pe->id;
                  }

                  if (id > 0 && id <= i && !CAS(&c->enq, &e, pe))
                      th->Ei = id;
                  else
                      th->Eh = ph->next;

                  if (e == BOT && CAS(&c->enq, &e, TOP)){
                    e = reinterpret_cast<enq_t*>(TOP);
                  }
              }

              if (e == TOP) return (q->Ei <= i ? BOT : TOP);

              long ei = ACQUIRE(&e->id);
              void *ev = ACQUIRE(&e->val);

              if (ei > i) {
                  if (c->val == TOP && q->Ei <= i) return BOT;
              } else {
                  if ((ei > 0 && CAS(&e->id, &ei, -i)) || (ei == -i && c->val == TOP)) {
                      long Ei = q->Ei;
                      while (Ei <= i && !CAS(&q->Ei, &Ei, i + 1))
                          ;
                      c->val = ev;
                  }
              }

              return c->val;
          }

          static void help_deq(queue_t *q, handle_t *th, handle_t *ph) {
              deq_t *deq = &ph->Dr;
              long idx = ACQUIRE(&deq->idx);
              long id = deq->id;

              if (idx < id) return;

              node_t *Dp = ph->Dp;
              th->hzd_node_id = ph->hzd_node_id;
              FENCE();
              idx = deq->idx;

              long i = id + 1, old = id, new_idx = 0;
              while (1) {
                  node_t *h = Dp;
                  for (; idx == old && new_idx == 0; ++i) {
                      cell_t *c = find_cell(&h, i, th);

                      long Di = q->Di;
                      while (Di <= i && !CAS(&q->Di, &Di, i + 1))
                          ;

                      void *v = help_enq(q, th, c, i);
                      if (v == BOT || (v != TOP && c->deq == BOT))
                          new_idx = i;
                      else
                          idx = ACQUIRE(&deq->idx);
                  }

                  if (new_idx != 0) {
                      if (CASra(&deq->idx, &idx, new_idx)) idx = new_idx;
                      if (idx >= new_idx) new_idx = 0;
                  }

                  if (idx < 0 || deq->id != id) break;

                  cell_t *c = find_cell(&Dp, idx, th);
                  deq_t *cd = reinterpret_cast<deq_t*>(BOT);
                  if (c->val == TOP || CAS(&c->deq, &cd, deq) || cd == deq) {
                      CAS(&deq->idx, &idx, -idx);
                      break;
                  }

                  old = idx;
                  if (idx >= i) i = idx + 1;
              }
          }

          static void *deq_fast(queue_t *q, handle_t *th, long *id) {
              long i = FAAcs(&q->Di, 1);
              cell_t *c = find_cell(&th->Dp, i, th);
              void *v = help_enq(q, th, c, i);
              deq_t *cd = reinterpret_cast<deq_t*>(BOT);

              if (v == BOT) return BOT;
              if (v != TOP && CAS(&c->deq, &cd, TOP)) return v;

              *id = i;
              return TOP;
          }

          static void *deq_slow(queue_t *q, handle_t *th, long id) {
              deq_t *deq = &th->Dr;
              RELEASE(&deq->id, id);
              RELEASE(&deq->idx, id);

              help_deq(q, th, th);
              long i = -deq->idx;
              cell_t *c = find_cell(&th->Dp, i, th);
              void *val = c->val;

              th->stat.onSlowDequeue();

              return val == TOP ? BOT : val;
          }

          static void *dequeue(queue_t *q, handle_t *th) {
              th->hzd_node_id = th->deq_node_id;

              void *v;
              long id = 0;
              int p = MAX_PATIENCE;

              do
                  v = deq_fast(q, th, &id);
              while (v == TOP && p-- > 0);
              if (v == TOP)
                  v = deq_slow(q, th, id);
              else {
                  th->stat.onFastDequeue();
              }

              if (v != EMPTY) {
                  help_deq(q, th, th->Dh);
                  th->Dh = th->Dh->next;
              }

              th->deq_node_id = th->Dp->id;
              RELEASE(&th->hzd_node_id, -1);

              if (th->spare == NULL) {
                  cleanup(q, th);
                  th->spare = new_node();
              }

              if (v == EMPTY) th->stat.onEmpty();
              return v;
          }

          static void queue_init(queue_t *q, int nprocs) {
              q->Hi = 0;
              q->Hp = new_node();

              q->Ei = 1;
              q->Di = 1;

              q->nprocs = nprocs;

          }

          static void queue_free(queue_t *q, handle_t *h) {
              q->stat += h->stat;
          }

          static void queue_register(queue_t *q, handle_t *th, int id) {
              th->next = NULL;
              th->hzd_node_id = -1;
              th->Ep = q->Hp;
              th->enq_node_id = th->Ep->id;
              th->Dp = q->Hp;
              th->deq_node_id = th->Dp->id;

              th->Er.id = 0;
              th->Er.val = BOT;
              th->Dr.id = 0;
              th->Dr.idx = -1;

              th->Ei = 0;
              th->spare = new_node();

              static handle_t *volatile _tail;
              handle_t *tail = _tail;

              if (tail == NULL) {
                  th->next = th;
                  if (CASra(&_tail, &tail, th)) {
                      th->Eh = th->next;
                      th->Dh = th->next;
                      return;
                  }
              }

              handle_t *next = tail->next;
              do
                  th->next = next;
              while (!CASra(&tail->next, &next, th));

              th->Eh = th->next;
              th->Dh = th->next;
          }

          queue_t m_internal_queue DOUBLE_CACHE_ALIGNED;
          std::atomic<ssize_t> m_active_socket DOUBLE_CACHE_ALIGNED {-1};
          size_t m_size DOUBLE_CACHE_ALIGNED;
          size_t m_socket_boundary DOUBLE_CACHE_ALIGNED;
          std::vector<handle_t> m_handlers DOUBLE_CACHE_ALIGNED;
          bool m_queue_done  DOUBLE_CACHE_ALIGNED = false;
        public:
          using value_type = T;
          using item_counter = typename Traits::item_counter;
          using gc = cds::gc::nogc;
          WFQueue(size_t ids, ssize_t socket_boundary = -1)
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
              if(!m_queue_done) {
                for(auto& e: m_handlers) {
                  queue_free(&m_internal_queue, &e);
                }
                m_queue_done = true;
              }
              return m_internal_queue.stat;
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
              return static_cast<bool>(dest);
          }

          ~WFQueue() {
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

    using wf_queue::WFQueue;

}} // namespace cds::container

#undef BOT
#undef TOP
#undef CACHE_ALIGNED
#undef DOUBLE_CACHE_ALIGNED
#undef FAA
#undef FAAcs
#undef CAS
#undef CAScs
#undef CASra
#undef CASa
#undef SWAP
#undef SWAPra
#undef ACQUIRE
#undef RELEASE
#undef FENCE
#undef PAUSE

#endif // #ifndef CDSLIB_CONTAINER_WF_QUEUE_H
