#ifndef CDSLIB_DETAILS_MEMKIND_ALLOCATOR_H
#define CDSLIB_DETAILS_MEMKIND_ALLOCATOR_H

#include <memkind.h>

namespace cds { namespace details {
        template <class T>
        struct MemkindDeleter {
          size_t count;
          MemkindDeleter(MemkindDeleter&&) = default;
          MemkindDeleter& operator=(MemkindDeleter&&) = default;
          void operator()(T* ptr) const {
            for(size_t i = 0; i < count; ++i) {
              (&ptr[i])->~T();
            }
            memkind_free(MEMKIND_HUGETLB, ptr);
          }
        };

        template <class T>
        using MemkindUniquePtr = std::unique_ptr<T[], MemkindDeleter<T>>;

        template <class T>
        MemkindUniquePtr<T> memkind_array(size_t count) {
          MemkindUniquePtr<T> ret(
              reinterpret_cast<T*>(memkind_calloc(MEMKIND_HUGETLB, count, sizeof(T))),
              MemkindDeleter<T>{count});
          for(size_t i = 0; i < count; ++i) {
            new (ret.get() + i) T();
          }
          return ret;
        }


        template <class T>
        struct memkind_allocator {
          using value_type = T;

          template <class T2>
          struct rebind {
            using other = memkind_allocator<T2>;
          };

          T* allocate(size_t n) {
            return reinterpret_cast<T*>(memkind_calloc(MEMKIND_HUGETLB, n, sizeof(value_type)));
          }

          void deallocate(T* p, size_t n) {
            memkind_free(MEMKIND_HUGETLB, p);
          }
        };

        template <class T>
        using memkind_vector = std::vector<T, memkind_allocator<T>>;
 }}
#endif // CDSLIB_DETAILS_MEMKIND_ALLOCATOR_H
