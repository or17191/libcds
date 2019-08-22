#ifndef CDSSTRESS_SB_ADAPTORS_H
#define CDSSTRESS_SB_ADAPTORS_H

#include <type_traits>
#include <utility>

#include <cds/algo/uuid.h>

namespace queue {
template <class Queue, class Value>
static bool push(Queue &queue, Value &&value, size_t id,
                 decltype(std::declval<Queue>().enqueue(std::forward<Value>(value), id)) * = nullptr)
{
    return queue.enqueue(std::forward<Value>(value), id);
}

template <class Queue, class Value>
static bool push(Queue &queue, Value &&value, size_t id,
                 decltype(std::declval<Queue>().enqueue(std::forward<Value>(value))) * = nullptr)
{
    return queue.enqueue(std::forward<Value>(value));
}

template <class Queue, class Value>
static bool pop(Queue &queue, Value &&value, size_t id, cds::uuid_type &basket, std::true_type,
                decltype(std::declval<Queue>().dequeue(std::forward<Value>(value), id)) * = nullptr)
{
    return queue.dequeue(std::forward<Value>(value), id, std::addressof(basket));
}

template <class Queue, class Value>
static bool pop(Queue &queue, Value &&value, size_t id, cds::uuid_type &basket, std::false_type,
                decltype(std::declval<Queue>().dequeue(std::forward<Value>(value), id)) * = nullptr)
{
    basket = 0;
    return queue.dequeue(std::forward<Value>(value), id);
}

template <class Queue, class Value, class HasBaskets>
static bool pop(Queue &queue, Value &&value, size_t id, cds::uuid_type &basket, HasBaskets,
                decltype(std::declval<Queue>().dequeue(std::forward<Value>(value))) * = nullptr)
{
    return queue.dequeue(std::forward<Value>(value));
}

template <class T>
using safe_add_pointer = std::add_pointer<typename std::remove_pointer<T>::type>;

} // namespace queue

#endif //  CDSSTRESS_SB_ADAPTORS_H
