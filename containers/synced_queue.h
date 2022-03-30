#include <unordered_map>
#include <queue>
#include <optional>

#include "absl/synchronization/mutex.h"

template <typename T>
class SyncedQueue
{
private:
   std::queue<T> m_queue;
   absl::Mutex m_mutex;

   bool not_empty() const
   {
       return !m_queue.empty();
   }

public:
   void push(const T& item)
   {
       absl::WriterMutexLock writer_lock(&m_mutex);
       m_queue.push(std::move(item));
   }

   std::optional<T> pop()
   {
       if(m_mutex.LockWhenWithTimeout(absl::Condition(this, &SyncedQueue<T>::not_empty), absl::Milliseconds(400)))
       {
           auto result = std::move(m_queue.front());
           m_queue.pop();
           m_mutex.Unlock();
           return result;
       }
       return std::nullopt;
   }

};

template <typename T>
class SyncedQueueMap
{
   private:
       std::unordered_map<std::string, SyncedQueue<T>>
           m_queue_map;
       absl::Mutex m_queue_mutex;

   public:
       void push(const std::string& stream_name, const T& output)
       {
           absl::WriterMutexLock lock(&m_queue_mutex);
           m_queue_map.try_emplace(stream_name);
           m_queue_map[stream_name].push(output);
       }

       std::optional<T> pop(const std::string& stream_name)
       {
           SyncedQueue<T>* queue_ptr;
           {
               absl::MutexLock lock(&m_queue_mutex);
               m_queue_map.try_emplace(stream_name);
               queue_ptr = &m_queue_map[stream_name];
           }
           return queue_ptr->pop();
       }

};