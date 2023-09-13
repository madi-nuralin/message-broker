#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <stdexcept>

#include "thread_pool.h"

namespace gammasoft {

class ThreadPoolImpl
{
    std::vector<std::thread> m_threads;
    std::queue<std::function<void()>> m_tasksQueue;
    std::mutex m_mutex;
    std::condition_variable m_condition;
    bool m_stop;

public:
    ThreadPoolImpl(int maxThrCnt);
    virtual ~ThreadPoolImpl();

    template<class F, class... Args>
        auto putTask(F&& f, Args&&... args) 
            -> std::future<typename std::result_of<F(Args...)>::type>;
};

inline ThreadPoolImpl::ThreadPoolImpl(int maxThrCnt)
    : m_stop(false)
{
    for( int i = 0; i < maxThrCnt; i++ ) {
        m_threads.emplace_back([this] {
            for(;;) {
                std::function<void()> runTask;
                {
                    std::unique_lock<std::mutex> lock(m_mutex);
                    m_condition.wait(lock, [this] {
                        return m_stop || !m_tasksQueue.empty();
                    });
                    if (m_stop && m_tasksQueue.empty()) {
                        return;
                    }
                    runTask = std::move(m_tasksQueue.front());
                    m_tasksQueue.pop();
                }

                runTask();
            }
        });
    }
}

template<class F, class... Args>
auto ThreadPoolImpl::putTask(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>
{
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );
        
    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_stop) {
            throw std::runtime_error("putTask is not permitted");
        }
        m_tasksQueue.emplace([task](){ (*task)(); });
    }

    m_condition.notify_one();
    return res;
}

inline ThreadPoolImpl::~ThreadPoolImpl()
{
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_stop = true;
    }

    m_condition.notify_all();

    for( auto it = m_threads.begin(); it != m_threads.end(); ++it ) {
        it->join();
    }
    m_threads.clear();
}

ThreadPool::ThreadPool(int maxThrCnt)
{
    m_impl = std::make_unique<ThreadPoolImpl>(maxThrCnt);
}

template<class F, class... Args>
auto ThreadPool::putTask(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>
{
    return m_impl->putTask(f, args);
}

}
