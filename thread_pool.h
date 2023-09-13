#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <memory>
#include <future>

namespace gammasoft {

class ThreadPool
{
    std::unique_ptr<class ThreadPoolImpl> m_impl;
public:
    typedef std::shared_ptr<ThreadPool> Ptr;

    ThreadPool(int maxThrCnt);
    virtual ~ThreadPool();

    template<class F, class... Args>
        auto putTask(F&& f, Args&&... args)
            -> std::future<typename std::result_of<F(Args...)>::type>;
};

}

#endif // THREAD_POOL_H
