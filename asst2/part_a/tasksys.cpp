#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    _num_threads = num_threads;
    _workers = new std::thread[_num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete[] _workers;
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i] = std::thread(
                &TaskSystemParallelSpawn::runTask, this, runnable, i, num_total_tasks);
    }

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i].join();
    }
}

void TaskSystemParallelSpawn::runTask(IRunnable* runnable, int thread_id, int num_total_tasks) {
    for (int i = thread_id; i < num_total_tasks; i += _num_threads) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    _num_threads = num_threads;
    _workers = new std::thread[_num_threads];
    _mutex = new std::mutex();
    _runnable = nullptr;
    _num_total_tasks = 0;
    _next_task = 0;
    _finished_task = 0;
    _stop_running = false;

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runTask, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // notify all threads to stop spinning
    _mutex->lock();
    _stop_running = true;
    _mutex->unlock();

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i].join();
    }

    delete[] _workers;
    delete _mutex;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::unique_lock<std::mutex> lock(*_mutex);
    _runnable = runnable;
    _num_total_tasks = num_total_tasks;
    _next_task = 0;
    _finished_task = 0;
    lock.unlock();

    while (true) {
        lock.lock();
        if (_finished_task == _num_total_tasks) {
            _runnable = nullptr;
            _num_total_tasks = 0;
            return;
        }
        lock.unlock();
    }
}

void TaskSystemParallelThreadPoolSpinning::runTask() {
    while (true) {
        // create unique lock here so it will be unlocked when we leave the while loop
        std::unique_lock<std::mutex> lock(*_mutex);
        if (_stop_running) break;
        if (_num_total_tasks == 0) continue;
        if (_next_task == _num_total_tasks) continue;

        // update _next_task with the lock
        int next_task_copy = _next_task;
        _next_task += 1;
        lock.unlock();

        // run task without the lock (allowing other threads to run tasks)
        _runnable->runTask(next_task_copy, _num_total_tasks);

        // update _finished_task with the lock
        lock.lock();
        _finished_task += 1;
        lock.unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    _num_threads = num_threads;
    _workers = new std::thread[_num_threads];
    _launch_or_stop = new std::condition_variable();
    _task_done = new std::condition_variable();
    _mutex = new std::mutex();
    _runnable = nullptr;
    _num_total_tasks = 0;
    _next_task = 0;
    _finished_task = 0;
    _stop_running = false;

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runTask, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    _mutex->lock();
    _stop_running = true;
    _mutex->unlock();

    // notify all threads to stop running
    _launch_or_stop->notify_all();

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i].join();
    }

    delete[] _workers;
    delete _launch_or_stop;
    delete _task_done;
    delete _mutex;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::unique_lock<std::mutex> lock(*_mutex);
    _runnable = runnable;
    _num_total_tasks = num_total_tasks;
    _next_task = 0;
    _finished_task = 0;

    // notify all workers to start working
    lock.unlock();
    _launch_or_stop->notify_all();

    // wake up condition: all tasks are finished
    lock.lock();
    _task_done->wait(lock, [this]{return _finished_task == _num_total_tasks;});
    _runnable = nullptr;
    _num_total_tasks = 0;
    lock.unlock();
}

void TaskSystemParallelThreadPoolSleeping::runTask() {
    while (true) {
        std::unique_lock<std::mutex> lock(*_mutex);

        // wake up condition: we should stop running or we have some work to do
        _launch_or_stop->wait(lock, [this]{
                return _stop_running || (_num_total_tasks > 0 && _next_task < _num_total_tasks);});
        if (_stop_running) return;

        // update _next_task with the lock
        int next_task_copy = _next_task;
        _next_task += 1;
        lock.unlock();

        // run task without the lock (allowing other threads to run tasks)
        _runnable->runTask(next_task_copy, _num_total_tasks);

        // update _finished_task with the lock, notify main thread if all tasks are done
        lock.lock();
        _finished_task += 1;
        if (_finished_task == _num_total_tasks) {
            lock.unlock();
            _task_done->notify_one();
            continue;
        }
        lock.unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
