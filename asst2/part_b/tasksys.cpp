#include "tasksys.h"
#include <cassert>
#include <iostream>

using std::cout;
using std::endl;

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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

TaskLaunch::TaskLaunch(IRunnable* runnable, int num_total_tasks, int num_deps) {
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    this->num_deps = num_deps;
    next_task = 0;
    num_done_tasks = 0;
    mutex = new std::mutex();
}

void TaskLaunch::print() {
    mutex->lock();
    cout << "{";
    cout << "num_total_tasks = " << num_total_tasks << ", ";
    cout << "num_deps = " << num_deps << ", ";
    cout << "next_task = " << next_task << ", ";
    cout << "num_done_tasks = " << num_done_tasks;
    cout << "}" << endl;
    mutex->unlock();
}

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
    _ready_or_stop = new std::condition_variable();
    _all_done = new std::condition_variable();
    _mutex = new std::mutex();
    _next_task_id = 0;
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
    _ready_or_stop->notify_all();

    for (int i = 0; i < _num_threads; ++i) {
        _workers[i].join();
    }

    for (auto pair : _task_launches) {
        delete pair.second;
    }

    delete[] _workers;
    delete _ready_or_stop;
    delete _all_done;
    delete _mutex;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    runAsyncWithDeps(runnable, num_total_tasks, std::vector<TaskID>());
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    std::unique_lock<std::mutex> lock(*_mutex);
    TaskID curr_task_id = _next_task_id;
    _next_task_id += 1;

    // build subsequent task map, remove deps which are already done
    int num_deps = deps.size();
    for (TaskID dep_id : deps) {
        if (_finished_tasks.count(dep_id) > 0) num_deps -= 1;
        else _subsequent_tasks[dep_id].push_back(curr_task_id);
    }

    _task_launches[curr_task_id] = new TaskLaunch(runnable, num_total_tasks, num_deps);
    if (num_deps == 0) {
        _ready_tasks.push_back(curr_task_id);
        lock.unlock();
        _ready_or_stop->notify_all();
    } else {
        _waiting_tasks.insert(curr_task_id);
        lock.unlock();
    }

    return curr_task_id;
}

void TaskSystemParallelThreadPoolSleeping::runTask() {
    while (true) {
        std::unique_lock<std::mutex> lock(*_mutex);

        // sleep if ready queue is empty
        _ready_or_stop->wait(lock, [this]{return _stop_running || _ready_tasks.size() > 0;});
        if (_stop_running) return;

        // read info of front task launch
        int task_id = _ready_tasks.front();
        TaskLaunch* task = _task_launches[task_id];
        IRunnable* runnable = task->runnable;
        int next_task_copy = task->next_task;
        task->next_task += 1;
        if (task->next_task == task->num_total_tasks)
            _ready_tasks.pop_front();  
        lock.unlock();

        // run task without any lock
        runnable->runTask(next_task_copy, task->num_total_tasks);

        // update # of done tasks 
        task->mutex->lock();
        task->num_done_tasks += 1;
        bool task_done = task->num_done_tasks == task->num_total_tasks;
        task->mutex->unlock();

        // update # of deps for subsequent tasks
        if (task_done) {
            lock.lock();
            bool add_new_task = false;
            bool finish_all_tasks = false;
            for (TaskID subseq_id : _subsequent_tasks[task_id]) {
                if (_finished_tasks.count(subseq_id) > 0) continue;
                TaskLaunch* subseq = _task_launches[subseq_id];
                subseq->num_deps -= 1;
                if (subseq->num_deps == 0) {
                    _waiting_tasks.erase(subseq_id);
                    _ready_tasks.push_back(subseq_id);
                    add_new_task = true;
                }
            }

            _finished_tasks.insert(task_id);
            finish_all_tasks = _finished_tasks.size() == _next_task_id;
            lock.unlock();

            if (add_new_task) _ready_or_stop->notify_all();
            if (finish_all_tasks) _all_done->notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    std::unique_lock<std::mutex> lock(*_mutex);
    _all_done->wait(lock, [this]{return _finished_tasks.size() == _next_task_id;});
    return;
}

void TaskSystemParallelThreadPoolSleeping::print() {
    // should be used with lock

    cout << "DEBUG: next_task_id = " << _next_task_id << endl;
    cout << "DEBUG: stop_running = " << _stop_running << endl;

    cout << "DEBUG: ready_tasks = [";
    for (TaskID id : _ready_tasks) {
        cout << id << ", ";
    }
    cout << "]" << endl;

    cout << "DEBUG: waiting_tasks = [";
    for (TaskID id : _waiting_tasks) {
        cout << id << ", ";
    }
    cout << "]" << endl;

    cout << "DEBUG: finished_tasks = [";
    for (TaskID id : _finished_tasks) {
        cout << id << ", ";
    }
    cout << "]" << endl;

    for (int i = 0; i < _next_task_id; ++i) {
        cout << "DEBUG: task_launch " << i << " = ";
        _task_launches[i]->print();
    }

    for (auto pair : _subsequent_tasks) {
        cout << "DEBUG: subsequent_tasks[" << pair.first << "] = [";
        for (auto subseq : pair.second) {
            cout << subseq << ", ";
        }
        cout << "]" << endl;
    }
}
