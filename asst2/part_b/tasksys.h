#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <unordered_set>
#include <unordered_map>

#include "itasksys.h"

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/* Record every bulk task launch. */
struct TaskLaunch {
    IRunnable* runnable;
    int num_total_tasks;
    int num_deps;
    int next_task;
    int num_done_tasks;
    std::mutex *mutex;

    TaskLaunch(IRunnable* runnable, int num_total_tasks, int num_deps);
    void print();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();

    private:
        int _num_threads;
        std::thread* _workers;
        std::condition_variable* _ready_or_stop;
        std::condition_variable* _all_done;
        std::mutex* _mutex;
        TaskID _next_task_id;
        bool _stop_running;

        std::deque<TaskID> _ready_tasks;
        std::unordered_set<TaskID> _waiting_tasks;
        std::unordered_set<TaskID> _finished_tasks;
        std::unordered_map<TaskID, TaskLaunch*> _task_launches;
        std::unordered_map<TaskID, std::vector<TaskID>> _subsequent_tasks;

        void runTask();
        void print();
};

#endif
