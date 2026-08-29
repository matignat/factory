#pragma once

#include <time.h>

////////////////////ERRORS///////////////////////////////
#define PLANTOK 0
#define ERROR -1

////////////////////TYPE DEFINITIONS//////////////////////
// Function type for task_t's task_function.
typedef int (*task_function_t)(int);

/*
 * A task to be executed by many workers.
 *@id: a unique task id.
 *@start: earliest possible start time for the task.
 *@task_function: the function to be called by each worker (`capacity` times).
 *@capacity: the number of workers needed to perform the task.
 *@data: array of size `capacity` with arguments to be used by workers when calling `task_function`.
 *@results: array of size `capacity` to store results computed by workers.
 */
typedef struct task_t {
    int id;
    time_t start;
    task_function_t task_function;
    int capacity;
    int* data;
    int* results;
} task_t;

// Forward declaration.
struct worker_t;

/* Function type for worker's work function.
 * @w: pointer to the worker_t performing the task.
 * @t: pointer to the task being performed.
 * @i: index of the worker in the task's capacity (0 .. capacity-1).
 */
typedef int (*worker_function_t)(struct worker_t* w, task_t* t, int i);

/* A worker in the plant.
 * @id: a unique worker id.
 * @start: the worker's start time.
 * @end: the worker's end time.
 * @work: the performing function of the worker.
 */
typedef struct worker_t {
    int id;
    time_t start;
    time_t end;
    worker_function_t work;
} worker_t;

///////////////////////////FUNCTIONALITY///////////////////////

// Initialize the plant.
// @stations: array of size `n_stations` of station capacities.
// @n_stations: number of stations in the plant.
// @n_workers: number of workers in the plant.
int init_plant(int* stations, int n_stations, int n_workers);

// Clean up plant resources.
int destroy_plant();

// Register a new worker.
int add_worker(worker_t* w);

// Register a new task.
int add_task(task_t* t);

// Collect the results of the task (blocking).
int collect_task(task_t* t);
