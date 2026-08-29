// A simple usage demo of the 'plant' module.
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "../common/plant.h"

int simple_work(worker_t* w, task_t* t, int number)
{
    printf("Worker %d: starting task %d.\n", w->id, t->id);
    const int i = t->task_function(t->data[number]);
    printf("Worker %d: finished task %d.\n", w->id, t->id);
    return i;
}

int square(int i)
{
    return i * i;
}

////////////////////TESTS///////////////////////////
int test1()
{
    const int n_stations = 1;
    const int n_workers = 2;
    int stations[n_stations];
    init_plant(stations, n_stations, n_workers);
    printf("Plant initialized.\n");

    // A new worker.
    worker_t w;
    w.id = 0;
    w.start = time(0);
    w.end = w.start + 10;
    w.work = simple_work;

    add_worker(&w);
    printf("Worker id %d added\n", w.id);

    // Another new worker.
    worker_t w2;
    w2.id = 1;
    w2.start = time(0);
    w2.end = w.start + 10;
    w2.work = simple_work;

    add_worker(&w2);
    printf("Worker id %d added\n", w2.id);

    // A new task.
    task_t t;
    int data[2] = { 1, 2 };
    int results[2] = { -1, -1 };
    t.id = 0;
    t.data = data;
    t.results = results;
    t.capacity = 2;
    t.task_function = square;

    add_task(&t);
    printf("Task id %d added\n", w.id);

    collect_task(&t);
    printf("Task id %d collected\n", t.id);
    printf("Collected %d %d\n", t.results[0], t.results[1]);

    return 0;

    printf("Test1 passed.\n");
}

int main()
{
    test1();
    return 0;
}