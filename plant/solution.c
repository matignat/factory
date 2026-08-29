#include "../common/plant.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#define pml(m) pthread_mutex_lock(&(m))
#define pmu(m) pthread_mutex_unlock(&(m))

/*
 *   Author: Mateusz Gnat
 *   Project: Factory
 * - Global plant state is protected by plant.mutex. The plant acts as a monitor.
 *   Features custom structures to contain all important data of tasks and workers.
 * - Scheduler thread assigns tasks to workers only when:
 *   (1) task.start has passed, (2) a station with sufficient capacity is free,
 *   (3) enough workers are available (IDLE and between start/end work time).
 * - A PENDING task becomes RUNNING after assignment; it becomes DONE when all assigned workers finish.
 *   Task might not be accepted in add_task if it's  undoable 
 *   and is FAILED if it couldn't be computed (not enough workers).
 * - destroy_plant() performs a graceful shutdown:
 *   when closing it stops accepting new workers/tasks, waits until all tasks become DONE/FAILED,
 *   then sets working to false, plant stops scheduler/workers and frees all resources.
 */

// Woker states
typedef enum {
    NOT_STARTED,
    IDLE,
    BUSY,
    ENDED
} worker_state_t;

// Task states
typedef enum {
    PENDING,
    DONE,
    RUNNING,
    FAILED
} task_status_t;

// Task info struct
typedef struct task_entry {
    task_t* t;
    task_status_t status;
    int remaining; // People working on this task
    pthread_cond_t done; // Place to wait for result (for collect_task)
    int station; // Factory station number
    struct task_entry* next; // Next task pointer (creates a list)
} task_entry_t;

// Worker info struct
typedef struct {
    worker_t* w;
    worker_state_t state;
    task_t* task; // Current task
    int index; // Index in task->results
    int w_idx; // Index in workers array
    pthread_t thread;
} worker_meta_t;

// Factory struct
typedef struct plant_t {
    int s;
    int* s_i;
    int* s_busy; // If station busy 1 else 0
    int max_s_cap; // Max capacity (max(s_i))

    int P; // Max worker count
    int registered; // Current registered worker count

    worker_meta_t* workers;
    pthread_cond_t* waiting; // Workers conditions

    pthread_cond_t sch_cond; // Scheduler thread condition

    task_entry_t* tasks;
    atomic_bool working; // Factory working - flag
    atomic_bool closing; // In destruction process

    pthread_mutex_t mutex;
} plant_t;

// Global plant (working to false until we ini. the plant)
plant_t plant = {
    .working = ATOMIC_VAR_INIT(false)
};

// Place for scheduler (needed to wait for it's end using join())
pthread_t scheduler_tid;

// Definitions
void* scheduler_thread(void* arg);
void* worker_thread(void* arg);

void refresh_worker_state(worker_meta_t* wm, time_t now);
bool available(worker_meta_t* wm, time_t now);
int pick_station(int need);

task_entry_t* find_entry(task_t* t);
struct timespec add_1s(void);
bool try_start_task(task_entry_t* te);

int init_plant(int* stations, int n_stations, int n_workers)
{
    plant.tasks = NULL;

    // Initialize stations (copy s_i and use malloc)
    plant.s = n_stations;
    plant.s_i = malloc(sizeof(int) * n_stations);
    for (int i = 0; i < n_stations; i++)
        plant.s_i[i] = stations[i];

    plant.s_busy = calloc(n_stations, sizeof(int));
    int maxcap = 0;
    for (int i = 0; i < plant.s; i++)
        if (plant.s_i[i] > maxcap)
            maxcap = plant.s_i[i];
    plant.max_s_cap = maxcap;

    // Intialize workers
    plant.P = n_workers;
    plant.registered = 0;

    // Conditions
    plant.workers = calloc(n_workers, sizeof(worker_meta_t));
    plant.waiting = malloc(sizeof(pthread_cond_t) * n_workers);
    for (int i = 0; i < n_workers; i++)
        pthread_cond_init(&plant.waiting[i], NULL);

    pthread_cond_init(&plant.sch_cond, NULL);

    pthread_mutex_init(&plant.mutex, NULL);

    // Flags
    atomic_store(&plant.closing, false);
    atomic_store(&plant.working, true);

    // Create scheduler thread
    pthread_create(&scheduler_tid, NULL, scheduler_thread, NULL);

    return PLANTOK;
}

bool all_tasks_finished(void)
{
    for (task_entry_t* te = plant.tasks; te; te = te->next)
        if (te->status == PENDING || te->status == RUNNING)
            return false;
    return true;
}

int destroy_plant()
{
    // Check if mutex exists
    if (!atomic_load(&plant.working))
        return ERROR;

    pml(plant.mutex);

    // Decline consecutive destroy calls
    if (!atomic_load(&plant.working) || atomic_load(&plant.closing)) {
        pmu(plant.mutex);
        return ERROR;
    }

    // Flag
    atomic_store(&plant.closing, true);

    // Wake scheduler to finish tasks
    pthread_cond_broadcast(&plant.sch_cond);

    // Wake after each ended task and check if it was the last one
    while (!all_tasks_finished())
        pthread_cond_wait(&plant.sch_cond, &plant.mutex);

    atomic_store(&plant.working, false);

    pthread_cond_broadcast(&plant.sch_cond);

    for (int i = 0; i < plant.registered; i++)
        pthread_cond_broadcast(&plant.waiting[i]);

    pmu(plant.mutex);

    // Wait for scheduler and all waorker
    pthread_join(scheduler_tid, NULL);
    for (int i = 0; i < plant.registered; i++)
        pthread_join(plant.workers[i].thread, NULL);

    // Destroy conditions
    for (int i = 0; i < plant.P; i++)
        pthread_cond_destroy(&plant.waiting[i]);

    pthread_cond_destroy(&plant.sch_cond);
    pthread_mutex_destroy(&plant.mutex);

    // Destroy task conditions
    task_entry_t* te = plant.tasks;
    while (te) {
        task_entry_t* nxt = te->next;
        pthread_cond_destroy(&te->done);
        free(te);
        te = nxt;
    }

    free(plant.waiting);
    free(plant.workers);
    free(plant.s_busy);
    free(plant.s_i);

    return PLANTOK;
}

int add_worker(worker_t* w)
{
    // Check if mutex exists
    if (!atomic_load(&plant.working))
        return ERROR;

    pml(plant.mutex);

    if (!atomic_load(&plant.working) || atomic_load(&plant.closing)) {
        pmu(plant.mutex);
        return ERROR;
    }

    // Check if plant is full
    if (plant.registered >= plant.P) {
        pmu(plant.mutex);
        return ERROR;
    }

    //  Check if id is unique
    for (int i = 0; i < plant.registered; i++) {
        if (plant.workers[i].w && plant.workers[i].w->id == w->id) {
            pmu(plant.mutex);
            return ERROR;
        }
    }

    // Set worker meta
    int wi = plant.registered;
    worker_meta_t* wm = &plant.workers[plant.registered++];

    wm->w = w;
    wm->state = NOT_STARTED;
    wm->task = NULL;
    wm->index = -1;
    wm->w_idx = wi;

    // Info ready, start thread, check if creation succeded
    int th = pthread_create(&wm->thread, NULL, worker_thread, wm);
    if (th != 0) {
        plant.registered--;
        wm->w = NULL;
        pmu(plant.mutex);
        return ERROR;
    }

    // Tell the plant that it's state changed
    pthread_cond_broadcast(&plant.sch_cond);
    pmu(plant.mutex);

    return PLANTOK;
}

int add_task(task_t* t)
{
    // Check if mutex exists
    if (!atomic_load(&plant.working))
        return ERROR;

    pml(plant.mutex);

    if (!atomic_load(&plant.working) || atomic_load(&plant.closing)) {
        pmu(plant.mutex);
        return ERROR;
    }

    // Ignore exisiting id's
    for (task_entry_t* cur = plant.tasks; cur; cur = cur->next) {
        if (cur->t->id == t->id) {
            pmu(plant.mutex);
            return ERROR;
        }
    }

    // Skip impossible tasks
    if (t->capacity <= 0 || t->capacity > plant.P || t->capacity > plant.max_s_cap) {
        pmu(plant.mutex);
        return ERROR;
    }

    // Set task entry
    task_entry_t* te = calloc(1, sizeof(*te));
    if (!te) {
        pmu(plant.mutex);
        return ERROR;
    }

    te->t = t;
    te->station = -1;
    te->remaining = 0;
    te->next = NULL;
    pthread_cond_init(&te->done, NULL);
    te->status = PENDING;

    if (!plant.tasks)
        plant.tasks = te;
    else {
        task_entry_t* last = plant.tasks;
        while (last->next)
            last = last->next;
        last->next = te;
    }

    pthread_cond_broadcast(&plant.sch_cond);
    pmu(plant.mutex);

    return PLANTOK;
}

int collect_task(task_t* t)
{
    // Check if mutex exists
    if (!atomic_load(&plant.working))
        return ERROR;

    pml(plant.mutex);

    if (!atomic_load(&plant.working) || atomic_load(&plant.closing)) {
        pmu(plant.mutex);
        return ERROR;
    }

    task_entry_t* te = find_entry(t);
    if (!te) {
        pmu(plant.mutex);
        return ERROR;
    }

    // Wait on tasks condition
    while (te->status != DONE && te->status != FAILED) {
        pthread_cond_wait(&te->done, &plant.mutex);
    }

    int ret = (te->status == DONE) ? PLANTOK : ERROR;
    pmu(plant.mutex);
    return ret;
}

// Find task entry in plant's array (by id)
task_entry_t* find_entry(task_t* t)
{
    for (task_entry_t* te = plant.tasks; te; te = te->next)
        if (te->t->id == t->id)
            return te;
    return NULL;
}

void* worker_thread(void* arg)
{
    worker_meta_t* wm = arg;
    int wi = wm->w_idx;

    pml(plant.mutex);

    while (atomic_load(&plant.working)) {
        time_t now = time(NULL);
        refresh_worker_state(wm, now);
        if (wm->state == ENDED)
            break;

        // Wait for task
        while (atomic_load(&plant.working) && wm->task == NULL) {
            pthread_cond_wait(&plant.waiting[wi], &plant.mutex);
            now = time(NULL);
            refresh_worker_state(wm, now);
            if (wm->state == ENDED)
                break;
        }

        // Here we have our task
        if (!atomic_load(&plant.working) || wm->state == ENDED)
            break;
        if (wm->task == NULL)
            continue;

        task_t* t = wm->task;
        int idx = wm->index;

        // Work
        pmu(plant.mutex);
        int r = wm->w->work(wm->w, t, idx);
        pml(plant.mutex);

        // Write results / end task if last worker
        t->results[idx] = r;

        task_entry_t* te = find_entry(t);

        if (te && te->status == RUNNING) {
            te->remaining--;
            if (te->remaining == 0) {
                te->status = DONE;
                plant.s_busy[te->station] = 0;
                pthread_cond_broadcast(&te->done);
            }
        }

        wm->task = NULL;
        wm->index = -1;

        now = time(NULL);
        if (now >= wm->w->end)
            wm->state = ENDED;
        else
            wm->state = IDLE;

        pthread_cond_broadcast(&plant.sch_cond);
    }

    wm->state = ENDED;
    pthread_cond_broadcast(&plant.sch_cond);
    pmu(plant.mutex);

    return NULL;
}

void refresh_worker_state(worker_meta_t* wm, time_t now)
{
    if (wm->state == NOT_STARTED && now >= wm->w->start)
        wm->state = IDLE;

    if (wm->state != BUSY && now >= wm->w->end)
        wm->state = ENDED;
}

bool available(worker_meta_t* wm, time_t now)
{
    return (wm->state == IDLE && wm->task == NULL && now < wm->w->end);
}

int pick_station(int need)
{
    for (int i = 0; i < plant.s; i++) {
        if (plant.s_busy[i] == 0 && plant.s_i[i] >= need)
            return i;
    }

    return -1;
}

struct timespec add_1s()
{
    struct timespec ts;
    ts.tv_sec = time(NULL) + 1;
    ts.tv_nsec = 0;
    return ts;
}

bool try_start_task(task_entry_t* te)
{
    // Set time
    time_t now = time(NULL);

    // Filter tasks
    if (te->status != PENDING)
        return false;
    if (now < te->t->start)
        return false;

    // Set need
    int need = te->t->capacity;

    // Pick sufficient station
    int st = pick_station(need);
    if (st < 0)
        return false;

    // Choose workers
    int* chosen = malloc(sizeof(int) * need);
    if (!chosen)
        return false;

    // Count available workers
    int cnt = 0;
    for (int i = 0; i < plant.registered && cnt < need; i++) {
        worker_meta_t* wm = &plant.workers[i];
        refresh_worker_state(wm, now);
        if (available(wm, now))
            chosen[cnt++] = i;
    }

    if (cnt < need) {
        free(chosen);
        return false;
    }

    // Commit the task
    te->status = RUNNING;
    te->station = st;
    te->remaining = need;
    plant.s_busy[st] = 1;

    // Start working, set worker status
    for (int k = 0; k < need; k++) {
        int wi = chosen[k];
        worker_meta_t* wm = &plant.workers[wi];

        wm->task = te->t;
        wm->index = k;
        wm->state = BUSY;
        pthread_cond_signal(&plant.waiting[wm->w_idx]);
    }

    free(chosen);
    return true;
}

// Counts max workes for all worker start times to see if task will ever be doable
static int max_sim_workers(time_t t)
{
    int best = 0;

    for (int ci = -1; ci < plant.registered; ci++) {
        time_t c = (ci == -1) ? t : plant.workers[ci].w->start;
        // Skip earlier starts
        if (c < t)
            continue;

        int cnt = 0;
        for (int j = 0; j < plant.registered; j++) {
            worker_t* w = plant.workers[j].w;
            if (w->start <= c && c < w->end)
                cnt++;
        }
        if (cnt > best)
            best = cnt;
    }
    return best;
}

void* scheduler_thread(void* arg)
{
    (void)arg;
    pml(plant.mutex);

    while (atomic_load(&plant.working)) {
        time_t now = time(NULL);

        // Refresh workers
        for (int i = 0; i < plant.registered; i++)
            refresh_worker_state(&plant.workers[i], now);

        bool progressed = false;

        // Try starting a task
        for (task_entry_t* te = plant.tasks; te; te = te->next) {
            if (try_start_task(te)) {
                progressed = true;
            }
        }

        if (atomic_load(&plant.closing)) {
            bool changed = false;
            time_t now2 = time(NULL);

            // Check all pending tasks on plant closure
            for (task_entry_t* te = plant.tasks; te; te = te->next) {
                if (te->status != PENDING)
                    continue;

                time_t t0 = now2;
                if (t0 < te->t->start)
                    t0 = te->t->start;

                // If we can't do it later, set as failed and wake up collect
                if (max_sim_workers(t0) < te->t->capacity) {
                    te->status = FAILED;
                    pthread_cond_broadcast(&te->done);
                    changed = true;
                }
            }

            // Wake destroying thread to check if it was the last task in progress
            if (changed) {
                progressed = true;
                pthread_cond_broadcast(&plant.sch_cond);
            }
        }

        // If nothing happened go to sleep, else try starting another one
        if (!progressed) {
            struct timespec ts = add_1s();
            pthread_cond_timedwait(&plant.sch_cond, &plant.mutex, &ts);
        }
    }

    pmu(plant.mutex);
    return NULL;
}