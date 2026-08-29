# Plant Multi-threaded Processing System (C Concurrency Project)

A concurrent simulation engine for a factory handling worker allocation, processing stations, and scheduled tasks, implemented in C.

Developed as part of a concurrent systems programming assignment.

---

## About the Project

The task involves implementing a robust, thread-safe factory plant management system located in the `plant/` directory. The factory initializes stations with specific capacities, registers workers with defined working time windows, accepts tasks (potentially scheduled for the future), and coordinates execution efficiently using multiple threads.

The core interface manages the lifecycle and operations of the plant:
* `init_plant` – Initializes the factory with given stations and expected worker count.
* `destroy_plant` – Gracefully cleans up resources, waiting for active execution to finish.
* `add_worker` – Registers a worker into the system.
* `add_task` – Submits a processing task.
* `collect_task` – Blocks until a specific task is completed and retrieves its status.

---

## System Architecture and Components

* **Tasks (`task_t`)**: Defined by a unique ID, execution start time (`start`), a function pointer (`task_function`), required worker capacity, and pointers for input data and output results. Tasks execute only when enough workers and a suitable station are available, and the current time matches or exceeds `start`.
* **Stations**: The factory features $S$ stations, each with a fixed capacity $s_i > 0$. A task occupies exactly one station whose capacity is greater than or equal to the task's required capacity (`task.capacity <= s_i`).
* **Workers (`worker_t`)**: Up to $P$ workers handle tasks within their operational time window (`start` to `end`). Each worker can handle only one task at a time and executes their assigned portion via the `work` callback function.

---

## Concurrency and Synchronization Rules

* **Thread Safety**: Functions like `add_task`, `add_collect`, and `add_worker` can be called concurrently from multiple threads.
* **Task Execution Flow**: A task runs when:
  1. The current time is $\ge$ `task.start`.
  2. Exactly `task.capacity` workers are idle and available.
  3. A free station with sufficient capacity is available.
* **Immediate Errors**: Tasks that cannot possibly be fulfilled due to lack of staff or structural limitations should fail immediately with `-1` when the system identifies the unrecoverable state.
* **Lifecycle Constraints**: Any interactions outside the `init_plant` and `destroy_plant` window are safely ignored, returning `-1`.

---

## Project Structure

* `plant/` – Directory containing the concurrent solution implementation.
* `common/plant.h` – Header file defining core data structures and function declarations.
* `Makefile` – Build and test configurations.

---

## Building and Testing

To compile and execute tests on the target environment, run:

```bash
make test
```
