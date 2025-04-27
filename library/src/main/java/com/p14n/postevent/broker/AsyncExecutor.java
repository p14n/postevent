package com.p14n.postevent.broker;

import java.util.List;
import java.util.concurrent.*;

/**
 * Interface for asynchronous task execution.
 */
public interface AsyncExecutor extends AutoCloseable {

    /**
     * Schedules a task for repeated fixed-rate execution.
     * 
     * @param command      The task to execute
     * @param initialDelay The time to delay first execution
     * @param period       The period between successive executions
     * @param unit         The time unit of the initialDelay and period parameters
     * @return A ScheduledFuture representing pending completion of the task
     */
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit);

    /**
     * Shuts down the executor and returns a list of runnables that were not
     * executed.
     * 
     * @return A list of runnables that were not executed
     */
    List<Runnable> shutdownNow();

    /**
     * Submits a task for execution and returns a Future representing the pending
     * 
     * @param task The task to submit
     * @param <T>  The type of the task
     * @return A Future representing pending completion of the task
     */
    <T> Future<T> submit(Callable<T> task);

}
