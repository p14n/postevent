package com.p14n.postevent.catchup;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Interface defining behavior for executing tasks one at a time with signal
 * tracking.
 * Provides thread-safe execution of tasks while maintaining a count of pending
 * signals
 * and ensuring only one task runs at any given time.
 *
 * <p>
 * This interface is useful for implementing throttling or sequential processing
 * patterns where multiple requests need to be processed one at a time, with the
 * ability
 * to trigger follow-up actions when new signals arrive during processing.
 * </p>
 *
 */
public interface OneAtATimeBehaviour {

    /**
     * Returns the atomic counter tracking the number of signals received.
     * This counter is used to track pending tasks or signals that arrive while
     * processing is in progress.
     *
     * @return AtomicInteger representing the number of pending signals
     */
    public AtomicInteger getSignals();

    /**
     * Returns the atomic boolean indicating whether a task is currently running.
     * This flag is used to ensure mutual exclusion between tasks.
     *
     * @return AtomicBoolean representing the running state
     */
    public AtomicBoolean getRunning();

    /**
     * Executes tasks one at a time with signal tracking.
     * This method ensures that only one task runs at a time while maintaining
     * a count of signals received during execution.
     *
     * <p>
     * The method operates as follows:
     * </p>
     * <ol>
     * <li>Increments the signal counter to track the incoming request</li>
     * <li>If a task is already running, returns immediately</li>
     * <li>If no task is running, acquires the lock and:
     * <ul>
     * <li>Resets the signal counter</li>
     * <li>Executes the main task</li>
     * <li>If new signals arrived during execution, triggers the next task</li>
     * </ul>
     * </li>
     * </ol>
     *
     * @param todo     The main task to execute when acquiring the lock
     * @param nextTodo The follow-up task to execute if new signals arrive during
     *                 processing
     * @throws RuntimeException if either task throws an exception during execution
     */
    public default void oneAtATime(Runnable todo, Runnable nextTodo) {
        getSignals().incrementAndGet();
        if (getRunning().get()) {
            return;
        }
        synchronized (getRunning()) {
            if (!getRunning().get()) {
                getRunning().set(true);
                getSignals().set(0);
                todo.run();
                getRunning().set(false);
                if (getSignals().get() > 0) {
                    nextTodo.run();
                }
            }
        }
    }
}
