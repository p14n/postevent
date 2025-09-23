package com.p14n.postevent.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

/**
 * Default implementation of {@link AsyncExecutor} that provides configurable
 * thread pool execution.
 * Supports both virtual threads and fixed-size thread pools for task execution,
 * along with
 * scheduled task capabilities.
 *
 * <p>
 * Key features:
 * </p>
 * <ul>
 * <li>Virtual thread pool support for efficient concurrent task execution</li>
 * <li>Configurable fixed-size thread pool alternative</li>
 * <li>Scheduled task execution with customizable intervals</li>
 * <li>Named thread factories for better debugging and monitoring</li>
 * </ul>
 */
public class DefaultExecutor implements AsyncExecutor {

        private final ScheduledExecutorService se;
        private final ExecutorService es;

        /**
         * Creates a new executor with a scheduled thread pool and virtual thread pool.
         *
         * @param scheduledSize the size of the scheduled thread pool
         */
        public DefaultExecutor(int scheduledSize) {
                this.se = createScheduledExecutorService(scheduledSize);
                this.es = createVirtualExecutorService();
        }

        /**
         * Creates a new executor with both scheduled and fixed-size thread pools.
         *
         * @param scheduledSize the size of the scheduled thread pool
         * @param fixedSize     the size of the fixed thread pool
         */
        public DefaultExecutor(int scheduledSize, int fixedSize) {
                this.se = createScheduledExecutorService(scheduledSize);
                this.es = createFixedExecutorService(fixedSize);
        }

        protected ThreadFactory createNamedFactory(String nameFormat,ThreadFactory backingFactory){
                AtomicLong count = (nameFormat != null) ? new AtomicLong(0) : null;
                return runnable -> {
                        Thread thread = backingFactory.newThread(runnable);
                        if (nameFormat != null) {
                                thread.setName(format(nameFormat, count.getAndIncrement()));
                        }
                        return thread;
                };
        }
        protected ThreadFactory createNamedFactory(String nameFormat) {
                return createNamedFactory(nameFormat,Executors.defaultThreadFactory());
        }
        /**
         * Creates a fixed-size thread pool with named threads.
         *
         * @param size the number of threads in the pool
         * @return a fixed thread pool executor service
         */
        protected ExecutorService createFixedExecutorService(int size) {
                return Executors.newFixedThreadPool(size,
                        createNamedFactory("post-event-fixed-%d"));
        }

        /**
         * Creates a virtual thread pool for efficient task execution.
         *
         * @return a virtual thread executor service
         */
        protected ExecutorService createVirtualExecutorService() {
                return Executors.newThreadPerTaskExecutor(
                        createNamedFactory("post-event-virtual-%d",Thread.ofVirtual().factory()));
        }

        /**
         * Creates a scheduled thread pool with named threads.
         *
         * @param size the number of threads in the pool
         * @return a scheduled thread pool executor service
         */
        protected ScheduledExecutorService createScheduledExecutorService(int size) {
                return Executors.newScheduledThreadPool(size,
                                createNamedFactory("post-event-scheduled-%d"));
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
                return se.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public List<Runnable> shutdownNow() {
                var x = new ArrayList<Runnable>();
                x.addAll(es.shutdownNow());
                x.addAll(se.shutdownNow());
                return x;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
                return es.submit(task);
        }

        @Override
        public void close() throws Exception {
                shutdownNow();
        }
}
