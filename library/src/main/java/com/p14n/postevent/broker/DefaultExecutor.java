package com.p14n.postevent.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DefaultExecutor implements AsyncExecutor {

        private final ScheduledExecutorService se;
        private final ExecutorService es;

        public DefaultExecutor(int scheduledSize) {
                this.se = createScheduledExecutorService(scheduledSize);
                this.es = createVirtualExecutorService();
        }

        public DefaultExecutor(int scheduledSize, int fixedSize) {
                this.se = createScheduledExecutorService(scheduledSize);
                this.es = createFixedExecutorService(fixedSize);
        }

        protected ExecutorService createFixedExecutorService(int size) {
                return Executors.newFixedThreadPool(size,
                                new ThreadFactoryBuilder().setNameFormat("post-event-fixed-%d").build());
        }

        protected ExecutorService createVirtualExecutorService() {
                return Executors.newThreadPerTaskExecutor(
                                new ThreadFactoryBuilder().setThreadFactory(Thread.ofVirtual().factory())
                                                .setNameFormat("post-event-virtual-%d").build());
        }

        protected ScheduledExecutorService createScheduledExecutorService(int size) {
                return Executors.newScheduledThreadPool(size,
                                new ThreadFactoryBuilder().setNameFormat("post-event-scheduled-%d").build());
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
