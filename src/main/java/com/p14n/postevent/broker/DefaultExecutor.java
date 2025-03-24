package com.p14n.postevent.broker;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class DefaultExecutor implements AsyncExecutor {

    private final ScheduledExecutorService se;
    private final ExecutorService es;

    public DefaultExecutor(int scheduledSize) {
        this.se = Executors.newScheduledThreadPool(scheduledSize);
        this.es = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return se.scheduleAtFixedRate(command,initialDelay,period,unit);
    }

    @Override
    public List<Runnable> shutdownNow() {
        var x = es.shutdownNow();
        x.addAll(se.shutdownNow());
        return x;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return es.submit(task);
    }
}
