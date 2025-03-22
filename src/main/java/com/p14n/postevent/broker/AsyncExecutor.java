package com.p14n.postevent.broker;

import java.util.List;
import java.util.concurrent.*;

public interface AsyncExecutor {

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                  long initialDelay,
                                                  long period,
                                                  TimeUnit unit);

    List<Runnable> shutdownNow();

    <T> Future<T> submit(Callable<T> task);

}
