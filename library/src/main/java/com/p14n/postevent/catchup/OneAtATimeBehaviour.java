package com.p14n.postevent.catchup;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public interface OneAtATimeBehaviour {
    final AtomicInteger signals = new AtomicInteger(0);
    final AtomicBoolean running = new AtomicBoolean(false);

    public default void oneAtATime(Runnable todo, Runnable nextTodo) {
        signals.incrementAndGet();
        if (running.get()) {
            return;
        }
        synchronized (running) {
            if (!running.get()) {
                running.set(true);
                signals.set(0);
                todo.run();
                running.set(false);
                if (signals.get() > 0) {
                    nextTodo.run();
                }
            }
        }
    }

}
