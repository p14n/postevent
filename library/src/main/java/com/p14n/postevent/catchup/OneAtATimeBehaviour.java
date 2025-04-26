package com.p14n.postevent.catchup;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public interface OneAtATimeBehaviour {

    public AtomicInteger getSignals();

    public AtomicBoolean getRunning();

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
