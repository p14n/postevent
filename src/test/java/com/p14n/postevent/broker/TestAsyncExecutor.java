package com.p14n.postevent.broker;

import java.util.*;
import java.util.concurrent.*;

public class TestAsyncExecutor implements AsyncExecutor {
    private final List<Task> pendingTasks = new CopyOnWriteArrayList<>();
    private final List<Task> scheduledTasks = new CopyOnWriteArrayList<>();
    private boolean isShutdown = false;

    private static class Task {
        final Runnable runnable;
        final long initialDelay;
        final long period;
        final TimeUnit unit;
        final boolean isScheduled;
        final Callable<?> callable;
        final Future<?> future;

        Task(Runnable runnable, long initialDelay, long period, TimeUnit unit, ScheduledFuture<?> future) {
            this.runnable = runnable;
            this.initialDelay = initialDelay;
            this.period = period;
            this.unit = unit;
            this.isScheduled = true;
            this.callable = null;
            this.future = future;
        }

        Task(Callable<?> callable, Future<?> future) {
            this.runnable = null;
            this.initialDelay = 0;
            this.period = 0;
            this.unit = null;
            this.isScheduled = false;
            this.callable = callable;
            this.future = future;
        }

        Task(Runnable runnable, Future<?> future) {
            this.runnable = runnable;
            this.initialDelay = 0;
            this.period = 0;
            this.unit = null;
            this.isScheduled = false;
            this.callable = null;
            this.future = future;
        }
    }

    private static class CompletableFutureImpl<T> implements Future<T> {
        private boolean cancelled = false;
        private boolean done = false;
        private T result;
        private Exception exception;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled = true;
            done = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public T get() throws ExecutionException {
            if (exception != null) {
                throw new ExecutionException(exception);
            }
            return result;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws ExecutionException {
            return get();
        }

        public void complete(T value) {
            result = value;
            done = true;
        }

        public void completeExceptionally(Exception e) {
            exception = e;
            done = true;
        }
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        if (isShutdown) {
            throw new RejectedExecutionException("Executor is shutdown");
        }
        ScheduledFuture<?> future = new ScheduledFuture<Void>() {
            @Override
            public long getDelay(TimeUnit unit) {
                return 0;
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return scheduledTasks.removeIf(t -> t.future == this);
            }

            @Override
            public boolean isCancelled() {
                return scheduledTasks.stream().noneMatch(t -> t.future == this);
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public Void get() {
                return null;
            }

            @Override
            public Void get(long timeout, TimeUnit unit) {
                return null;
            }
        };
        Task task = new Task(command, initialDelay, period, unit, future);
        scheduledTasks.add(task);
        return future;
    }

    @Override
    public List<Runnable> shutdownNow() {
        isShutdown = true;
        List<Runnable> runnables = new ArrayList<>();
        for (Task task : pendingTasks) {
            if (task.runnable != null) {
                runnables.add(task.runnable);
            }
        }
        pendingTasks.clear();
        return runnables;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (isShutdown) {
            throw new RejectedExecutionException("Executor is shutdown");
        }
        CompletableFutureImpl<T> future = new CompletableFutureImpl<>();
        Task newTask = new Task(task, future);
        pendingTasks.add(newTask);
        return future;
    }

    @Override
    public void close() {
        shutdownNow();
    }

    @SuppressWarnings("unchecked")
    public void tick(Random random, boolean includeScheduled) {

        var handleScheduled = includeScheduled && !scheduledTasks.isEmpty();
        if (pendingTasks.isEmpty() && !handleScheduled) {
            return;
        }

        // Create a copy of tasks and shuffle them
        List<Task> tasksCopy = new ArrayList<>(pendingTasks);
        if (handleScheduled)
            tasksCopy.addAll(scheduledTasks);
        Collections.shuffle(tasksCopy, random);
        Task task = tasksCopy.getFirst();

        // Process the task
        try {
            if (task.callable != null) {
                CompletableFutureImpl<Object> future = (CompletableFutureImpl<Object>) task.future;
                try {
                    Object result = task.callable.call();
                    future.complete(result);
                    pendingTasks.remove(task);
                } catch (Exception e) {
                    e.printStackTrace();
                    future.completeExceptionally(e);
                    pendingTasks.remove(task);
                }
            } else if (task.runnable != null) {
                task.runnable.run();
                if (!task.isScheduled) {
                    pendingTasks.remove(task);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
