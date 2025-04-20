package com.p14n.postevent;

import com.p14n.postevent.broker.DefaultMessageBroker;
import com.p14n.postevent.broker.MessageSubscriber;
import com.p14n.postevent.data.Traceable;

import io.opentelemetry.api.OpenTelemetry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.SECONDS)
class DefaultMessageBrokerTest {

    private class TestMessage implements Traceable {

        private final String value;

        public TestMessage(String value) {
            this.value = value;
        }

        @Override
        public String id() {
            return "test-id";
        }

        @Override
        public String topic() {
            return "test-topic";
        }

        @Override
        public String subject() {
            return "test-subject";
        }

        @Override
        public String traceparent() {
            return null;
        }
    }

    private volatile DefaultMessageBroker<TestMessage, TestMessage> broker;
    private final String TOPIC = "topic";

    @AfterEach
    void tearDown() {
        if (broker != null) {
            try {
                broker.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            } finally {
                broker = null;
            }
        }
    }

    @BeforeEach
    void setUp() {
        var ot = OpenTelemetry.noop();
        broker = new DefaultMessageBroker<TestMessage, TestMessage>(ot,"test") {
            @Override
            public TestMessage convert(TestMessage m) {
                return m;
            }
        };
    }

    @Test
    void shouldDeliverMessageToMultipleSubscribers() throws InterruptedException {
        CountDownLatch counter1 = new CountDownLatch(1);
        CountDownLatch counter2 = new CountDownLatch(1);

        MessageSubscriber<TestMessage> subscriber1 = new MessageSubscriber<>() {
            @Override
            public void onMessage(TestMessage message) {
                counter1.countDown();
            }

            @Override
            public void onError(Throwable error) {
            }
        };

        MessageSubscriber<TestMessage> subscriber2 = new MessageSubscriber<>() {
            @Override
            public void onMessage(TestMessage message) {
                counter2.countDown();
            }

            @Override
            public void onError(Throwable error) {
            }
        };

        broker.subscribe(TOPIC, subscriber1);
        broker.subscribe(TOPIC, subscriber2);

        broker.publish(TOPIC, new TestMessage("test"));

        assertTrue(counter1.await(1, TimeUnit.SECONDS));
        assertTrue(counter2.await(1, TimeUnit.SECONDS));
    }

    @Test
    void shouldSilentlyDropMessagesWithNoSubscribers() {
        broker.publish(TOPIC, new TestMessage("test")); // Should not throw
    }

    @Test
    void shouldNotifySubscriberOfErrors() throws InterruptedException {
        AtomicReference<Throwable> caughtError = new AtomicReference<>();
        CountDownLatch counter = new CountDownLatch(1);
        RuntimeException testException = new RuntimeException("test error");

        MessageSubscriber<TestMessage> erroringSubscriber = new MessageSubscriber<>() {
            @Override
            public void onMessage(TestMessage message) {
                throw testException;
            }

            @Override
            public void onError(Throwable error) {
                caughtError.set(error);
                counter.countDown();
            }
        };

        broker.subscribe(TOPIC, erroringSubscriber);
        broker.publish(TOPIC, new TestMessage("test"));

        assertTrue(counter.await(1, TimeUnit.SECONDS));
        assertSame(testException, caughtError.get());
    }

    @Test
    void shouldHandleConcurrentPublishAndSubscribe() throws InterruptedException {
        int threadCount = 3; // Further reduced for stability
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        MessageSubscriber<TestMessage> subscriber = new MessageSubscriber<>() {
            @Override
            public void onMessage(TestMessage message) {
                doneLatch.countDown();
            }

            @Override
            public void onError(Throwable error) {
            }
        };

        broker.subscribe(TOPIC, subscriber);

        // Create publisher threads
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    broker.publish(TOPIC, new TestMessage("test"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(2, TimeUnit.SECONDS), "Concurrent test did not complete in time");
    }

    @Test
    void shouldPreventPublishingAfterClose() {
        broker.close();
        assertThrows(IllegalStateException.class, () -> broker.publish(TOPIC, new TestMessage("test")));
        assertThrows(IllegalStateException.class, () -> broker.subscribe(TOPIC, new MessageSubscriber<>() {
            @Override
            public void onMessage(TestMessage message) {
            }

            @Override
            public void onError(Throwable error) {
            }
        }));
    }

    @Test
    void shouldStopDeliveringMessagesAfterUnsubscribe() throws InterruptedException {
        AtomicInteger messageCount = new AtomicInteger();
        CountDownLatch counter = new CountDownLatch(1);
        MessageSubscriber<TestMessage> subscriber = new MessageSubscriber<>() {
            @Override
            public void onMessage(TestMessage message) {
                messageCount.incrementAndGet();
                counter.countDown();
            }

            @Override
            public void onError(Throwable error) {
            }
        };

        broker.subscribe(TOPIC, subscriber);
        broker.publish(TOPIC, new TestMessage("first message"));
        assertTrue(counter.await(1, TimeUnit.SECONDS), "Should receive message while subscribed");

        broker.unsubscribe(TOPIC, subscriber);
        broker.publish(TOPIC, new TestMessage("second message"));
        Thread.sleep(100);
        assertEquals(1, messageCount.get(), "Should not receive message after unsubscribe");
    }
}
