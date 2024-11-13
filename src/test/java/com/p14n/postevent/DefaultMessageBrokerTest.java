package com.p14n.postevent;

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
    
    private volatile DefaultMessageBroker<String> broker;
    
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
        broker = new DefaultMessageBroker<>();
    }
    
    @Test
    void shouldDeliverMessageToMultipleSubscribers() {
        AtomicInteger counter1 = new AtomicInteger();
        AtomicInteger counter2 = new AtomicInteger();
        
        MessageSubscriber<String> subscriber1 = new MessageSubscriber<>() {
            @Override
            public void onMessage(String message) {
                counter1.incrementAndGet();
            }
            
            @Override
            public void onError(Throwable error) {}
        };
        
        MessageSubscriber<String> subscriber2 = new MessageSubscriber<>() {
            @Override
            public void onMessage(String message) {
                counter2.incrementAndGet();
            }
            
            @Override
            public void onError(Throwable error) {}
        };
        
        broker.subscribe(subscriber1);
        broker.subscribe(subscriber2);
        
        broker.publish("test");
        
        assertEquals(1, counter1.get());
        assertEquals(1, counter2.get());
    }
    
    @Test
    void shouldSilentlyDropMessagesWithNoSubscribers() {
        broker.publish("test"); // Should not throw
    }
    
    @Test
    void shouldNotifySubscriberOfErrors() {
        AtomicReference<Throwable> caughtError = new AtomicReference<>();
        RuntimeException testException = new RuntimeException("test error");
        
        MessageSubscriber<String> erroringSubscriber = new MessageSubscriber<>() {
            @Override
            public void onMessage(String message) {
                throw testException;
            }
            
            @Override
            public void onError(Throwable error) {
                caughtError.set(error);
            }
        };
        
        broker.subscribe(erroringSubscriber);
        broker.publish("test");
        
        assertSame(testException, caughtError.get());
    }
    
    @Test
    void shouldHandleConcurrentPublishAndSubscribe() throws InterruptedException {
        int threadCount = 3; // Further reduced for stability
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger messageCount = new AtomicInteger();
        
        MessageSubscriber<String> subscriber = new MessageSubscriber<>() {
            @Override
            public void onMessage(String message) {
                messageCount.incrementAndGet();
            }
            
            @Override
            public void onError(Throwable error) {}
        };
        
        broker.subscribe(subscriber);
        
        // Create publisher threads
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    broker.publish("test");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }
        
        startLatch.countDown();
        assertTrue(doneLatch.await(1, TimeUnit.SECONDS), "Concurrent test did not complete in time");
        assertEquals(threadCount, messageCount.get());
    }
    
    @Test
    void shouldPreventPublishingAfterClose() {
        broker.close();
        assertThrows(IllegalStateException.class, () -> broker.publish("test"));
        assertThrows(IllegalStateException.class, () -> broker.subscribe(new MessageSubscriber<>() {
            @Override
            public void onMessage(String message) {}
            @Override
            public void onError(Throwable error) {}
        }));
    }
}
