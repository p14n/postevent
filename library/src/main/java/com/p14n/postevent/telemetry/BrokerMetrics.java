package com.p14n.postevent.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * Manages OpenTelemetry metrics for message broker operations.
 * Tracks the number of published messages, received messages, and active
 * subscribers
 * across different topics.
 *
 * <p>
 * This class provides three main metrics:
 * </p>
 * <ul>
 * <li>messages_published: Counter for total messages published per topic</li>
 * <li>messages_received: Counter for total messages received by subscribers per
 * topic</li>
 * <li>active_subscribers: Up/down counter for current number of subscribers per
 * topic</li>
 * </ul>
 */
public class BrokerMetrics {
        private final LongCounter publishedMessages;
        private final LongCounter receivedMessages;
        private final LongUpDownCounter activeSubscribers;

        /**
         * Creates a new BrokerMetrics instance with the provided OpenTelemetry meter.
         *
         * @param meter OpenTelemetry meter used to create the metric instruments
         */
        public BrokerMetrics(Meter meter) {
                publishedMessages = meter.counterBuilder("messages_published")
                                .setDescription("Number of messages published")
                                .build();

                receivedMessages = meter.counterBuilder("messages_received")
                                .setDescription("Number of messages received by subscribers")
                                .build();

                activeSubscribers = meter.upDownCounterBuilder("active_subscribers")
                                .setDescription("Number of active subscribers")
                                .build();
        }

        /**
         * Records a message publication event for the specified topic.
         * Increments the published messages counter with the topic attribute.
         *
         * @param topic The topic the message was published to
         */
        public void recordPublished(String topic) {
                publishedMessages.add(1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }

        /**
         * Records a message reception event for the specified topic.
         * Increments the received messages counter with the topic attribute.
         *
         * @param topic The topic the message was received from
         */
        public void recordReceived(String topic) {
                receivedMessages.add(1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }

        /**
         * Records the addition of a subscriber for the specified topic.
         * Increments the active subscribers counter with the topic attribute.
         *
         * @param topic The topic the subscriber was added to
         */
        public void recordSubscriberAdded(String topic) {
                activeSubscribers.add(1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }

        /**
         * Records the removal of a subscriber for the specified topic.
         * Decrements the active subscribers counter with the topic attribute.
         *
         * @param topic The topic the subscriber was removed from
         */
        public void recordSubscriberRemoved(String topic) {
                activeSubscribers.add(-1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }
}
