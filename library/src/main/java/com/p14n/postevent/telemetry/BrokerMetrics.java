package com.p14n.postevent.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

public class BrokerMetrics {
        private final LongCounter publishedMessages;
        private final LongCounter receivedMessages;
        private final LongUpDownCounter activeSubscribers;

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

        public void recordPublished(String topic) {
                publishedMessages.add(1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }

        public void recordReceived(String topic) {
                receivedMessages.add(1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }

        public void recordSubscriberAdded(String topic) {
                activeSubscribers.add(1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }

        public void recordSubscriberRemoved(String topic) {
                activeSubscribers.add(-1, Attributes.of(
                                AttributeKey.stringKey("topic"), topic));
        }
}