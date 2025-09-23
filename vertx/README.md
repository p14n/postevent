# Vert.x EventBus Integration

## Server implementation
EventBusMessageBroker - allows publishing to persistent topics, handles translation between vertx and postevent
VertxConsumerServer - sets up DDL for given topics and starts catchup for those topics

## Client implementation
VertxPersistentConsumer - consumes events from vertx eventbus.  Creates system event bus and catchup client, handles translation between vertx and postevent on the transactional consumer side.

Todo
 - [x] Implement autoclose on new classes
 - [ ] Adapt classes to use vertx threading model


