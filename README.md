# postevent
A library to publish and receive events using postgres and grpc

## features
* Publish events based on the [cloudevents spec](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
  * Events are persisted in the source database
* Connect consumers either in-process or from a remote node (via grpc)
* Receive events in order (by `subject`)
* Receive all previous events on first connect
* Receive new events in real time 
