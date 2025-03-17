# postevent
A library to publish and receive events using postgres and grpc

## features
* Publish events based on the [cloudevents spec](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
  * Events are persisted in the source database
* Connect consumers either in-process or from a remote node (via grpc)
* Receive events in order (by `subject`)
* Receive all previous events on first connect
* Receive new events in real time 

Create a local Event consumer
- [x] Publish a message to the event topic
- [x] Consume the message from the event topic
- [x] Ensure the message received is the same as the message published

Create a persistent Event consumer
- [X] Publish a message to the event topic
- [X] Message received is first stored in the database, then sent to the consumer

Create a catchup mechanism
* The PC/PR detects that it has gaps in the event sequence
   * As a new event is received, the CHWM is updated if the CHWM is idn-1
   * If the CHWM is not idn-1, the PC/PR will trigger the catchup mechanism instead of the processor
   * The catchup mechanism will request a batch of messages from the server from (CHWM+1) to the min idn greater than the CHWM
   * The catchup mechanism fills in the gap, looks for contiguous values up to the next gap and updates the CHWM.
   * The catchup mechanism looks for the next gap (CHWM+1 upwards) and repeats until there are no gaps
   * The catchup mechanism restarts the processor 
* Request a batch of messages from the server
* Write each message to the consumer
* Stop when the catchup mechanism detects that it is overwriting the live messages
- [x] Request a batch of messages from the server (starting from?)
- [x] Write each message to the consumer
- [x] Stop when the catchup mechanism detects that it is overwriting the live messages
- [x] Verifies that there are no gaps in the event sequence (check the sequence for the earliest unprocessed event until this one)

Create a processor
- [x] Verifies that there are no earlier unprocessed events for the same subject

- [ ] Hook up catchup mechanism
- [ ] Make server handle multiple topics
- [ ] Make threading mechanism pluggable
- [ ] DST tests

DB Debezium DONE
LC Local Consumer DONE
RC Remote Consumer 
PC Persistent Consumer DONE
PR Processor 
BF Business Function Ongoing
CC Catchup Client 
CS Catchup Server
CHWM contiguous high water mark

Local constant consumption
```mermaid
graph LR;
   Debezium-->PC;
   PC-->PR;
   PR-->BF;
```

Local constant consumption with catchup
```mermaid
graph LR;
   Debezium-->PC;
   PC-->PR;
   PR-->BF;
   PR-->CC;
   CC-->CS;
   CC-->PR;
   CS-->CC;
```

Remote constant consumption
```mermaid
graph LR;
   Debezium-->LC;
   LC-->RC;
   RC-->PC;
   PC-->PR;
   PR-->BF;
```


Remote constant consumption with catchup
```mermaid
graph LR;
   Debezium-->LC;
   LC-->RC;
   RC-->PC;
   PC-->PR;
   PR-->BF;
   PR-->CC;
   CC-->CS;
   CC-->PR;
   CS-->CC;
```
