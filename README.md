[Mongoose](https://github.com/emc-mongoose/mongoose)'s storage driver for [Pravega](http://pravega.io) performance testing

# Content

1. [Introduction](#1-introduction)<br/>
2. [Features](#2-features)<br/>
3. [Usage](#3-usage)<br/>
&nbsp;&nbsp;3.1. [Basic](#31-basic)<br/>
&nbsp;&nbsp;3.2. [Docker](#32-docker)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.1. [Standalone](#321-standalone)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.2. [Distributed](#322-distributed)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.1. [Additional Node](#3221-additional-node)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.2. [Entry Node](#3222-entry-node)<br/>
&nbsp;&nbsp;3.3. [Specific Configuration Options](#33-specific-configuration-options)<br/>
4. [Design](#4-design)<br/>
&nbsp;&nbsp;4.1. [Event Operations](#41-event-operations)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.1. [Create](#411-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.2. [Read](#412-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.3. [Update](#413-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.4. [Delete](#414-delete)<br/>
&nbsp;&nbsp;4.2. [Stream Operations](#42-stream-operations)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.1. [Create](#421-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.2. [Read](#422-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.3. [Update](#423-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.4. [Delete](#424-delete)<br/>
5. [Development](#5-development)<br/>
&nbsp;&nbsp;5.1. [Build](#51-build)<br/>
&nbsp;&nbsp;5.2. [Test](#52-test)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.2.1 [Automated](#521-automated)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.2.2 [Manual](#522-manual)<br/>

# 1. Introduction

The storage driver extends the Mongoose's Abstract Coop Storage Driver and uses the following libraries:
* `pravega-client` version 0.3.2

# 2. Features

* Authentication: TBD
* SSL/TLS: TBD
* Item Types:
    * `data` -> "event"
    * `path` -> "stream"
* Supported load operations:
    * `create` (events)
    * `read` (streams)
    * `delete` (streams)
* Storage-specific:
    * Stream sealing
    * Routing keys

# 3. Usage

## 3.1. Basic

Get the latest pre-built jar file which is available at:
http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-pravega/
The jar file may be downloaded manually and placed into the <USER_HOME_DIR>/.mongoose/4.0.2/ext directory of
Mongoose to be automatically loaded into the runtime.

```bash
java -jar mongoose-4.0.2.jar \
    --storage-driver-type=pravega \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    ...
```

## 3.2. Docker

### 3.2.1. Standalone

```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    ...
```

### 3.2.2. Distributed

#### 3.2.2.1. Additional Node

```bash
docker run \
    --network host \
    --expose 1099 \
    emcmongoose/mongoose-storage-driver-pravega \
    --run-node
```

#### 3.2.2.2. Entry Node

```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --load-step-node-addrs=<ADDR1,ADDR2,...> \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=<NODE_PORT> \
    ...
```

## 3.3. Specific Configuration Options

| Name                              | Type            | Default Value | Description                                      |
|:----------------------------------|:----------------|:--------------|:-------------------------------------------------|
| storage-driver-create-key-enabled | boolean         | true          | Specifies if Mongoose should generate its own routing key during the events creation
| storage-driver-create-key-count   | integer         | 1             | Specifies a max count of unique routing keys to use during the events creation (may be considered as a routing key period). 0 value means to use unique routing key for each new event
| storage-driver-read-timeoutMillis | integer         | 100           | The event read timeout in milliseconds
| storage-net-node-addrs            | list of strings | 127.0.0.1     | The list of the Pravega storage nodes to use for the load
| storage-net-node-port             | integer         | 9020          | The default port of the Pravega storage nodes, should be explicitly set to 9090 (the value used by Pravega by default)

# 4. Design

Mongoose and Pravega are using quite different concepts. So it's necessary to determine how
[Pravega-specific terms](http://pravega.io/docs/latest/terminology/) are mapped to the
[Mongoose abstractions]((https://github.com/emc-mongoose/mongoose/tree/master/doc/design/architecture#1-basic-terms)).

| Pravega | Mongoose |
|---------|----------|
| [Stream](http://pravega.io/docs/latest/pravega-concepts/#streams) | *Path Item* |
| Scope | Storage Namespace
| [Event](http://pravega.io/docs/latest/pravega-concepts/#events) | *Data Item* |
| Stream Segment | N/A |

## 4.1. Event Operations

Mongoose should perform the load operations on the *events* when the configuration option `item-type` is set to `data`.

### 4.1.1. Create

Steps:
1. Get the endpoint URI from the cache.
2. Check if the corresponding `StreamManager` exists using the cache, create a new one if it doesn't.
3. Check if the destination scope exists using the cache, create a new one if it doesn't.
4. Check if the destination stream exists using the cache, create a new one if it doesn't.
5. Check if the corresponding `ClientFactory` exists using the cache, create a new one if it doesn't.
6. Check if the `EventStreamWriter` exists using the cache, create new one if it doesn't.
7. Submit the event writing, use a routing key if configured.
8. Submit the load operation completion handler.

### 4.1.2. Read

**Notes**:
> * The Pravega storage doesn't support reading the stream events in the random order.
> * Works synchronously

There is also another option, called `storage-driver-read-timeoutMillis`. Pravega documentation says it only works when
there is no available event in the stream. `readNextEvent()` will block for the specified time in ms. So, in theory 0
and 1 should work just fine. They do not. In practice, this value should be somewhere between 100 and 2000 ms (2000 is
Pravega default value).

Steps:
1. Get the endpoint URI from the cache.
2. Check if the corresponding `StreamManager` exists using the cache, create a new one if it doesn't.
3. Check if the corresponding `ClientFactory` exists using the cache, create a new one if it doesn't.
4. Check if the corresponding `EventStreamReader<ByteBuffer>` exists using the cache, create a new one if it doesn't.
5. Read the next event, verify the returned byte buffer content if configured so, discard it otherwise.
6. Invoke the load operation completion handler.

### 4.1.3. Update

Not supported. A stream append may be performed using the `create` load operation type and a same stream previously used
to write the events.

### 4.1.4. Delete

Not supported.

## 4.2. Stream Operations

Mongoose should perform the load operations on the *streams* when the configuration option `item-type` is set to `path`.

### 4.2.1. Create

**Notes**:
> * Just creates empty streams
> * Works synchronously

Steps:
1. Get the endpoint URI from the cache.
2. Check if the corresponding `StreamManager` exists using the cache, create a new one if it doesn't.
3. Check if the destination scope exists using the cache, create a new one if it doesn't.
4. Create the corresponding stream using the `StreamManager` instance, scope name, etc.
5. Invoke the load operation completion handler.

### 4.2.2. Read

Read the whole streams (all the corresponding events)

Steps:
1. Get the endpoint URI from the cache.
2. Check if the corresponding `StreamManager` exists using the cache, create a new one if it doesn't.
3. Check if the corresponding `ClientFactory` exists using the cache, create a new one if it doesn't.
4. Check if the corresponding `EventStreamReader<ByteBuffer>` exists using the cache, create a new one if it doesn't.
5. Read all events in the stream in the loop, discard the returned byte buffers content.
6. Invoke the load operation completion handler.

### 4.2.3. Update

Not supported

### 4.2.4. Delete

Before the deletion, the stream must be sealed because of Pravega concepts. So the sealing of the stream is done during
the deletion too.

## 4.2. Open Issues

TODO

# 5. Development

## 5.1. Build

```bash
./gradlew clean jar
```

## 5.2. Test

### 5.2.1. Automated

```bash
./gradlew clean test
```

### 5.2.1. Manual

1. Build the storage driver
2. Copy the storage driver's jar file into the mongoose's `ext` directory:
```bash
cp -f build/libs/mongoose-storage-driver-pravega.jar ~/.mongoose/4.0.2/ext/
```
3. Run the Pravega standalone node:
```bash
docker run --network host --expose 9090 pravega/pravega standalone
```
4. Run some Mongoose scenario:
```bash
java -jar mongoose-4.0.2.jar \
    --storage-driver-type=pravega \
    --storage-net-node-port=9090 \
    --item-data-size=1000 \
    --storage-driver-limit-concurrency=100 \
    --item-output-path=goose-events-stream-0
```
