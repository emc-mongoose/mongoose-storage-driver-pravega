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
4. [Design](#4-design)<br/>
5. [Development](#5-development)<br/>
&nbsp;&nbsp;5.1. [Build](#51-build)<br/>
&nbsp;&nbsp;5.2. [Test](#52-test)<br/>

# 1. Introduction

The storage driver extends the Mongoose's Abstract COOP Storage Driver and uses the following libraries:
* `pravega-client` version 0.3.2

# 2. Features

* Authentication: TBD
* SSL/TLS: TBD
* Item Types: `data`
* Supported load operations:
    * `create`
    * `read`
    * `update` (append only)
    * `delete`
* Storage-specific:
    * Stream sealing
    * Routing keys

# 3. Usage

## 3.1. Basic

Get the latest pre-built jar file which is available at:
http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-pravega/
The jar file may be downloaded manually and placed into the <USER_HOME_DIR>/.mongoose/<VERSION>/ext directory of
Mongoose to be automatically loaded into the runtime.

```bash
java -jar mongoose-<VERSION>.jar \
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

# 4. Design

Mongoose and Pravega are using different concepts. So it's necessary to determine how
[Pravega-specific terms](http://pravega.io/docs/latest/terminology/) are mapped to the
[Mongoose abstractions]((https://github.com/emc-mongoose/mongoose/tree/master/doc/design/architecture#1-basic-terms)).

| Pravega | Mongoose |
|---------|----------|
| [Stream](http://pravega.io/docs/latest/pravega-concepts/#streams) | *Data Item* |
| Stream Segment | N/A |
| Scope | *Storage Path* |
| [Event](http://pravega.io/docs/latest/pravega-concepts/#events) | *Data Item chunk* which may be transferred with a single *Load Operation* |

Mongoose generates a load executing the load operations on the *streams* which are considered as *items*. However, the
load operations rate is measured as ***events per second*** but not streams per second.

## 4.1. Load Operations

### 4.1.1. Create

1. Initialize a `ClientFactory` instance using the `storage-net-node-addrs` as a part of the *controller URI* and
    *item path* as a *scope name*.
2. Invoke `StreamManager.createStream` using *item path* as a *scope name* and *item id* as a *stream name*. Fail the
    load operation using the status code 14 if the method returns `false`.
3. Get an `EventStreamWriter<ByteBuffer>` instance using the *item id* as a *stream name*.
4. Submit the next `ByteBuffer` event writing using a `writeEvent` method with a routing key either without it. Note
    that it returns the `CompletableFuture` which should be handled somehow.
5. TODO

### 4.1.2. Read

1. See step #1 for `create`
2. Get an `EventStreamReader<ByteBuffer>` instance
3. Read the next event using the method `readNextEvent` using some very small timeout (check if 0 is possible)
4. TODO

### 4.1.3. Update

Update should work in the same way as `create` but it should fail only if the stream doesn't exist yet.

### 4.1.4. Delete

`StreamManager.deleteStream`

## 4.2. Open Issues

1. The configuration option `item-data-size` will specify the resulting stream size. How to specify the event size?
2. How to generate the different load operations using the same item (stream)?

# 5. Development

## 5.1. Build

```bash
./gradlew clean jar
```

## 5.2. Test

```bash
./gradlew clean test
```
