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
&nbsp;&nbsp;4.1. [Data Item Operations](#41-data-item-operations)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.1. [Create](#411-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.2. [Read](#412-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.3. [Update](#413-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.1.4. [Delete](#414-delete)<br/>
&nbsp;&nbsp;4.2. [Path Item Operations](#42-path-item-operations)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.1. [Create](#421-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.2. [Read](#422-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.3. [Update](#423-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.4. [Delete](#424-delete)<br/>
5. [Development](#5-development)<br/>
&nbsp;&nbsp;5.1. [Build](#51-build)<br/>
&nbsp;&nbsp;5.2. [Test](#52-test)<br/>

# 1. Introduction

The storage driver extends the Mongoose's Abstract Coop Storage Driver and uses the following libraries:
* `pravega-client` version 0.3.2

# 2. Features

* Authentication: TBD
* SSL/TLS: TBD
* Item Types: `data`, `path`
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
| [Stream](http://pravega.io/docs/latest/pravega-concepts/#streams) | *Path Item* |
| Scope | *Storage Path* part (*stream name* is the 2nd part) |
| [Event](http://pravega.io/docs/latest/pravega-concepts/#events) | *Data Item* |
| Stream Segment | N/A |

**Note**:
> The Pravega storage driver should extend the NIO storage driver base.

## 4.1. Data Item Operations

Mongoose should perform the load operations on the *events* when the configuration option `item-type` is set to `data`.

### 4.1.1. Create

1. Check the corresponding scope if it exists and create it if not.
2. Check the corresponding stream if it exists and create it if not.
1. Get an `EventStreamWriter<ByteBuffer>` instance using the *item id* as a *stream name*.
2. Submit the next `ByteBuffer` event writing using a `writeEvent` method with a routing key either without it. Note
that it returns the `CompletableFuture` which should be handled properly.

### 4.1.2. Read

**Note**:
> Pravega storage doesn't support reading the events in the random order. So the `item-input-file` configuration option
> couldn't be used also. The only way to specify the items (events) to read is a scope + stream (`item-input-path` in
> Mongoose terms)

1. Get an `EventStreamReader<ByteBuffer>` instance
2. Read the next event using the method `readNextEvent` using some very small timeout (check if 0 is possible)

### 4.1.3. Update

Not supported. Stream append may be performed using `create` load operation type.

### 4.1.4. Delete

Not supported.

## 4.2. Path Item Operations

Mongoose should perform the load operations on the *streams* when the configuration option `item-type` is set to `path`.

### 4.2.1. Create

1. Check the corresponding scope if it exists and create it if not.
2. Invoke `StreamManager.createStream` using *item path* as a *scope name* and *item id* as a *stream name*. Fail the
load operation using the status code *14* if the method returns `false`.

### 4.2.2. Read

For each item read the whole corresponding stream. Unlike events, it's possible to read the streams in the random order
so both `item-input-path` (scope's streams listing) and `item-input-file` (stream ids list) options should be supported.

There is also another option, called `reader-timeout`. Pravega documentation says it only works when there is no available Event 
in the stream. `readNextEvent()` will block for the specified time in ms. So, in theory 0 and 1 should work just fine. 
They do not. In practice, this value should be somewhere between 100 and 2000 ms (2000 is Pravega default value).
### 4.2.3. Update

Not supported

### 4.2.4. Delete

A deletion of a path composed of a scope and a stream is implemented through `invokePathDelete()` method. Before the deletion, the stream must be sealed because of Pravega concepts. So the sealing of the stream is done in `invokePathDelete()` method too.

## 4.2. Open Issues

TODO

# 5. Development

## 5.1. Build

```bash
./gradlew clean jar
```

## 5.2. Test

```bash
./gradlew clean test
```
