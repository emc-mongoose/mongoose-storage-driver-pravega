[![Gitter chat](https://badges.gitter.im/emc-mongoose.png)](https://gitter.im/emc-mongoose)
[![Issue Tracker](https://img.shields.io/badge/Issue-Tracker-red.svg)](https://mongoose-issues.atlassian.net/projects/PRAVEGA)
[![CI status](https://travis-ci.org/emc-mongoose/mongoose-storage-driver-pravega.svg?branch=master)](https://travis-ci.org/emc-mongoose/mongoose-storage-driver-pravega/builds)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-pravega/maven-metadata.xml.svg)](http://central.maven.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-pravega)
[![Docker Pulls](https://img.shields.io/docker/pulls/emcmongoose/mongoose-storage-driver-pravega.svg)](https://hub.docker.com/r/emcmongoose/mongoose-storage-driver-pravega/)

# Content

1. [Introduction](#1-introduction)<br/>
2. [Features](#2-features)<br/>
3. [Deployment](#3-deployment)<br/>
&nbsp;&nbsp;3.1. [Basic](#31-basic)<br/>
&nbsp;&nbsp;3.2. [Docker](#32-docker)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.1. [Standalone](#321-standalone)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.2. [Distributed](#322-distributed)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.1. [Additional Node](#3221-additional-node)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.2. [Entry Node](#3222-entry-node)<br/>
4. [Configuration](#4-configuration)<br/>
&nbsp;&nbsp;4.1. [Specific Options](#41-specific-options)<br/>
&nbsp;&nbsp;4.2. [Tuning](#42-tuning)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.1. [Concurrency](#421-concurrency)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;4.2.2. [Base Storage Driver Usage Warnings](#422-base-storage-driver-usage-warnings)<br/>
5. [Usage](#5-usage)<br/>
&nbsp;&nbsp;5.1. [Event Stream Operations](#51-event-stream-operations)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.1.1. [Create](#511-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5.1.1.1. [Transactional](#5111-transactional)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.1.2. [Read](#512-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.1.3. [Update](#513-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.1.4. [Delete](#514-delete)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.1.5. [End-to-end Latency](#515-end-to-end-latency)<br/>
&nbsp;&nbsp;5.2. [Byte Stream Operations](#52-byte-stream-operations)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.2.1. [Create](#521-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.2.2. [Read](#522-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.2.3. [Update](#523-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.2.4. [Delete](#524-delete)<br/>
&nbsp;&nbsp;5.3. [Misc](#53-misc)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.3.1. [Manual Scaling](#531-manual-scaling)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;5.3.2. [Multiple Destination Streams](#532-multiple-destination-streams)<br/>
6. [Open Issues](#6-open-issues)<br/>
7. [Development](#7-development)<br/>
&nbsp;&nbsp;7.1. [Build](#71-build)<br/>
&nbsp;&nbsp;7.2. [Test](#72-test)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;7.2.1. [Automated](#721-automated)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;7.2.1.1. [Unit](#7211-unit)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;7.2.1.2. [Integration](#7212-integration)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;7.2.1.3. [Functional](#7213-functional)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;7.2.2. [Manual](#722-manual)<br/>

# 1. Introduction

Mongoose and Pravega are using quite different concepts. So it's necessary to determine how
[Pravega-specific terms](http://pravega.io/docs/latest/terminology/) are mapped to the
[Mongoose abstractions]((https://gitlab.com/emcmongoose/mongoose/tree/master/doc/design/architecture#1-basic-terms)).

| Pravega | Mongoose |
|---------|----------|
| [Stream](http://pravega.io/docs/latest/pravega-concepts/#streams) | *Item Path* or *Data Item* |
| Scope | Storage Namespace
| [Event](http://pravega.io/docs/latest/pravega-concepts/#events) | *Data Item* |
| Stream Segment | N/A |


# 2. Features

* Authentication: not implemented yet
* SSL/TLS: not implemented yet
* Item Types:
    * `data`: corresponds to an ***event*** either ***byte stream*** depending on the configuration 
    * `path`: not supported
    * `token`: not supported
* Supported load operations:
    * `create` (events, byte streams)
    * `read` (events, byte streams)
    * `delete` (streams)
* Storage-specific:
    * [Scaling policies](#515-manual-scaling)
    * Stream sealing
    * Routing keys
    * Byte streams
    * [Transactional events write](#5111-transactional) (batch mode)

# 3. Deployment

## 3.1. Basic

Java 11+ is required to build/run.

1. Get the latest `mongoose-base` jar from the 
[maven repo](http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-base/)
and put it to your working directory. Note the particular version, which is referred as *BASE_VERSION* below.

2. Get the latest `mongoose-storage-driver-preempt` jar from the
[maven repo](http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-preempt/)
and put it to the `~/.mongoose/<BASE_VERSION>/ext` directory.

3. Get the latest `mongoose-storage-driver-pravega` jar from the
[maven repo](http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-pravega/)
and put it to the `~/.mongoose/<BASE_VERSION>/ext` directory.

```bash
java -jar mongoose-base-<BASE_VERSION>.jar \
    --storage-driver-type=pravega \
    --storage-namespace=scope1 \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --storage-net-node-port=9090 \
    --load-batch-size=100 \
    --storage-driver-limit-queue-input=10000 \
    ...
```

## 3.2. Docker

### 3.2.1. Standalone

```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --storage-namespace=scope1 \
    --storage-net-node-addrs=<NODE_IP_ADDRS> \
    --load-batch-size=100 \
    --storage-driver-limit-queue-input=10000 \
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
    --storage-namespace=scope1 \
    --load-batch-size=100 \
    --storage-driver-limit-queue-input=10000 \
    ...
```

# 4. Configuration

## 4.1. Specific Options

| Name                              | Type            | Default Value | Description                                      |
|:----------------------------------|:----------------|:--------------|:-------------------------------------------------|
| storage-driver-control-timeoutMillis | integer      | 30000         | The timeout for any Pravega Controller API call
| storage-driver-event-key-enabled | boolean         | false         | Specifies if Mongoose should generate its own routing key during the events creation
| storage-driver-event-key-count   | integer         | 0             | Specifies a max count of unique routing keys to use during the events creation (may be considered as a routing key period). 0 value means to use unique routing key for each new event
| storage-driver-event-timeoutMillis | integer         | 100           | The event read timeout in milliseconds
| storage-driver-scaling-type       | enum | "fixed" | The scaling policy type to use (fixed/event_rate/kbyte_rate). [See the Pravega documentation](http://pravega.io/docs/latest/terminology/) for details
| storage-driver-scaling-rate       | integer         | 0             | The scaling policy target rate. May be meausred in events per second either kilobytes per second depending on the scaling policy type
| storage-driver-scaling-factor     | integer         | 0             | The scaling policy factor. From the Pravega javadoc: *the maximum number of splits of a segment for a scale-up event.*
| storage-driver-scaling-segments   | integer         | 1             | From the Pravega javadoc: *the minimum number of segments that a stream can have independent of the number of scale down events.*
| storage-driver-stream-data        | enum            | "events"      | Work on events or byte streams (if `bytes` is set)
| storage-net-node-addrs            | list of strings | 127.0.0.1     | The list of the Pravega storage nodes to use for the load
| storage-net-node-port             | integer         | 9090          | The default port of the Pravega storage nodes, should be explicitly set to 9090 (the value used by Pravega by default)

## 4.2. Tuning

### 4.2.1. Concurrency

There are two configuration options controlling the load operations concurrency level.

* `storage-driver-limit-concurrency`
Limits the count of the active load operations at any moment of the time. The best practice is to set it to 0 (unlimited
concurrency for the asynchronous operations, aka the *top gear of the "burst mode"*).

* `storage-driver-threads`
The count of the threads running/submitting the load operations execution. The meaningful values are usually only few
times more than the count of the available CPU threads.

### 4.2.2. Base Storage Driver Usage Warnings

See the [design notes](https://github.com/emc-mongoose/mongoose-storage-driver-preempt#design)

# 5. Usage

## 5.1. Event Stream Operations

Mongoose should perform the load operations on the *events* when the configuration option `item-type` is set to `data`.

### 5.1.1. Create

Steps:
1. Get the endpoint URI from the cache.
2. Check if the corresponding `StreamManager` exists using the cache, create a new one if it doesn't.
3. Check if the destination scope exists using the cache, create a new one if it doesn't.
4. Check if the destination stream exists using the cache, create a new one if it doesn't.
5. Check if the corresponding `ClientFactory` exists using the cache, create a new one if it doesn't.
6. Check if the `EventStreamWriter` exists using the cache, create new one if it doesn't.
7. Submit the event writing, use a routing key if configured.
8. Submit the load operation completion handler.

#### 5.1.1.1. Transactional

Using the [transactions](http://pravega.io/docs/latest/transactions/#pravega-transactions) to create the events allows
to write the events in the batch mode. The maximum count of the events per transaction is defined by the
`load-batch-size` configuration option value.

Example:
```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --storage-namespace=scope1 \
    --storage-driver-event-batch \
    --load-step-limit-count=100000 \
    --load-batch-size=1024 \
    --item-output-path=eventsStream1 \
    --item-data-size=10KB
```

### 5.1.2. Read

**Notes**:
> * The Pravega storage doesn't support reading the stream events in the random order.

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

### 5.1.3. Update

Not supported. A stream append may be performed using the `create` load operation type and a same stream previously used
to write the events.

### 5.1.4. Delete

Not supported.

### 5.1.5. End-to-end Latency

The end-to-end latency is a time span between the CREATE and READ operations executed for the same item. The end-to-end 
latency may be measured using Mongoose's 
[Pipeline Load extension](https://github.com/emc-mongoose/mongoose-load-step-pipeline) which is included into this 
extension's docker image. To do this, it's necessary to produce the raw operations trace data.

Scenario example:
<https://github.com/emc-mongoose/mongoose-storage-driver-pravega/blob/master/src/test/robot/api/storage/data/e2e_latency.js>

Command line example:
```bash
docker run \
    --network host \
    --volume "$(pwd)"/src/test/robot/api/storage/data:/root \
    --volume /tmp/log:/root/.mongoose/<BASE_VERSION>/log \
    emcmongoose/mongoose-storage-driver-pravega \
    --storage-namespace=scope1 \
    --item-output-path=stream1 \
    --run-scenario=/root/e2e_latency.js \
    --load-step-id=e2e_latency \
    --item-data-size=10KB \
    --load-op-limit-count=100000 \
    --output-metrics-trace-persist
```

Once the raw operations trace data is obtained, it may be used to produce the end-to-end latency data using the tool:
<https://github.com/emc-mongoose/e2e-latency-generator>

## 5.2. Byte Stream Operations

Mongoose should perform the load operations on the *streams* when the configuration option `storage-driver-stream-data`
is set to `bytes`. This means that the whole streams are being accounted as *items*.

### 5.2.1. Create

Creates the [byte streams](https://github.com/pravega/pravega/wiki/PDP-30-ByteStream-API). The created byte stream is
filled with content up to the size determined by the `item-data-size` option. The create operation will fail with the
[status code](https://github.com/emc-mongoose/mongoose-base/tree/master/doc/interfaces/output#232-files) #7 if the
stream existed before.

**Example**:
```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --storage-driver-stream-data=bytes \
    --storage-namespace=scope1 \
    --storage-driver-limit-concurrency=100 \
    --storage-driver-threads=100
```

### 5.2.2. Read

Reads the [byte streams](https://github.com/pravega/pravega/wiki/PDP-30-ByteStream-API).

**Example**:
```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --item-input-file=streams.csv \
    --read \
    --storage-driver-stream-data=bytes \
    --storage-driver-limit-concurrency=10 \
    --storage-driver-threads=10 \
    --storage-namespace=scope1
```

It's also possible to perform the byte streams read w/o the input stream items file:
```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --item-input-path=scope1 \
    --read \
    --storage-driver-stream-data=bytes \
    --storage-driver-limit-concurrency=10 \
    --storage-driver-threads=10 \
    --storage-namespace=scope1
```
All streams in the specified scope are listed and analyzed for the current size before the reading.

### 5.2.3. Update

Not implemented yet

### 5.2.4. Delete

Before the deletion, the stream must be sealed because of Pravega concepts. So the sealing of the stream is done during
the deletion too.

## 5.3. Misc

### 5.3.1. Manual Scaling

It's required to make a manual destination stream scaling while the event writing load is in progress in order to see
if the rate changes. The additional load step may be used to perform such scaling. In order to not perform any
additional load it should be explicitly configured to do a minimal work:
* load operations count limit: 1
* concurrency limit: 1
* payload size: 1 bytes

For more details see the corresponding [scenario content](https://github.com/emc-mongoose/mongoose-storage-driver-pravega/blob/master/src/main/resources/example/scenario/js/pravega/manual_scaling.js).

### 5.3.2. Multiple Destination Streams

The configuration [expression language](https://github.com/emc-mongoose/mongoose-base/tree/master/src/main/java/com/emc/mongoose/base/config/el#52-variable-items-output-path)
feature may be used to specify multiple destination streams to write the events. The example of the command to write
the events into 1000 destination streams (in the random order):

```bash
docker run \
    --network host \
    emcmongoose/mongoose-storage-driver-pravega \
    --storage-namespace=scope1 \
    --item-data-size=1000 \
    --item-output-path=stream-%p\{1000\;1\}
```

# 6. Open Issues

| Issue | Description |
|-------|-------------|

# 7. Development

## 7.1. Build

Note the Pravega commit # which should be used to build the corresponding Mongoose plugin.
Specify the required Pravega commit # in the `build.gradle` file. Then run:

```bash
./gradlew clean pravegaClientJars
./gradlew jar
```

## 7.2. Test

### 7.2.1. Automated

#### 7.2.1.1. Unit

```bash
./gradlew clean test
```

#### 7.2.1.2. Integration
```bash
docker run -d --name=storage --network=host pravega/pravega:<PRAVEGA_VERSION> standalone
./gradlew integrationTest
```

#### 7.2.1.3. Functional
```bash
./gradlew jar
export SUITE=api.storage
TEST=create_event_stream ./gradlew robotest
TEST=create_byte_streams ./gradlew robotest
TEST=read_byte_streams ./gradlew robotest
TEST=read_all_byte_streams ./gradlew robotest
TEST=create_event_transactional_stream ./gradlew robotest
```

### 7.2.1. Manual

1. [Build the storage driver](#71-build)
2. Copy the storage driver's jar file into the mongoose's `ext` directory:
```bash
cp -f build/libs/mongoose-storage-driver-pravega-*.jar ~/.mongoose/<MONGOOSE_BASE_VERSION>/ext/
```
Note that the Pravega storage driver depends on the 
[Coop Storage Driver](http://repo.maven.apache.org/maven2/com/github/emc-mongoose/mongoose-storage-driver-coop/) 
extension so it should be also put into the `ext` directory
3. Build and install the corresponding Pravega version:
```bash
./gradlew pravegaExtract
```
4. Run the Pravega standalone node:
```bash
build/pravega_/bin/pravega-standalone
```
4. Run Mongoose's default scenario with some specific command-line arguments:
```bash
java -jar mongoose-<MONGOOSE_BASE_VERSION>.jar \
    --storage-driver-type=pravega \
    --storage-net-node-port=9090 \
    --storage-driver-limit-concurrency=10 \
    --item-output-path=goose-events-stream-0
```
