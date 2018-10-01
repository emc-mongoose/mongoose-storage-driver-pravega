[Mongoose](https://github.com/emc-mongoose/mongoose)'s storage driver for [Pravega](http://pravega.io) performance testing

# Content

1. Introduction<br/>
2. Features<br/>
3. Usage<br/>
&nbsp;&nbsp;3.1. Basic<br/>
&nbsp;&nbsp;3.2. Docker<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.1. Standalone<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.2. Distributed<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.1. Additional Node<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.2. Entry Node<br/>
4. Design<br/>
5. Development<br/>
&nbsp;&nbsp;5.1. Build<br/>
&nbsp;&nbsp;5.2. Test<br/>

# 1. Introduction

The storage driver extends the Mongoose's Abstract NIO Storage Driver and uses the following libraries:
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

# 5. Development

## 5.1. Build

```bash
./gradlew clean jar
```

## 5.2. Test

```bash
./gradlew clean test
```