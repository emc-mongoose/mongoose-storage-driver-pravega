package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.base.item.op.OpType.NOOP;
import static com.emc.mongoose.base.item.op.Operation.SLASH;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_IO;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.MAX_BACKOFF_MILLIS;
import static com.emc.mongoose.storage.driver.pravega.io.StreamScaleUtil.scaleToFixedSegmentCount;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation.Status;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.logging.LogContextThreadFactory;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamReaderCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamWriterCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamWriterCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.EventStreamClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.EventStreamClientFactoryCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.EventWriterCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunctionForStreamConfig;
import com.emc.mongoose.storage.driver.pravega.cache.StreamCreateFunction;
import com.emc.mongoose.storage.driver.pravega.io.ByteBufferSerializer;
import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;
import com.emc.mongoose.storage.driver.pravega.io.StreamDataType;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.confuse.Config;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import lombok.val;
import org.apache.logging.log4j.Level;

public class PravegaStorageDriver<I extends DataItem, O extends DataOperation<I>>
    extends CoopStorageDriverBase<I, O> {

  protected final String uriSchema;
  protected final String scopeName;
  protected final String[] endpointAddrs;
  protected final int nodePort;
  protected final int controlApiTimeoutMillis;
  protected final boolean createRoutingKeys;
  protected final long createRoutingKeysPeriod;
  protected final int readTimeoutMillis;
  protected final Serializer<DataItem> evtSerializer = new DataItemSerializer(false);
  protected final Serializer<ByteBuffer> evtDeserializer = new ByteBufferSerializer();
  protected final EventWriterConfig evtWriterConfig = EventWriterConfig.builder().build();
  protected final ReaderConfig evtReaderConfig = ReaderConfig.builder().build();
  protected final ScalingPolicy scalingPolicy;
  protected final StreamDataType streamDataType;
  // round-robin counter to select the endpoint node for each load operation in order to distribute
  // them uniformly
  private final AtomicInteger rrc = new AtomicInteger(0);
  private final ScheduledExecutorService bgExecutor;

  private volatile boolean listWasCalled = false;

  @Value
  final class ScopeCreateFunctionImpl implements ScopeCreateFunction {
    Controller controller;

    @Override
    public final StreamCreateFunction apply(final String scopeName) {
      try {
        if (controller.createScope(scopeName).get(controlApiTimeoutMillis, MILLISECONDS)) {
          Loggers.MSG.trace("Scope \"{}\" was created", scopeName);
        } else {
          Loggers.MSG.info(
              "Scope \"{}\" was not created, may be already existing before", scopeName);
        }
      } catch (final InterruptedException e) {
        throwUnchecked(e);
      } catch (final Throwable cause) {
        LogUtil.exception(
            Level.WARN, cause, "{}: failed to create the scope \"{}\"", stepId, scopeName);
      }
      return new StreamCreateFunctionImpl(controller, scopeName);
    }
  }

  @Value
  final class ScopeCreateFunctionForStreamConfigImpl implements ScopeCreateFunctionForStreamConfig {

    Controller controller;

    @Override
    public final StreamConfiguration apply(final String scopeName) {
      final StreamConfiguration streamConfig =
          StreamConfiguration.builder().scalingPolicy(scalingPolicy).scope(scopeName).build();
      try {
        if (controller.createScope(scopeName).get(controlApiTimeoutMillis, MILLISECONDS)) {
          Loggers.MSG.trace("Scope \"{}\" was created", scopeName);
        } else {
          Loggers.MSG.info(
              "Scope \"{}\" was not created, may be already existing before", scopeName);
        }
      } catch (final InterruptedException e) {
        throwUnchecked(e);
      } catch (final Throwable cause) {
        LogUtil.exception(
            Level.WARN, cause, "{}: failed to create the scope \"{}\"", stepId, scopeName);
      }
      return streamConfig;
    }
  }

  @Value
  final class StreamCreateFunctionImpl implements StreamCreateFunction {

    Controller controller;
    String scopeName;

    @Override
    public final StreamConfiguration apply(final String streamName) {
      final StreamConfiguration streamConfig =
          StreamConfiguration.builder()
              .scalingPolicy(scalingPolicy)
              .build();
      try {
      	val createStreamFuture = controller.createStream(scopeName, streamName, streamConfig);
        if (createStreamFuture.get(controlApiTimeoutMillis, MILLISECONDS)) {
          Loggers.MSG.trace(
              "Stream \"{}/{}\" was created using the config: {}",
              scopeName,
              streamName,
              streamConfig);
        } else {
          scaleToFixedSegmentCount(
              controller, controlApiTimeoutMillis, scopeName, streamName, scalingPolicy);
        }
      } catch (final InterruptedException e) {
        throwUnchecked(e);
      } catch (final Throwable cause) {
        LogUtil.exception(
            Level.WARN, cause, "{}: failed to create the stream \"{}\"", stepId, streamName);
      }
      return streamConfig;
    }
  }

  @Value
  final class EventWriterCreateFunctionImpl implements EventWriterCreateFunction {

	  EventStreamClientFactory clientFactory;

    @Override
    public final EventStreamWriter<DataItem> apply(final String streamName) {
      return clientFactory.createEventWriter(streamName, evtSerializer, evtWriterConfig);
    }
  }

  @Value
  public final class ReaderCreateFunctionImpl implements ReaderCreateFunction {

	  EventStreamClientFactory clientFactory;

    @Override
    public EventStreamReader<ByteBuffer> apply(String readerGroup) {
      return clientFactory.createReader("reader", readerGroup, evtDeserializer, evtReaderConfig);
    }
  }

  // caches allowing the lazy creation of the necessary things:
  // * endpoints
  private final Map<String, URI> endpointCache = new ConcurrentHashMap<>();
	// * client configs
	private final Map<URI, ClientConfig> clientConfigCache = new ConcurrentHashMap<>();
	// * controllers
  private final Map<ClientConfig, Controller> controllerCache = new ConcurrentHashMap<>();
  // * scopes
  private final Map<Controller, ScopeCreateFunction> scopeCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, StreamCreateFunction> streamCreateFuncCache = new ConcurrentHashMap<>();
  // * streams
  private final Map<String, Map<String, StreamConfiguration>> scopeStreamsCache =
      new ConcurrentHashMap<>();
  // * client factories
  private final Map<ClientConfig, EventStreamClientFactoryCreateFunction> clientFactoryCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, EventStreamClientFactory> clientFactoryCache = new ConcurrentHashMap<>();
  // * event writers
  private final Map<EventStreamClientFactory, EventWriterCreateFunction> evtWriterCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, EventStreamWriter<DataItem>> evtWriterCache = new ConcurrentHashMap<>();
  // * reader group managers
  private final Map<URI, ReaderGroupManagerCreateFunction> readerGroupManagerCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, ReaderGroupManager> readerGroupManagerCache = new ConcurrentHashMap<>();
  // * event stream reader
  private final Map<EventStreamClientFactory, ReaderCreateFunction> eventStreamReaderCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, EventStreamReader<ByteBuffer>> eventStreamReaderCache =
      new ConcurrentHashMap<>();
  // * scopes with StreamConfigs
  private final Map<Controller, ScopeCreateFunctionForStreamConfig>
      scopeCreateFuncForStreamConfigCache = new ConcurrentHashMap<>();
  private final Map<String, StreamConfiguration> scopeStreamConfigsCache =
      new ConcurrentHashMap<>();

  public PravegaStorageDriver(
      final String stepId,
      final DataInput dataInput,
      final Config storageConfig,
      final boolean verifyFlag,
      final int batchSize)
      throws IllegalConfigurationException, IllegalArgumentException {
    super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
    val driverConfig = storageConfig.configVal("driver");
    this.controlApiTimeoutMillis = driverConfig.intVal("control-timeoutMillis");
    val scalingConfig = driverConfig.configVal("scaling");
    this.scalingPolicy = PravegaScalingConfig.scalingPolicy(scalingConfig);
    val netConfig = storageConfig.configVal("net");
    this.uriSchema = netConfig.stringVal("uri-schema");
    this.scopeName = storageConfig.stringVal("namespace");
    if (scopeName == null || scopeName.isEmpty()) {
      Loggers.ERR.warn("Scope name not set, use the \"storage-namespace\" configuration option");
    }
    val nodeConfig = netConfig.configVal("node");
    this.nodePort = nodeConfig.intVal("port");
    val endpointAddrList = nodeConfig.listVal("addrs");
    val eventConfig = driverConfig.configVal("event");
    val createRoutingKeysConfig = eventConfig.configVal("key");
    this.createRoutingKeys = createRoutingKeysConfig.boolVal("enabled");
    this.createRoutingKeysPeriod = createRoutingKeysConfig.longVal("count");
    this.readTimeoutMillis = eventConfig.intVal("timeoutMillis");
    this.streamDataType =
        StreamDataType.valueOf(driverConfig.stringVal("stream-data").toUpperCase());
    this.endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
    this.requestAuthTokenFunc = null; // do not use
    this.requestNewPathFunc = null; // do not use
    this.bgExecutor = Executors.newScheduledThreadPool(ioWorkerCount, new LogContextThreadFactory(toString(), true));
  }

  String nextEndpointAddr() {
    return endpointAddrs[rrc.getAndIncrement() % endpointAddrs.length];
  }

  URI createEndpointUri(final String nodeAddr) {
    try {
      final String addr;
      final int port;
      val portSepPos = nodeAddr.lastIndexOf(':');
      if (portSepPos > 0) {
        addr = nodeAddr.substring(0, portSepPos);
        port = Integer.parseInt(nodeAddr.substring(portSepPos + 1));
      } else {
        addr = nodeAddr;
        port = nodePort;
      }
      val uid = credential == null ? null : credential.getUid();
      return new URI(uriSchema, uid, addr, port, "/", null, null);
    } catch (final URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  ClientConfig createClientConfig(final URI endpointUri) {
  	return ClientConfig.builder().controllerURI(endpointUri).build();
  }

  @SuppressWarnings("UnstableApiUsage")
  Controller createController(final ClientConfig clientConfig) {
    val controllerConfig =
        ControllerImplConfig.builder()
            .clientConfig(clientConfig)
            .maxBackoffMillis(MAX_BACKOFF_MILLIS)
            .build();
    return new ControllerImpl(controllerConfig, bgExecutor);
  }

  /** Not used in this driver implementation */
  @Override
  protected String requestNewPath(final String path) {
    throw new AssertionError("Should not be invoked");
  }

  /** Not used in this driver implementation */
  @Override
  protected String requestNewAuthToken(final Credential credential) {
    throw new AssertionError("Should not be invoked");
  }

  @Override
  public List<I> list(
      final ItemFactory<I> itemFactory,
      final String path,
      final String prefix,
      final int idRadix,
      final I lastPrevItem,
      final int count)
      throws EOFException {

    if (listWasCalled) {
      throw new EOFException();
    }
    // this must be changed to the number of working load generator's threads
    val buff = new ArrayList<I>(1);
    buff.add(itemFactory.getItem(path + prefix, 0, 0));

    listWasCalled = true;
    return buff;
  }

  /** Not used in this driver implementation */
  @Override
  public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

  @Override
  protected boolean prepare(final O operation) {
    super.prepare(operation);
    var endpointAddr = operation.nodeAddr();
    if (endpointAddr == null) {
      endpointAddr = nextEndpointAddr();
      operation.nodeAddr(endpointAddr);
    }
    return true;
  }

  @Override
  protected final boolean submit(final O op) throws IllegalStateException {
    if (concurrencyThrottle.tryAcquire()) {
      val opType = op.type();
      if (NOOP.equals(opType)) {
        submitNoop(op);
      } else {
        final String nodeAddr = op.nodeAddr();
        switch (streamDataType) {
          case EVENTS:
            submitEventOperation(op, nodeAddr);
            break;
          case BYTES:
            submitByteStreamOperation(op, nodeAddr);
            break;
          default:
            throw new AssertionError("Unexpected stream data type: " + streamDataType);
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected final int submit(final List<O> ops, final int from, final int to)
      throws IllegalStateException {
    for (var i = from; i < to; i++) {
      if (!submit(ops.get(i))) {
        return i - from;
      }
    }
    return to - from;
  }

  @Override
  protected final int submit(final List<O> ops) throws IllegalStateException {
    val opsCount = ops.size();
    for (var i = 0; i < opsCount; i++) {
      if (!submit(ops.get(i))) {
        return i;
      }
    }
    return opsCount;
  }

  void submitNoop(final O op) {
    op.startRequest();
    completeOperation(op, SUCC);
  }

  void submitEventOperation(final O op, final String nodeAddr) {
    val type = op.type();
    switch (type) {
      case CREATE:
        submitEventCreateOperation(op, nodeAddr);
        break;
      case READ:
        submitEventReadOperation(op, nodeAddr);
        break;
      default:
        throw new AssertionError("Unsupported event operation type: " + type);
    }
  }

  void submitByteStreamOperation(final O op, final String nodeAddr) {
    val type = op.type();
    switch (type) {
      case CREATE:
        submitStreamCreateOperation(op, nodeAddr);
        break;
      case READ:
        submitStreamReadOperation(op, nodeAddr);
        break;
      case DELETE:
        submitStreamDeleteOperation(op, nodeAddr);
        break;
      default:
        throw new AssertionError("Unsupported byte stream operation type: " + type);
    }
  }

  void submitEventCreateOperation(final O evtOp, final String nodeAddr) {
    try {
      // prepare
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
      val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
      val scopeCreateFunc =
          scopeCreateFuncCache.computeIfAbsent(controller, ScopeCreateFunctionImpl::new);
      // create the scope if necessary
      val streamCreateFunc = streamCreateFuncCache.computeIfAbsent(scopeName, scopeCreateFunc);
      val streamName = extractStreamName(evtOp.dstPath());
      scopeStreamsCache
          .computeIfAbsent(scopeName, ScopeCreateFunction::createStreamCache)
          .computeIfAbsent(streamName, streamCreateFunc);
      // create the client factory create function if necessary
      val clientFactoryCreateFunc =
          clientFactoryCreateFuncCache.computeIfAbsent(
              clientConfig, EventStreamClientFactoryCreateFunctionImpl::new);
      // create the client factory if necessary
      val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
      // create the event stream writer create function if necessary
      val evtWriterCreateFunc =
          evtWriterCreateFuncCache.computeIfAbsent(
              clientFactory, EventWriterCreateFunctionImpl::new);
      // create the event stream writer if necessary
      val evtWriter = evtWriterCache.computeIfAbsent(streamName, evtWriterCreateFunc);
      val evtItem = evtOp.item();
      // submit the event writing
      final CompletionStage<Void> writeEvtFuture;
      if (createRoutingKeys) {
        val routingKey =
            Long.toString(
                createRoutingKeysPeriod > 0
                    ? evtItem.offset() % createRoutingKeysPeriod
                    : evtItem.offset(),
                Character.MAX_RADIX);
        evtOp.startRequest();
		  evtOp.finishRequest();
		  writeEvtFuture = evtWriter.writeEvent(routingKey, evtItem);
      } else {
        evtOp.startRequest();
        evtOp.finishRequest();
        writeEvtFuture = evtWriter.writeEvent(evtItem);
      }
      writeEvtFuture.handle(
          (returned, thrown) -> {
            if (null == thrown) {
				evtOp.startResponse();
				evtOp.finishResponse();
				try {
                evtOp.countBytesDone(evtItem.size());
              } catch (final IOException ignored) {
              }
              return completeOperation(evtOp, SUCC);
            } else {
              return completeFailedOperation(evtOp, thrown);
            }
          });
    } catch (final NullPointerException e) {
      if (!isStarted()) { // occurs on manual interruption which is normal so should be handled
        completeOperation(evtOp, INTERRUPTED);
      } else {
        completeFailedOperation(evtOp, e);
      }
    } catch (final Throwable thrown) {
      throwUncheckedIfInterrupted(thrown);
      completeFailedOperation(evtOp, thrown);
    }
  }

  void submitEventReadOperation(final O evtOp, final String nodeAddr) {
    try {
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val streamName = extractStreamName(evtOp.dstPath());
      scopeStreamsCache.computeIfAbsent(scopeName, ScopeCreateFunction::createStreamCache);
      val readerGroup = UUID.randomUUID().toString().replace("-", "");
      val readerGroupConfigBuilder = ReaderGroupConfig.builder();
      val stream = Stream.of(scopeName, streamName);
      val readerGroupConfig = readerGroupConfigBuilder.stream(stream).build();
      val readerGroupManagerCreateFunc =
          readerGroupManagerCreateFuncCache.computeIfAbsent(
              endpointUri, ReaderGroupManagerCreateFunctionImpl::new);
      val readerGroupManager =
          readerGroupManagerCache.computeIfAbsent(scopeName, readerGroupManagerCreateFunc);
      readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
      val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
      val clientFactoryCreateFunc =
          clientFactoryCreateFuncCache.computeIfAbsent(
              clientConfig, EventStreamClientFactoryCreateFunctionImpl::new);
      val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
      val readerCreateFunc =
          eventStreamReaderCreateFuncCache.computeIfAbsent(
              clientFactory, ReaderCreateFunctionImpl::new);
      val evtReader = eventStreamReaderCache.computeIfAbsent(readerGroup, readerCreateFunc);
      Loggers.MSG.trace("Reading all the events from {} {}", scopeName, streamName);
      val evt = evtReader.readNextEvent(readTimeoutMillis);
      val payload = evt.getEvent();
      val bytesDone = payload.remaining();
      val evtItem = evtOp.item();
      evtItem.size(bytesDone);
      evtOp.countBytesDone(evtItem.size());
      completeOperation(evtOp, SUCC);
    } catch (final Throwable thrown) {
      throwUncheckedIfInterrupted(thrown);
      completeFailedOperation(evtOp, thrown);
    }
  }

  void submitStreamCreateOperation(final O streamOp, final String nodeAddr) {
    try {
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
      val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
      val scopeCreateFuncForStreamConfig =
          scopeCreateFuncForStreamConfigCache.computeIfAbsent(
              controller, ScopeCreateFunctionForStreamConfigImpl::new);
      val streamConfig =
          scopeStreamConfigsCache.computeIfAbsent(scopeName, scopeCreateFuncForStreamConfig);
      val streamName = extractStreamName(streamOp.item().name());
      val createStreamFuture = controller.createStream(streamName, scopeName, streamConfig);
      createStreamFuture.handle(
          (result, thrown) -> {
            if (null == thrown) {
              LogUtil.exception(
                  Level.DEBUG,
                  thrown,
                  "Failed to create the stream {} in the scope {}",
                  streamName,
                  scopeName);
            }
            handleStreamCreate(endpointUri, streamName, streamOp);
            return result;
          });
    } catch (final NullPointerException e) {
      if (!isStarted()) {
        completeOperation(streamOp, INTERRUPTED);
      } else {
        completeFailedOperation(streamOp, e);
      }
    } catch (final Throwable thrown) {
      throwUncheckedIfInterrupted(thrown);
      completeFailedOperation(streamOp, thrown);
    }
  }

  void submitStreamReadOperation(final O streamOp, final String nodeAddr) {
    // TODO: Alex, issue SDP-51
  }

  void submitStreamDeleteOperation(final O streamOp, final String nodeAddr) {
    try {
      val streamName = extractStreamName(streamOp.item().name());
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
      val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
      val sealFuture = controller.sealStream(scopeName, streamName);
      sealFuture.handle(
          (result, thrown) -> {
            if (null != thrown) {
              LogUtil.exception(
                  Level.DEBUG,
                  thrown,
                  "Failed to seal the stream {} in the scope {}",
                  streamName,
                  scopeName);
            }
            if (!result) {
              Loggers.ERR.debug(
                  "Failed to seal the stream {} in the scope {}", streamName, scopeName);
            }
            return handleStreamSealBeforeDeletion(streamName, controller, streamOp);
          });
    } catch (final NullPointerException e) {
      if (!isStarted()) {
        completeOperation(streamOp, INTERRUPTED);
      } else {
        completeFailedOperation(streamOp, e);
      }
    } catch (final Throwable cause) {
      completeFailedOperation(streamOp, cause);
    }
  }

  boolean completeOperation(final O op, final Status status) {
    concurrencyThrottle.release();
    op.status(status);
    return handleCompleted(op);
  }

  boolean completeFailedOperation(final O op, final Throwable thrown) {
    LogUtil.exception(Level.DEBUG, thrown, "{}: unexpected load operation failure", stepId);
    return completeOperation(op, FAIL_UNKNOWN);
  }

  boolean handleStreamSealBeforeDeletion(
      final String streamName, final Controller controller, final O streamOp) {
  	streamOp.startRequest();
    val deleteFuture = controller.deleteStream(scopeName, streamName);
    streamOp.finishRequest();
    deleteFuture.handle(
        (result, thrown) -> {
          if (null == thrown) {
          	streamOp.startResponse();
          	streamOp.finishResponse();
            completeOperation(streamOp, SUCC);
          } else {
            completeFailedOperation(streamOp, thrown);
          }
          return result;
        });
    return true;
  }

  /**
   * @param endpointUri selected storage node URI
   * @param streamName stream name
   * @param streamOp stream operation instance
   */
  void handleStreamCreate(final URI endpointUri, final String streamName, final O streamOp) {
    var countBytesDone = 0L;
    val streamItem = streamOp.item();
    try {
      val streamSize = streamItem.size();
      if (streamSize > 0) {
        // TODO create the client factory create function if necessary
        // TODO create the client factory if necessary
        /*val byteStreamClient =
            byteStreamClientCache.computeIfAbsent(
                clientFactory, ClientFactory::createByteStreamClient);
        val byteStreamWriterCreateFunc =
            byteStreamWriterCreateFuncCache.computeIfAbsent(
                byteStreamClient, ByteStreamWriterCreateFunctionImpl::new);
        val byteStreamWriter =
            byteStreamWriterCache.computeIfAbsent(streamName, byteStreamWriterCreateFunc);
        var byteBuff = (ByteBuffer) null;
        var n = 0;
        while (countBytesDone < streamSize) {
          byteBuff = DirectMemUtil.getThreadLocalReusableBuff(streamSize - countBytesDone);
          n = streamItem.read(byteBuff);
          byteStreamWriter.write(byteBuff);
          countBytesDone += n;
        }*/
      }
      completeOperation(streamOp, SUCC);
    } catch (final IOException e) {
      LogUtil.exception(Level.DEBUG, e, "Failed to write the bytes stream {}", streamName);
      completeOperation(streamOp, FAIL_IO);
    } finally {
      streamOp.countBytesDone(countBytesDone);
      streamItem.size(countBytesDone);
    }
  }

  @Override
  protected void doClose() throws IOException {
    super.doClose();
    bgExecutor.shutdownNow();
    // clear all caches
    endpointCache.clear();
    clientConfigCache.clear();
    closeAllWithTimeout(controllerCache.values());
    controllerCache.clear();
    scopeCreateFuncCache.clear();
    streamCreateFuncCache.clear();
    scopeStreamsCache.values().forEach(Map::clear);
    scopeStreamsCache.clear();
    clientFactoryCreateFuncCache.clear();
    closeAllWithTimeout(clientFactoryCache.values());
    clientFactoryCache.clear();
    evtWriterCreateFuncCache.clear();
    closeAllWithTimeout(evtWriterCache.values());
    evtWriterCache.clear();
    readerGroupManagerCreateFuncCache.clear();
    closeAllWithTimeout(readerGroupManagerCache.values());
    readerGroupManagerCache.clear();
    closeAllWithTimeout(eventStreamReaderCreateFuncCache.keySet());
    eventStreamReaderCreateFuncCache.clear();
    closeAllWithTimeout(eventStreamReaderCache.values());
    eventStreamReaderCache.clear();
  }

  void closeAllWithTimeout(final Collection<? extends AutoCloseable> closeables) {
    if (null != closeables && closeables.size() > 0) {
      final ExecutorService closeExecutor = Executors.newFixedThreadPool(closeables.size());
      closeables.forEach(
          closeable ->
              closeExecutor.submit(
                  () -> {
                    try {
                      closeable.close();
                    } catch (final Exception e) {
                      LogUtil.exception(
                          Level.WARN,
                          e,
                          "{}: storage driver failed to close \"{}\"",
                          stepId,
                          closeable);
                    }
                  }));
      try {
        if (!closeExecutor.awaitTermination(controlApiTimeoutMillis, MILLISECONDS)) {
          Loggers.ERR.warn(
              "{}: storage driver timeout while closing one of \"{}\"",
              stepId,
              closeables.stream().findFirst().get().getClass().getSimpleName());
        }
      } catch (final InterruptedException e) {
        throwUnchecked(e);
      } finally {
        closeExecutor.shutdownNow();
      }
    }
  }

  static String extractStreamName(final String itemPath) {
    String result = itemPath;
    if (result.startsWith(SLASH)) {
      result = result.substring(1);
    }
    if (result.endsWith(SLASH) && result.length() > 1) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  @Override
  public String toString() {
    return String.format(super.toString(), DRIVER_NAME);
  }
}
