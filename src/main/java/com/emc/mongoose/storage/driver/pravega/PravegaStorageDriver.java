package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.base.item.op.OpType.NOOP;
import static com.emc.mongoose.base.item.op.Operation.SLASH;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.base.item.op.Operation.Status.RESP_FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.BACKGROUND_THREAD_COUNT;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DEFAULT_SCOPE;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.MAX_BACKOFF_MILLIS;
import static com.emc.mongoose.storage.driver.pravega.io.StreamScaleUtil.scaleToFixedSegmentCount;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.Operation.Status;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.emc.mongoose.storage.driver.pravega.cache.ClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ClientFactoryCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.EventWriterCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunctionForStreamConfig;
import com.emc.mongoose.storage.driver.pravega.cache.StreamCreateFunction;

import com.emc.mongoose.storage.driver.pravega.io.ByteBufferSerializer;

import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;
import com.github.akurilov.confuse.Config;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.Value;
import lombok.val;
import org.apache.logging.log4j.Level;

public class PravegaStorageDriver<I extends Item, O extends Operation<I>>
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
  // round-robin counter to select the endpoint node for each load operation in order to distribute
  // them uniformly
  private final AtomicInteger rrc = new AtomicInteger(0);
  private final ScheduledExecutorService bgExecutor =
      Executors.newScheduledThreadPool(BACKGROUND_THREAD_COUNT);

  // should be an inner class in order to access the stream create function implementation
  // constructor
  @Value
  final class ScopeCreateFunctionImpl implements ScopeCreateFunction {

    Controller controller;

    @Override
    public final StreamCreateFunction apply(final String scopeName) {
      try {
        if (controller.createScope(scopeName).get(controlApiTimeoutMillis, TimeUnit.MILLISECONDS)) {
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

  // should be an inner class in order to access the storage driver instance stream config field
  @Value
  final class ScopeCreateFunctionForStreamConfigImpl implements ScopeCreateFunctionForStreamConfig {

    Controller controller;

    @Override
    public final StreamConfiguration apply(final String scopeName) {
      final StreamConfiguration streamConfig =
          StreamConfiguration.builder().scalingPolicy(scalingPolicy).scope(scopeName).build();
      try {
        if (controller.createScope(scopeName).get(controlApiTimeoutMillis, TimeUnit.MILLISECONDS)) {
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

  // should be an inner class in order to access the storage driver instance stream config field
  @Value
  final class StreamCreateFunctionImpl implements StreamCreateFunction {

    Controller controller;
    String scopeName;

    @Override
    public final StreamConfiguration apply(final String streamName) {
      final StreamConfiguration streamConfig =
          StreamConfiguration.builder()
              .scalingPolicy(scalingPolicy)
              .streamName(streamName)
              .scope(scopeName)
              .build();
      try {
        if (controller
            .createStream(streamConfig)
            .get(controlApiTimeoutMillis, TimeUnit.MILLISECONDS)) {
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

  // should be an inner class in order to access the storage driver instance's fields (serializer,
  // writer config)
  @Value
  final class EventWriterCreateFunctionImpl implements EventWriterCreateFunction {

    ClientFactory clientFactory;

    @Override
    public final EventStreamWriter<DataItem> apply(final String streamName) {
      return clientFactory.createEventWriter(streamName, evtSerializer, evtWriterConfig);
    }
  }

  // should be an inner class in order to access the storage driver instance's fields (serializer,
  // reader config)
  @Value
  public final class ReaderCreateFunctionImpl implements ReaderCreateFunction {

    ClientFactory clientFactory;

    @Override
    public EventStreamReader<ByteBuffer> apply(String readerGroup) {
      return clientFactory.createReader("reader", readerGroup, evtDeserializer, evtReaderConfig);
    }
  }


  // caches allowing the lazy creation of the necessary things:
  // * endpoints
  private final Map<String, URI> endpointCache = new ConcurrentHashMap<>();
  // * stream managers
  private final Map<URI, Controller> controllerCache = new ConcurrentHashMap<>();
  // * scopes
  private final Map<Controller, ScopeCreateFunction> scopeCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, StreamCreateFunction> streamCreateFuncCache = new ConcurrentHashMap<>();
  // * streams
  private final Map<String, Map<String, StreamConfiguration>> scopeStreamsCache =
      new ConcurrentHashMap<>();
  // * client factories
  private final Map<URI, ClientFactoryCreateFunction> clientFactoryCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, ClientFactory> clientFactoryCache = new ConcurrentHashMap<>();
  // * event writers
  private final Map<ClientFactory, EventWriterCreateFunction> evtWriterCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, EventStreamWriter<DataItem>> evtWriterCache = new ConcurrentHashMap<>();
  // * reader group managers
  private final Map<URI, ReaderGroupManagerCreateFunction> readerGroupManagerCreateFuncCache =
      new ConcurrentHashMap<>();
  private final Map<String, ReaderGroupManager> readerGroupManagerCache = new ConcurrentHashMap<>();
  // * event stream reader
  private final Map<ClientFactory, ReaderCreateFunction> eventStreamReaderCreateFuncCache =
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
    this.uriSchema = storageConfig.stringVal("net-uri-schema");
    this.scopeName = driverConfig.stringVal("namespace-scope");
    val nodeConfig = storageConfig.configVal("net-node");
    nodePort = storageConfig.intVal("net-node-port");
    val endpointAddrList = nodeConfig.listVal("addrs");
    val createRoutingKeysConfig = driverConfig.configVal("create-key");
    createRoutingKeys = createRoutingKeysConfig.boolVal("enabled");
    createRoutingKeysPeriod = createRoutingKeysConfig.longVal("count");
    readTimeoutMillis = driverConfig.intVal("read-timeoutMillis");
    endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
    requestAuthTokenFunc = null; // do not use
    requestNewPathFunc = null; // do not use
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

  @SuppressWarnings("UnstableApiUsage")
  Controller createController(final URI endpointUri) {
    val clientConfig = ClientConfig.builder().controllerURI(endpointUri).build();
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
      throws IOException {

    val buff = new ArrayList<I>(count);
    for (int i = 0; i < count; i++) {
      buff.add(itemFactory.getItem(path + prefix, 0, 0));
    }
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
        if (op instanceof DataOperation) {
          submitEventOperation((DataOperation) op, opType);
        } else if (op instanceof PathOperation) {
          submitStreamOperation((PathOperation) op, opType);
        } else {
          throw new AssertionError(
              DRIVER_NAME + " storage driver doesn't support the token operations");
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
  protected final int submit(final List<O> ops)
      throws IllegalStateException {
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

  boolean completeOperation(final O op, final Status status) {
    concurrencyThrottle.release();
    op.status(status);
    op.finishRequest();
    op.startResponse();
    op.finishResponse();
    return handleCompleted(op);
  }

  boolean completeFailedOperation(final O op, final Throwable thrown) {
    // LogUtil.exception(Level.WARN, thrown, "{}: unexpected load operation failure", stepId);
    thrown.printStackTrace();
    return completeOperation(op, FAIL_UNKNOWN);
  }


  boolean completeEventReadOperation(
      final DataOperation evtOp, final DataItem evtItem, final Throwable thrown) {
    if (null == thrown) {
      try {
        evtOp.countBytesDone(evtItem.size());
      } catch (final IOException ignored) {
      }
      return completeOperation((O) evtOp, SUCC);
    } else {
      return completeFailedOperation((O) evtOp, thrown);
    }
  }

  boolean completeStreamReadOperation(
      final PathOperation evtOp, final int bytesDone, final Throwable thrown) {
    if (null == thrown) {
      evtOp.countBytesDone(bytesDone);
      return completeOperation((O) evtOp, SUCC);
    } else {
      return completeFailedOperation((O) evtOp, thrown);
    }
  }

  boolean completeEventCreateOperation(
      final DataOperation evtOp, final DataItem evtItem, final Throwable thrown) {
    if (null == thrown) {
      try {
        evtOp.countBytesDone(evtItem.size());
      } catch (final IOException ignored) {
      }
      return completeOperation((O) evtOp, SUCC);
    } else {
      return completeFailedOperation((O) evtOp, thrown);
    }
  }

  void submitEventOperation(final DataOperation evtOp, final OpType opType) {
    final String nodeAddr = evtOp.nodeAddr();
    switch (opType) {
      case CREATE:
        submitEventCreateOperation(evtOp, nodeAddr);
        break;
      case READ:
        submitEventReadOperation(evtOp, nodeAddr);
        break;
      case UPDATE:
        throw new AssertionError("Not implemented");
      case DELETE:
        throw new AssertionError("Not implemented");
      case LIST:
        throw new AssertionError("Not implemented");
      default:
        throw new AssertionError("Not implemented");
    }
  }

  void submitEventCreateOperation(final DataOperation evtOp, final String nodeAddr) {
    try {
      // prepare
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val controller = controllerCache.computeIfAbsent(endpointUri, this::createController);
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
              endpointUri, ClientFactoryCreateFunctionImpl::new);
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
        writeEvtFuture = evtWriter.writeEvent(routingKey, evtItem);
      } else {
        writeEvtFuture = evtWriter.writeEvent(evtItem);
      }
      evtOp.startRequest();
      writeEvtFuture.handle(
          (returned, thrown) -> completeEventCreateOperation(evtOp, evtItem, thrown));
    } catch (final NullPointerException e) {
      if (!isStarted()) { // occurs on manual interruption which is normal so should be handled
        completeOperation((O) evtOp, INTERRUPTED);
      } else {
        completeFailedOperation((O) evtOp, e);
      }
    } catch (final Throwable thrown) {
    	if(thrown instanceof InterruptedException) {
    		throwUnchecked(thrown);
		}
      completeFailedOperation((O) evtOp, thrown);
    }
  }


  void submitEventReadOperation(final Operation Op, final String nodeAddr) {
    val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
    val scopeName = DEFAULT_SCOPE;
    val streamName =
        (Op instanceof DataOperation)
            ? extractStreamName(Op.dstPath())
            : // dataOp
            extractStreamName(Op.item().name()); // pathOp
    scopeStreamsCache.computeIfAbsent(scopeName, ScopeCreateFunction::createStreamCache);
    val readerGroup = UUID.randomUUID().toString().replace("-", "");
    val readerGroupConfig =
        ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
    val readerGroupManagerCreateFunc =
        readerGroupManagerCreateFuncCache.computeIfAbsent(
            endpointUri, ReaderGroupManagerCreateFunctionImpl::new);
    val readerGroupManager =
        readerGroupManagerCache.computeIfAbsent(scopeName, readerGroupManagerCreateFunc);

    readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
    val clientFactoryCreateFunc =
        clientFactoryCreateFuncCache.computeIfAbsent(
            endpointUri, ClientFactoryCreateFunctionImpl::new);
    @Cleanup
    val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
    val readerCreateFunc =
        eventStreamReaderCreateFuncCache.computeIfAbsent(
            clientFactory, ReaderCreateFunctionImpl::new);
    @Cleanup val evtReader = eventStreamReaderCache.computeIfAbsent(readerGroup, readerCreateFunc);

    final CompletionStage<Void> readEvtFuture;
    Loggers.MSG.trace("Reading all the events from {} {}", scopeName, streamName);
    /*readEvtFuture = ? */
    if (Op instanceof DataOperation) {
      readEvent((DataOperation) Op, evtReader); // dataOp
    } else {
      readStream((PathOperation) Op, evtReader); // pathOp
    }
    // readEvtFuture.handle((returned, thrown) ->completeEventReadOperation(Op, bytesDone, thrown);
    Loggers.MSG.trace("No more events from {} {}", scopeName, streamName);
  }

  void readEvent(DataOperation dataOp, EventStreamReader<ByteBuffer> evtReader) {
    EventRead<ByteBuffer> event = null;
    int bytesDone = 0;
    Exception thrown = null;
    try {
      event = evtReader.readNextEvent(readTimeoutMillis);
      event.getEvent();
      Loggers.MSG.trace("Read event {}", event.getEvent());
      bytesDone = event.getEvent().remaining();
      dataOp.item().size(bytesDone);

      completeEventReadOperation(dataOp, dataOp.item(), thrown);
    } catch (Exception e) { // including ReinitializationRequiredException
      thrown = e;
      completeEventReadOperation(dataOp, dataOp.item(), thrown);
    }
  }

  void readStream(PathOperation pathOp, EventStreamReader<ByteBuffer> evtReader) {
    EventRead<ByteBuffer> event = null;
    int bytesDone = 0;
    Exception thrown = null;
    try {
      for (event = evtReader.readNextEvent(readTimeoutMillis); null != event.getEvent(); ) {
        Loggers.MSG.trace("Read event {}", event.getEvent());
        bytesDone += event.getEvent().remaining();
      }
      // pathOp.countBytesDone(bytesDone);
      // we need to set the size of the item instead of the countBytesDone
      completeStreamReadOperation(pathOp, bytesDone, thrown);
    } catch (Exception e) { // including ReinitializationRequiredException
      //	thrown = e;
      /*pathOp.item() should be instead */
      completeStreamReadOperation(pathOp, bytesDone, thrown);
    }

  }

  void submitStreamOperation(final PathOperation streamOp, final OpType opType) {
    final String nodeAddr = streamOp.nodeAddr();
    switch (opType) {
      case CREATE:
        submitStreamCreateOperation(streamOp, nodeAddr);
        break;
      case READ:
        submitStreamReadOperation(streamOp, nodeAddr);
        break;
      case UPDATE:
        throw new AssertionError("Not implemented");
      case DELETE:
        submitStreamDeleteOperation(streamOp, nodeAddr);
        break;
      case LIST:
        throw new AssertionError("Not implemented");
      default:
        throw new AssertionError("Not implemented");
    }
  }

  boolean completeStreamCreateOperation(
      final PathOperation streamOp, final boolean result, final Throwable thrown) {
    // TODO erase code duplication
    val streamName = extractStreamName(streamOp.item().name());
    if (null == thrown) {
      if (result) {
        return completeOperation((O) streamOp, SUCC);
      } else {
        // Should I give stream and scope name to this method?
        Loggers.ERR.debug("The stream {} already exists in the scope {}", streamName, scopeName);
        return completeOperation((O) streamOp, RESP_FAIL_UNKNOWN);
      }
    } else {
      return completeFailedOperation((O) streamOp, thrown);
    }
  }

  void submitStreamCreateOperation(final PathOperation streamOp, final String nodeAddr) {
    try {
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val controller = controllerCache.computeIfAbsent(endpointUri, this::createController);
      val scopeCreateFuncForStreamConfig =
          scopeCreateFuncForStreamConfigCache.computeIfAbsent(
              controller, ScopeCreateFunctionForStreamConfigImpl::new);
      val streamConfig =
          scopeStreamConfigsCache.computeIfAbsent(scopeName, scopeCreateFuncForStreamConfig);
      val streamName = extractStreamName(streamOp.item().name());
      /*We need to use StreamConfiguration.builder() because scopeCreateFuncForStreamConfig returns
      StreamConfig without streamName field. So this looks like implementation of StreamManager.createStream()
       */
      val createStreamFuture =
          controller.createStream(combineStreamConfigAndName(streamName, streamConfig));
      createStreamFuture.handle(
          (returned, thrown) -> completeStreamCreateOperation(streamOp, returned, thrown));
    } catch (final NullPointerException e) {
		if(! isStarted()) {
			completeOperation((O) streamOp, INTERRUPTED);
		} else {
			completeFailedOperation((O) streamOp, e);
		}
	} catch (final Throwable thrown) {
    	if(thrown instanceof InterruptedException) {
    		throwUnchecked(thrown);
		}
      completeFailedOperation((O) streamOp, thrown);
    }
  }

  void submitStreamReadOperation(final PathOperation streamOp, final String nodeAddr) {
    // TODO: Alex, issue SDP-51
  }

  boolean completeStreamDeleteOperation(
      final PathOperation streamOp, final boolean result, final Throwable thrown) {
    // TODO erase code duplication
    val streamName = extractStreamName(streamOp.item().name());
    if (null == thrown) {
      if (result) {
        return completeOperation((O) streamOp, SUCC);
      } else {
        // Should I give stream and scope name to this method?
        Loggers.ERR.debug("Failed to delete the stream {} in the scope {}", streamName, scopeName);
        return completeOperation((O) streamOp, RESP_FAIL_UNKNOWN);
      }
    } else {
      return completeFailedOperation((O) streamOp, thrown);
    }
  }

  void submitStreamDeleteOperation(final PathOperation streamOp, final String nodeAddr) {
    try {
      val streamName = extractStreamName(streamOp.item().name());
      val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
      val controller = controllerCache.computeIfAbsent(endpointUri, this::createController);
      if (!controller
          .sealStream(scopeName, streamName)
          .get(controlApiTimeoutMillis, TimeUnit.MILLISECONDS)) {
        Loggers.ERR.debug("Failed to seal the stream {} in the scope {}", streamName, scopeName);
      }
      val deleteStreamFuture = controller.deleteStream(scopeName, streamName);
      deleteStreamFuture.handle(
          (returned, thrown) -> completeStreamDeleteOperation(streamOp, returned, thrown));
    } catch (final NullPointerException e) {
      if (!isStarted()) {
        completeOperation((O) streamOp, INTERRUPTED);
      } else {
        completeFailedOperation((O) streamOp, e);
      }
    } catch (final InterruptedException e) {
      throwUnchecked(e);
    } catch (final Throwable cause) {
      completeFailedOperation((O) streamOp, cause);
    }
  }

  @Override
  protected void doClose() throws IOException {
    super.doClose();
    bgExecutor.shutdownNow();
    // clear all caches
    endpointCache.clear();
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
        if (!closeExecutor.awaitTermination(controlApiTimeoutMillis, TimeUnit.MILLISECONDS)) {
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

  static StreamConfiguration combineStreamConfigAndName(
      final String streamName, final StreamConfiguration config) {
    return StreamConfiguration.builder()
        .scalingPolicy(config.getScalingPolicy())
        .streamName(streamName)
        .scope(config.getScope())
        .build();
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
