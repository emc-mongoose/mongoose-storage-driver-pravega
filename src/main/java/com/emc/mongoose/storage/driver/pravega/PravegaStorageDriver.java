package com.emc.mongoose.storage.driver.pravega;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.base.item.op.OpType.NOOP;
import static com.emc.mongoose.base.item.op.Operation.SLASH;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_IO;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_TIMEOUT;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.INTERRUPTED;
import static com.emc.mongoose.base.item.op.Operation.Status.PENDING;
import static com.emc.mongoose.base.item.op.Operation.Status.RESP_FAIL_UNKNOWN;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.DRIVER_NAME;
import static com.emc.mongoose.storage.driver.pravega.PravegaConstants.MAX_BACKOFF_MILLIS;
import static com.emc.mongoose.storage.driver.pravega.io.StreamDataType.BYTES;
import static com.emc.mongoose.storage.driver.pravega.io.StreamDataType.EVENTS;
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
import com.emc.mongoose.storage.driver.pravega.cache.ByteStreamClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.EventStreamClientFactoryCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.EventStreamClientFactoryCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ReaderGroupManagerCreateFunctionImpl;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunction;
import com.emc.mongoose.storage.driver.pravega.cache.ScopeCreateFunctionForStreamConfig;
import com.emc.mongoose.storage.driver.pravega.cache.StreamCreateFunction;
import com.emc.mongoose.storage.driver.pravega.io.ByteBufferSerializer;
import com.emc.mongoose.storage.driver.pravega.io.ByteStreamWriteChannel;
import com.emc.mongoose.storage.driver.pravega.io.DataItemSerializer;
import com.emc.mongoose.storage.driver.pravega.io.StreamDataType;
import com.emc.mongoose.storage.driver.preempt.PreemptStorageDriverBase;
import com.github.akurilov.commons.concurrent.ContextAwareThreadFactory;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.confuse.Config;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.netty.impl.ConnectionPoolImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
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
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.pravega.client.stream.impl.Credentials;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.common.util.AsyncIterator;
import lombok.Value;
import lombok.val;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

public class PravegaStorageDriver<I extends DataItem, O extends DataOperation<I>>
				extends PreemptStorageDriverBase<I, O> {

	protected final Semaphore concurrencyThrottle;
	protected final String uriSchema;
	protected final String scopeName;
	protected final String[] endpointAddrs;
	protected final int nodePort;
	protected final int maxConnectionsPerSegmentstore;
	protected final long controlApiTimeoutMillis;
	protected final boolean transactionMode;
	protected final long evtOpTimeoutMillis;
	protected final Serializer<I> evtSerializer = new DataItemSerializer<>(false);
	protected final Serializer<ByteBuffer> evtDeserializer = new ByteBufferSerializer();
	protected final EventWriterConfig evtWriterConfig;
	protected final ReaderConfig evtReaderConfig = ReaderConfig
					.builder()
					.initialAllocationDelay(0)
					.build();
	protected final ThreadLocal<ReaderGroupConfig.ReaderGroupConfigBuilder> evtReaderGroupConfigBuilder = ThreadLocal.withInitial(ReaderGroupConfig::builder);
	protected final ScalingPolicy scalingPolicy;
	protected final StreamDataType streamDataType;
	// round-robin counter to select the endpoint node for each load operation in order to distribute them uniformly
	private final AtomicInteger rrc = new AtomicInteger(0);
	private final ScheduledExecutorService bgExecutor;
	private final RoutingKeyFunction<I> routingKeyFunc;
	private volatile AsyncIterator<Stream> streamIterator = null;
	private final boolean controlScopeFlag;
	private final boolean controlStreamFlag;
	private final Credentials cred;

	Queue<EventStreamReader<ByteBuffer>> createEventStreamReaderPool(final String unused) {
		return new ArrayBlockingQueue<>(ioWorkerCount);
	}

	@Value
	final class ScopeCreateFunctionImpl
					implements ScopeCreateFunction {

		Controller controller;

		@Override
		public final StreamCreateFunction apply(final String scopeName) {
			if(controlScopeFlag) {
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
						Level.WARN, cause, "{}: failed to create the scope \"{}\"", stepId, scopeName
					);
				}
			}
			return new StreamCreateFunctionImpl(controller, scopeName);
		}
	}

	@Value
	final class ScopeCreateFunctionForStreamConfigImpl
					implements ScopeCreateFunctionForStreamConfig {

		Controller controller;

		@Override
		public final StreamConfiguration apply(final String scopeName) {
			final StreamConfiguration streamConfig = StreamConfiguration
				.builder()
				.scalingPolicy(scalingPolicy)
				.scope(scopeName)
				.build();
			if(controlScopeFlag) {
				try {
					if (controller.createScope(scopeName).get(controlApiTimeoutMillis, MILLISECONDS)) {
						Loggers.MSG.trace("Scope \"{}\" was created", scopeName);
					} else {
						Loggers.MSG.info("Scope \"{}\" was not created, may be already existing before", scopeName);
					}
				} catch (final InterruptedException e) {
					throwUnchecked(e);
				} catch (final Throwable cause) {
					LogUtil.exception(
						Level.WARN, cause, "{}: failed to create the scope \"{}\"", stepId, scopeName
					);
				}
			}
			return streamConfig;
		}
	}

	@Value
	final class StreamCreateFunctionImpl
					implements StreamCreateFunction {

		Controller controller;
		String scopeName;

		@Override
		public final StreamConfiguration apply(final String streamName) {
			final StreamConfiguration streamConfig = StreamConfiguration.builder()
							.scalingPolicy(scalingPolicy)
							.build();
			if(controlStreamFlag) {
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
			}
			return streamConfig;
		}
	}

	@Value
	final class ByteStreamClientFactoryCreateFunctionImpl
					implements ByteStreamClientFactoryCreateFunction {

		ConnectionFactory connFactory;

		@Override
		public ByteStreamClientFactory apply(final Controller controller) {
			return new ByteStreamClientImpl(scopeName, controller, connFactory);
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
	private final Map<Controller, ScopeCreateFunction> scopeCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, StreamCreateFunction> streamCreateFuncCache = new ConcurrentHashMap<>();
	// * streams
	private final Map<String, Map<String, StreamConfiguration>> scopeStreamsCache = new ConcurrentHashMap<>();
	// * event stream client factories
	private final Map<ClientConfig, EventStreamClientFactoryCreateFunction> clientFactoryCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<String, EventStreamClientFactory> clientFactoryCache = new ConcurrentHashMap<>();
	// * event stream writers
	private final ThreadLocal<Map<String, EventStreamWriter<I>>> threadLocalEvtWriterCache = ThreadLocal.withInitial(ConcurrentHashMap::new);
	// * reader group
	private final Map<String, ReaderGroupConfig> evtReaderGroupConfigCache = new ConcurrentHashMap<>();
	private final Map<ClientConfig, ReaderGroupManagerCreateFunction> evtReaderGroupManagerCreateFuncCache = new ConcurrentHashMap<>();
	// * RGManager per scope
	//todo: It doesn't make any sense as we can only have one scope so far.
	private final Map<String, ReaderGroupManager> evtReaderGroupManagerCache = new ConcurrentHashMap<>();
	// * RGName per stream per scope
	private final Map<String, Map<String, String>> evtReaderGroupCache = new ConcurrentHashMap<>();
	// * event stream reader
	private final Map<EventStreamClientFactory, ReaderCreateFunction> evtStreamReaderCreateFuncCache = new ConcurrentHashMap<>();
	// * pool of readers for each stream
	private final Map<String, Queue<EventStreamReader<ByteBuffer>>> evtStreamReaderPoolCache = new ConcurrentHashMap<>();
	// * scopes with StreamConfigs
	private final Map<Controller, ScopeCreateFunctionForStreamConfig> scopeCreateFuncForStreamConfigCache = new ConcurrentHashMap<>();
	private final Map<String, StreamConfiguration> scopeStreamConfigsCache = new ConcurrentHashMap<>();
	// * connection factory cache
	private final Map<ClientConfig, ConnectionFactory> connFactoryCache = new ConcurrentHashMap<>();
	// * byte stream client cache
	private final Map<ConnectionFactory, ByteStreamClientFactoryCreateFunction> byteStreamClientCreateFuncCache = new ConcurrentHashMap<>();
	private final Map<Controller, ByteStreamClientFactory> byteStreamClientFactoryCache = new ConcurrentHashMap<>();
	// * byte stream reader cache
	private final ThreadLocal<Map<String, ByteStreamReader>> threadLocalByteStreamReaderCache = ThreadLocal.withInitial(ConcurrentHashMap::new);
	// * batch event writers
	private final ThreadLocal<Map<EventStreamClientFactory, TransactionalEventStreamWriter<I>>> threadLocalTxnEvtWriterCache = ThreadLocal.withInitial(ConcurrentHashMap::new);

	public PravegaStorageDriver(
		final String stepId,
		final DataInput dataInput,
		final Config storageConfig,
		final boolean verifyFlag,
		final int batchSize
	) throws IllegalConfigurationException, IllegalArgumentException {
		this(
			stepId,
			dataInput,
			storageConfig,
			verifyFlag,
			batchSize,
			StreamDataType.valueOf(storageConfig.stringVal("driver-stream-data").toUpperCase())
		);
	}

	PravegaStorageDriver(
		final String stepId,
		final DataInput dataInput,
		final Config storageConfig,
		final boolean verifyFlag,
		final int batchSize,
		final StreamDataType streamDataType
	) throws IllegalConfigurationException, IllegalArgumentException {
		super(stepId, dataInput, storageConfig, verifyFlag, BYTES.equals(streamDataType) ? 1 : batchSize);
		this.concurrencyThrottle = new Semaphore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE, true);
		val driverConfig = storageConfig.configVal("driver");
		val controlConfig = driverConfig.configVal("control");
		this.controlApiTimeoutMillis = controlConfig.longVal("timeoutMillis");
		this.controlScopeFlag = controlConfig.boolVal("scope");
		this.controlStreamFlag = controlConfig.boolVal("stream");
		val scalingConfig = driverConfig.configVal("scaling");
		this.scalingPolicy = PravegaScalingConfig.scalingPolicy(scalingConfig);
		val netConfig = storageConfig.configVal("net");
		this.uriSchema = netConfig.stringVal("uri-schema");
		this.scopeName = storageConfig.stringVal("namespace");
		this.maxConnectionsPerSegmentstore = netConfig.intVal("maxConnPerSegmentstore");
		if (scopeName == null || scopeName.isEmpty()) {
			Loggers.ERR.warn("Scope name not set, use the \"storage-namespace\" configuration option");
		}
		val nodeConfig = netConfig.configVal("node");
		this.nodePort = nodeConfig.intVal("port");
		val endpointAddrList = nodeConfig.listVal("addrs");
		val eventConfig = driverConfig.configVal("event");
		val createRoutingKeysConfig = eventConfig.configVal("key");
		val createRoutingKeys = createRoutingKeysConfig.boolVal("enabled");
		val createRoutingKeysPeriod = createRoutingKeysConfig.longVal("count");
		this.routingKeyFunc = createRoutingKeys ? new RoutingKeyFunctionImpl<>(createRoutingKeysPeriod) : null;
		this.evtOpTimeoutMillis = eventConfig.longVal("timeoutMillis");
		this.streamDataType = StreamDataType.valueOf(driverConfig.stringVal("stream-data").toUpperCase());
		if (EVENTS.equals(streamDataType)) {
			this.transactionMode = eventConfig.boolVal("transaction");
		} else {
			this.transactionMode = false;
		}
		this.endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
		this.requestAuthTokenFunc = null; // do not use
		this.requestNewPathFunc = null; // do not use
		this.bgExecutor = Executors.newScheduledThreadPool(
						Runtime.getRuntime().availableProcessors(),
						new LogContextThreadFactory(toString(), true));
		cred = ServiceLoader
			.load(Credentials.class, Credentials.class.getClassLoader())
			.findFirst()
			.orElseGet(this::getBasicCredentialsIfAny);
		Loggers.MSG.info("Authentication type: {}", cred == null ? "none" : cred.getAuthenticationType());
		val connConfig = nodeConfig.configVal("conn");
		val poolingFlag = connConfig.boolVal("pooling");
		evtWriterConfig = EventWriterConfig
				.builder()
				.enableConnectionPooling(poolingFlag)
				.build();
	}

	Credentials getBasicCredentialsIfAny() {
		if(credential == null || Credential.NONE.equals(credential)) {
			return null;
		} else {
			return new DefaultCredentials(credential.getSecret(), credential.getUid());
		}
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
		val maxConnPerSegStore = maxConnectionsPerSegmentstore;
		val clientConfigBuilder = ClientConfig
			.builder()
			.controllerURI(endpointUri)
			.maxConnectionsPerSegmentStore(maxConnPerSegStore);
		if(null != cred) {
			clientConfigBuilder.credentials(cred);
		}
		return clientConfigBuilder.build();
	}

	Controller createController(final ClientConfig clientConfig) {
		val controllerConfig = ControllerImplConfig
			.builder()
			.clientConfig(clientConfig)
			.maxBackoffMillis(MAX_BACKOFF_MILLIS)
			.build();
		return new ControllerImpl(controllerConfig, bgExecutor);
	}

	ConnectionFactory createConnectionFactory(final ClientConfig clientConfig) {
		val connPool = new ConnectionPoolImpl(clientConfig);
		return new ConnectionFactoryImpl(clientConfig, connPool, bgExecutor);
	}

	<K, V> Map<K, V> createInstanceCache(final Object ignored) {
		return new ConcurrentHashMap<>();
	}

	/**
	 * Not used in this driver implementation
	 */
	@Override
	protected String requestNewPath(final String path) {
		throw new AssertionError("Should not be invoked");
	}

	/**
	 * Not used in this driver implementation
	 */
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
		final List<I> items;
		if (BYTES.equals(streamDataType)) {
			items = listStreams(itemFactory, path, prefix, idRadix, lastPrevItem, count);
		} else {
			items = makeEventItems(itemFactory, path, prefix, lastPrevItem, 1);
			//as we don't know how many items in the stream, we allocate memory for 1 item
		}
		return items;
	}

	List<I> listStreams(
					final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
					final I lastPrevItem, final int count) throws EOFException {

		val endpointUri = endpointCache.computeIfAbsent(endpointAddrs[0], this::createEndpointUri);
		val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
		val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);

		if (streamIterator == null) {
			val scopeName = path.startsWith(SLASH) ? path.substring(1) : path;
			streamIterator = controller.listStreams(scopeName);
		}

		final int prefixLength = (prefix == null || prefix.isEmpty()) ? 0 : prefix.length();
		final List<I> streamItems = new ArrayList<>(count);
		var i = 0;
		try {
			while (i < count) {
				val stream = streamIterator.getNext().get(controlApiTimeoutMillis, MILLISECONDS);
				if (null == stream) {
					if (i == 0) {
						streamIterator = null;
						throw new EOFException("End of stream listing");
					} else {
						break;
					}
				} else {
					val streamName = stream.getStreamName();
					try {
						if (prefixLength > 0) {
							if (streamName.startsWith(prefix)) {
								val streamItem = makeStreamItem(
												clientConfig, controller, streamName, idRadix, stream.getScope(), itemFactory);
								streamItems.add(streamItem);
								i++;
							}
						} else {
							val streamItem = makeStreamItem(
											clientConfig, controller, streamName, idRadix, stream.getScope(), itemFactory);
							streamItems.add(streamItem);
							i++;
						}
					} catch(final Exception e) {
						throwUncheckedIfInterrupted(e);
						LogUtil.exception(
							Level.WARN, e, "{}: failed to make the item for the stream \"{}\" @ scope \"{}\"",
							stepId, streamName, scopeName
						);
					}
				}
			}
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final ExecutionException e) {
			LogUtil.exception(Level.WARN, e, "{}: scope \"{}\" streams listing failure", stepId, scopeName);
		} catch (final TimeoutException e) {
			LogUtil.exception(Level.WARN, e, "{}: scope \"{}\" streams listing timeout", stepId, scopeName);
		}
		return streamItems;
	}

	I makeStreamItem(
					final ClientConfig clientConfig, final Controller controller, final String streamName, final int idRadix,
					final String scopeName, final ItemFactory<I> itemFactory) {
		val connFactory = connFactoryCache.computeIfAbsent(clientConfig, this::createConnectionFactory);
		val clientFactoryCreateFunc = byteStreamClientCreateFuncCache.computeIfAbsent(
						connFactory, ByteStreamClientFactoryCreateFunctionImpl::new);
		val clientFactory = byteStreamClientFactoryCache.computeIfAbsent(controller, clientFactoryCreateFunc);
		val byteStreamReaderCache = threadLocalByteStreamReaderCache.get();
		val byteStreamReader = byteStreamReaderCache.computeIfAbsent(
						streamName, clientFactory::createByteStreamReader);
		val byteStreamSize = byteStreamReader.fetchTailOffset();
		val offset = Long.parseLong(streamName, idRadix);
		return itemFactory.getItem(SLASH + scopeName + SLASH + streamName, offset, byteStreamSize);
	}

	List<I> makeEventItems(
					final ItemFactory<I> itemFactory, final String path, final String prefix, final I lastPrevItem, final int count) throws EOFException {
		if (null != lastPrevItem) {
			throw new EOFException();
		}
		val items = new ArrayList<I>();
		for (var i = 0; i < count; i++) {
			items.add(itemFactory.getItem(path + SLASH + (prefix == null ? i : prefix + i), 0, 0));
		}
		return items;
	}

	/**
	 * Not used in this driver implementation
	 */
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

	final class IoWorkerThreadFactory
					extends ContextAwareThreadFactory {

		public IoWorkerThreadFactory() {
			super("io_worker_" + stepId, true, ThreadContext.getContext());
		}

		@Override
		public final Thread newThread(final Runnable task) {
			return new IoWorkerThread(
							task, threadNamePrefix + "#" + threadNumber.incrementAndGet(), daemonFlag, exceptionHandler,
							threadContext);
		}

		final class IoWorkerThread
						extends LogContextThreadFactory.ContextAwareThread {

			public IoWorkerThread(
							final Runnable task, final String name, final boolean daemonFlag,
							final UncaughtExceptionHandler exceptionHandler, final Map<String, String> threadContext) {
				super(task, name, daemonFlag, exceptionHandler, threadContext);
			}

			@Override
			public final void interrupt() {
				val evtWriterCache = threadLocalEvtWriterCache.get();
				evtWriterCache.values().parallelStream().forEach(EventStreamWriter::close);
				evtWriterCache.clear();
				val txnEvtWriterCache = threadLocalTxnEvtWriterCache.get();
				txnEvtWriterCache
								.entrySet()
								.parallelStream()
								.forEach(
												e -> {
													e.getKey().close();
													e.getValue().close();
												});
				txnEvtWriterCache.clear();
				val byteStreamReaderCache = threadLocalByteStreamReaderCache.get();
				byteStreamReaderCache.values().parallelStream().forEach(ByteStreamReader::close);
				byteStreamReaderCache.clear();
				super.interrupt();
			}
		}
	}

	@Override
	protected ThreadFactory ioWorkerThreadFactory() {
		return new IoWorkerThreadFactory();
	}

	@Override
	protected final void execute(final O op)
					throws IllegalStateException {
		val opType = op.type();
		if (NOOP.equals(opType)) {
			noop(op);
		} else {
			final String nodeAddr = op.nodeAddr();
			switch (streamDataType) {
			case EVENTS:
				// do not use Collections.singletonList neither List.of as far as these methods return unmodifiable
				val ops = new ArrayList<O>();
				ops.add(op);
				execute(ops);
				break;
			case BYTES:
				byteStreamOperation(op, nodeAddr);
				break;
			default:
				throw new AssertionError("Unexpected stream data type: " + streamDataType);
			}
		}
	}

	@Override
	protected final void execute(final List<O> ops)
					throws IllegalStateException {
		val type = ops.get(0).type(); // any
		switch (type) {
		case CREATE:
			if (transactionMode) {
				createEventsTransaction(ops);
			} else {
				createEvents(ops);
			}
			break;
		case READ:
			readEvents(ops);
			break;
		default:
			throw new AssertionError("Unsupported event operation type: " + type);
		}
		try {
			ops.clear();
		} catch (final UnsupportedOperationException e) {
			LogUtil.exception(Level.WARN, e, "Failed to clear the ops buffer");
		}
	}

	void noop(final O op) {
		op.startRequest();
		completeOperation(op, SUCC);
	}

	void byteStreamOperation(final O op, final String nodeAddr) {
		val type = op.type();
		switch (type) {
		case CREATE:
			createByteStream(op, nodeAddr);
			break;
		case READ:
			readByteStream(op, nodeAddr);
			break;
		case UPDATE:
			appendByteStream(op, nodeAddr);
			break;
		case DELETE:
			deleteByteStream(op, nodeAddr);
			break;
		default:
			throw new AssertionError("Unsupported byte stream operation type: " + type);
		}
	}

	void createEventsTransaction(final List<O> ops) {
		val opsCount = ops.size();
		if (opsCount > 0) {
			try {
				val anyEvtOp = ops.get(0);
				val nodeAddr = anyEvtOp.nodeAddr();
				// prepare
				val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
				val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
				val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
				val scopeCreateFunc = scopeCreateFuncCache.computeIfAbsent(controller, ScopeCreateFunctionImpl::new);
				// create the scope if necessary
				val streamCreateFunc = streamCreateFuncCache.computeIfAbsent(scopeName, scopeCreateFunc);
				val streamName = extractStreamName(anyEvtOp.dstPath());
				scopeStreamsCache
								.computeIfAbsent(scopeName, this::createInstanceCache)
								.computeIfAbsent(streamName, streamCreateFunc);
				// create the client factory create function if necessary
				val clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(clientConfig,
								EventStreamClientFactoryCreateFunctionImpl::new);
				// create the client factory if necessary
				val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
				// create the batch event stream writer if necessary
				val txnEvtWriterCache = threadLocalTxnEvtWriterCache.get();
				val batchEvtWriter = txnEvtWriterCache.computeIfAbsent(
					clientFactory,
					clientFactory_ -> clientFactory_.createTransactionalEventWriter(
						UUID.randomUUID().toString(), streamName, evtSerializer, evtWriterConfig
					)
				);
				val txn = batchEvtWriter.beginTxn();
				O evtOp;
				I evtItem;
				var routingKey = (String) null;
				try {
					if (null == routingKeyFunc) {
						for (var i = 0; i < opsCount; i++) {
							evtOp = ops.get(i);
							evtOp.startRequest();
							txn.writeEvent(evtOp.item());
						}
					} else {
						for (var i = 0; i < opsCount; i++) {
							evtOp = ops.get(i);
							evtItem = evtOp.item();
							routingKey = routingKeyFunc.apply(evtItem);
							evtOp.startRequest();
							txn.writeEvent(routingKey, evtItem);
						}
					}
					txn.commit();
					completeOperations(ops, SUCC);
				} catch (final TxnFailedException e) {
					LogUtil.exception(
									Level.DEBUG, e, "{}: transaction failure, aborting {} events write", stepId, opsCount);
					completeOperations(ops, RESP_FAIL_UNKNOWN);
					txn.abort();
				}
			} catch (final Throwable e) {
				LogUtil.exception(
								Level.DEBUG, e, "{}: unexpected failure while trying to write {} events", stepId, opsCount);
				completeOperations(ops, FAIL_UNKNOWN);
				throwUncheckedIfInterrupted(e);
			}
		}
	}

	void createEvents(final List<O> ops) {
		val opsCount = ops.size();
		if (opsCount > 0) {
			try {
				val anyEvtOp = ops.get(0);
				val nodeAddr = anyEvtOp.nodeAddr();
				// prepare
				val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
				val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
				val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
				val scopeCreateFunc = scopeCreateFuncCache.computeIfAbsent(controller, ScopeCreateFunctionImpl::new);
				// create the scope if necessary
				val streamCreateFunc = streamCreateFuncCache.computeIfAbsent(scopeName, scopeCreateFunc);
				val streamName = extractStreamName(anyEvtOp.dstPath());
				scopeStreamsCache
								.computeIfAbsent(scopeName, this::createInstanceCache)
								.computeIfAbsent(streamName, streamCreateFunc);
				// create the client factory create function if necessary
				val clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(
								clientConfig,
								EventStreamClientFactoryCreateFunctionImpl::new);
				// create the client factory if necessary
				val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
				val evtWriterCache = threadLocalEvtWriterCache.get();
				val evtWriter = evtWriterCache.computeIfAbsent(
								streamName,
								(streamName_) -> clientFactory.createEventWriter(streamName, evtSerializer, evtWriterConfig));
				if (null == routingKeyFunc) {
					for (var i = 0; i < opsCount; i++) {
						val evtOp = ops.get(i);
						concurrencyThrottle.acquire();
						evtOp.startRequest();
						val evtWriteFuture = evtWriter.writeEvent(evtOp.item());
						evtWriteFuture.handle((result, thrown) -> handleEventWrite(evtOp, thrown));
						try {
							evtOp.finishRequest();
						} catch (final IllegalStateException ignored) {}
					}
				} else {
					for (var i = 0; i < opsCount; i++) {
						val evtOp = ops.get(i);
						val evtItem = evtOp.item();
						val routingKey = routingKeyFunc.apply(evtItem);
						concurrencyThrottle.acquire();
						evtOp.startRequest();
						val evtWriteFuture = evtWriter.writeEvent(routingKey, evtItem);
						evtWriteFuture.handle((result, thrown) -> handleEventWrite(evtOp, thrown));
						try {
							evtOp.finishRequest();
						} catch (final IllegalStateException ignored) {}
					}
				}
			} catch (final Throwable e) {
				LogUtil.exception(
					Level.DEBUG, e, "{}: unexpected failure while trying to write {} events", stepId, opsCount
				);
				completeOperations(ops, FAIL_UNKNOWN);
				throwUncheckedIfInterrupted(e);
			}
		}
	}

	boolean handleEventWrite(final O evtOp, final Throwable thrown) {
		concurrencyThrottle.release();
		if (null == thrown) {
			evtOp.startResponse();
			evtOp.finishResponse();
			try {
				evtOp.countBytesDone(evtOp.item().size());
			} catch (final IOException ignored) {}
			return completeOperation(evtOp, SUCC);
		} else {
			return completeFailedOperation(evtOp, thrown);
		}
	}

	void readEvents(final List<O> evtOps) {
		val opsCount = evtOps.size();
		if (opsCount > 0) {
			try {
				val anyEvtOp = evtOps.get(0);
				val nodeAddr = anyEvtOp.nodeAddr();
				val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
				val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
				val streamName = extractStreamName(anyEvtOp.dstPath());
				val readerGroupConfigBuilder = evtReaderGroupConfigBuilder.get();
				val readerGroupConfig = evtReaderGroupConfigCache.computeIfAbsent(
					scopeName + SLASH + streamName, key -> readerGroupConfigBuilder.stream(key).build());
				val readerGroupManagerCreateFunc = evtReaderGroupManagerCreateFuncCache.computeIfAbsent(
					clientConfig, ReaderGroupManagerCreateFunctionImpl::new);
				val clientFactoryCreateFunc = clientFactoryCreateFuncCache.computeIfAbsent(
					clientConfig, EventStreamClientFactoryCreateFunctionImpl::new);
				val clientFactory = clientFactoryCache.computeIfAbsent(scopeName, clientFactoryCreateFunc);
				val readerGroupManager =evtReaderGroupManagerCache.computeIfAbsent(
					scopeName, readerGroupManagerCreateFunc);
				final var evtReaderGroupName = evtReaderGroupCache
					.computeIfAbsent(scopeName, key -> new ConcurrentHashMap<>())
					.computeIfAbsent(streamName,  key ->  {
						val evtLocalReaderGroupName = "rg-" + scopeName + "-" + streamName + "-" + (System.nanoTime());
						readerGroupManager.createReaderGroup(evtLocalReaderGroupName, readerGroupConfig);
						return evtLocalReaderGroupName;
					});
				val evtReaderPool = evtStreamReaderPoolCache.computeIfAbsent(streamName, this::createEventStreamReaderPool);
				var evtReader_ = evtReaderPool.poll();
				if(null == evtReader_) {
					evtReader_ = clientFactory.createReader("reader-" + (System.nanoTime()), evtReaderGroupName, evtDeserializer, evtReaderConfig);
				}
				val evtReader = evtReader_;
				for(var i = 0; i < opsCount; i ++) {
					readEvent(readerGroupManager, evtReaderGroupName, evtReader, evtOps.get(i));
					//in multistream case readerGroup and associated reader might be different for each op
				}
				evtReaderPool.offer(evtReader);
			} catch(final Throwable e) {
				throwUncheckedIfInterrupted(e);
				completeOperations(evtOps, FAIL_UNKNOWN);
				LogUtil.exception(
					Level.DEBUG, e, "{}: unexpected failure while trying to read {} events", stepId, opsCount
				);
			}
		}
	}

	void readEvent(ReaderGroupManager readerGroupManager,String evtReaderGroupName, EventStreamReader<ByteBuffer> evtReader, final O evtOp)
	throws IOException {
		evtOp.startRequest();
		evtOp.finishRequest();
		// should we startRequest() after all preparations or at the beginning of the method in a multiStream case when we partially prepare objects inside this method?
		var evtRead_ = evtReader.readNextEvent(evtOpTimeoutMillis);
		while (evtRead_.isCheckpoint()) {
			Loggers.MSG.debug("{}: stream checkpoint @ position {}", stepId, evtRead_.getPosition());
			evtRead_ = evtReader.readNextEvent(evtOpTimeoutMillis);
		}
		evtOp.startResponse();
		evtOp.finishResponse();
		val evtRead = evtRead_;
		val evtData = evtRead.getEvent();
		if (null == evtData) {
			val streamPos = evtRead.getPosition();
			if (((PositionImpl)streamPos).getOwnedSegments().isEmpty()) {
				//means that reader doesn't own any segments, so it can't read anything
				Loggers.MSG.debug("{}: empty reader. No EventSegmentReader assigned", stepId);
				completeOperation(evtOp,PENDING);
			} else {
				val leftBytesForReaderGroup = ((ReaderGroupImpl)(readerGroupManager.getReaderGroup(evtReaderGroupName))).unreadBytes();
				if (leftBytesForReaderGroup == 0) {
					//end of all segments. unreadBytes() has a 20-30 sec delay.
					completeOperation(evtOp,FAIL_TIMEOUT);
					Loggers.MSG.info("{}: no more events for RG {}", stepId, evtReaderGroupName);
				} else {
					//end of one of the segments
					completeOperation(evtOp,PENDING);
				}
			}
			} else {
				val bytesDone = evtData.remaining();
				val evtItem = evtOp.item();
				evtItem.size(bytesDone);
				evtOp.countBytesDone(evtItem.size());
				completeOperation(evtOp, SUCC);
			}
	}

	void createByteStream(final O streamOp, final String nodeAddr) {
		try {
			val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
			val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
			val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
			val scopeCreateFuncForStreamConfig = scopeCreateFuncForStreamConfigCache.computeIfAbsent(
							controller, ScopeCreateFunctionForStreamConfigImpl::new);
			val streamConfig = scopeStreamConfigsCache.computeIfAbsent(scopeName, scopeCreateFuncForStreamConfig);
			val streamName = extractStreamName(streamOp.item().name());
			streamOp.startRequest();
			controller
							.createStream(scopeName, streamName, streamConfig)
							.handle(
											(createdFlag, thrown) -> {
												streamOp.startResponse();
												if (null != thrown) {
													LogUtil.exception(
																	Level.DEBUG,
																	thrown,
																	"Failed to create the stream {} in the scope {}",
																	streamName,
																	scopeName);
												}
												if (!createdFlag) {
													Loggers.ERR.warn(
																	"{}: failed to create the stream \"{}\" in the scope \"{}\", may be existing before",
																	stepId, streamName, scopeName);
													completeOperation(streamOp, RESP_FAIL_UNKNOWN);
												} else {
													handleByteStreamWrite(controller, clientConfig, streamName, streamOp);
												}
												return createdFlag;
											});
			try {
				streamOp.finishRequest();
			} catch (final IllegalStateException ignored) {}
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

	void readByteStream(final O streamOp, final String nodeAddr) {
		val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
		val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
		val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
		val streamItem = streamOp.item();
		val streamName = extractStreamName(streamItem.name());
		try {
			var remainingBytes = streamItem.size();
			if (remainingBytes > 0) {
				val connFactory = connFactoryCache.computeIfAbsent(clientConfig, this::createConnectionFactory);
				val clientFactoryCreateFunc = byteStreamClientCreateFuncCache.computeIfAbsent(
								connFactory, ByteStreamClientFactoryCreateFunctionImpl::new);
				val byteStreamReaderCache = threadLocalByteStreamReaderCache.get();
				val clientFactory = byteStreamClientFactoryCache.computeIfAbsent(controller, clientFactoryCreateFunc);
				val byteStreamReader = byteStreamReaderCache.computeIfAbsent(
								streamName, clientFactory::createByteStreamReader);
				byteStreamReader.seekToOffset(0);
				streamOp.startRequest();
				byteStreamReader
								.onDataAvailable()
								.handle(
												(availableByteCount, thrown) -> {
													streamOp.startResponse();
													streamOp.startDataResponse();
													return handleByteStreamRead(
																	streamOp, byteStreamReader, availableByteCount, thrown);
												});
				try {
					streamOp.finishRequest();
				} catch (final IllegalStateException ignored) {}
			}
		} catch (final IOException e) {
			LogUtil.exception(Level.DEBUG, e, "Failed to read the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_IO);
		} catch (final Throwable e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.WARN, e, "Failed to read the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_UNKNOWN);
		}
	}

	void appendByteStream(final O streamOp, final String nodeAddr) {
		// TODO
	}

	void deleteByteStream(final O streamOp, final String nodeAddr) {
		try {
			val streamName = extractStreamName(streamOp.item().name());
			val endpointUri = endpointCache.computeIfAbsent(nodeAddr, this::createEndpointUri);
			val clientConfig = clientConfigCache.computeIfAbsent(endpointUri, this::createClientConfig);
			val controller = controllerCache.computeIfAbsent(clientConfig, this::createController);
			controller
							.sealStream(scopeName, streamName)
							.handle(
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
			throwUncheckedIfInterrupted(cause);
			completeFailedOperation(streamOp, cause);
		}
	}

	void completeOperations(final List<O> ops, final Status status) {
		I item;
		O op;
		try {
			for (var i = 0; i < ops.size(); i++) {
				op = ops.get(i);
				op.status(status);
				item = op.item();
				op.countBytesDone(item.size());
				op.finishRequest();
				op.startResponse();
				op.finishResponse();
				handleCompleted(op);
			}
		} catch (final IOException ignored) {}
	}

	boolean completeOperation(final O op, final Status status) {
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
		try {
			streamOp.finishRequest();
		} catch (final IllegalStateException ignored) {}
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
	 * @param controller  pravega controller instance
	 * @param clientConfig pravega client config instance
	 * @param streamName  stream name
	 * @param streamOp    stream operation instance
	 */
	void handleByteStreamWrite(
					final Controller controller, final ClientConfig clientConfig, final String streamName, final O streamOp) {
		val streamItem = streamOp.item();
		try {
			var remainingBytes = streamItem.size();
			if (remainingBytes > 0) {
				val connFactory = connFactoryCache.computeIfAbsent(clientConfig, this::createConnectionFactory);
				val clientFactoryCreateFunc = byteStreamClientCreateFuncCache.computeIfAbsent(
								connFactory, ByteStreamClientFactoryCreateFunctionImpl::new);
				val clientFactory = byteStreamClientFactoryCache.computeIfAbsent(controller, clientFactoryCreateFunc);
				var countBytesDone = 0L;
				var n = 0L;
				val byteStreamWriteChan = new ByteStreamWriteChannel(clientFactory, streamName);
				try {
					while (remainingBytes > 0) {
						n = streamItem.writeToSocketChannel(byteStreamWriteChan, remainingBytes);
						if (0 < countBytesDone) {
							streamOp.startDataResponse();
						}
						countBytesDone += n;
						remainingBytes -= n;
					}
				} finally {
					streamOp.finishResponse();
					byteStreamWriteChan.close();
					streamOp.countBytesDone(countBytesDone);
					streamItem.size(countBytesDone);
				}
			}
			completeOperation(streamOp, SUCC);
		} catch (final IOException e) {
			LogUtil.exception(Level.DEBUG, e, "Failed to write the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_IO);
		} catch (final Throwable e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(Level.WARN, e, "Failed to write the bytes stream {}", streamName);
			completeOperation(streamOp, FAIL_UNKNOWN);
		}
	}

	int handleByteStreamRead(
					final O streamOp, final ByteStreamReader reader,
					final int availableByteCount, final Throwable thrown) {
		if (thrown == null) {
			val buff = DirectMemUtil.getThreadLocalReusableBuff(availableByteCount);
			try {
				val n = reader.read(buff);
				if (n > 0) {
					val transferredByteCount = streamOp.countBytesDone() + n;
					streamOp.countBytesDone(transferredByteCount);
					val expectedStreamSize = streamOp.item().size();
					if (expectedStreamSize > transferredByteCount) {
						reader
										.onDataAvailable()
										.handle(
														(availableByteCount_, thrown_) -> handleByteStreamRead(streamOp, reader, availableByteCount_, thrown_));
					} else {
						completeByteStreamRead(streamOp, reader, null);
					}
				} else { // end of byte stream
					completeByteStreamRead(streamOp, reader, null);
				}
			} catch (final Throwable e) {
				completeByteStreamRead(streamOp, reader, e);
			}
		} else {
			completeByteStreamRead(streamOp, reader, thrown);
		}
		return availableByteCount;
	}

	void completeByteStreamRead(
					final O streamOp, final ByteStreamReader reader, final Throwable e) {
		streamOp.finishResponse();
		if (null == e) {
			completeOperation(streamOp, SUCC);
		} else if (e instanceof IOException) {
			LogUtil.exception(Level.DEBUG, e, "{}: failure", streamOp);
			completeOperation(streamOp, FAIL_IO);
		} else if (e instanceof InterruptedException) {
			completeOperation(streamOp, INTERRUPTED);
			throwUnchecked(e);
		} else {
			LogUtil.exception(Level.WARN, e, "{}: failure", streamOp);
			completeOperation(streamOp, FAIL_UNKNOWN);
		}
	}

	@Override
	protected void doClose()
					throws IOException {
		super.doClose();
		// clear all caches & pools
		evtReaderGroupManagerCreateFuncCache.clear();
		closeAllWithTimeout(evtReaderGroupManagerCache.values());
		evtReaderGroupManagerCache.clear();
		closeAllWithTimeout(evtStreamReaderCreateFuncCache.keySet());
		evtStreamReaderCreateFuncCache.clear();
		evtStreamReaderPoolCache.values().forEach(
				pool -> {
					closeAllWithTimeout(pool);
					pool.clear();
				}
		);
		evtStreamReaderPoolCache.clear();
		scopeStreamConfigsCache.clear();
		closeAllWithTimeout(connFactoryCache.values());
		connFactoryCache.clear();
		byteStreamClientCreateFuncCache.clear();
		closeAllWithTimeout(byteStreamClientFactoryCache.values());
		byteStreamClientFactoryCache.clear();
		scopeCreateFuncCache.clear();
		streamCreateFuncCache.clear();
		scopeStreamsCache.values().forEach(Map::clear);
		scopeStreamsCache.clear();
		clientFactoryCreateFuncCache.clear();
		closeAllWithTimeout(clientFactoryCache.values());
		clientFactoryCache.clear();
		clientConfigCache.clear();
		closeAllWithTimeout(controllerCache.values());
		controllerCache.clear();
		endpointCache.clear();
		bgExecutor.shutdownNow();
	}

	void closeAllWithTimeout(final Collection<? extends AutoCloseable> closeables) {
		if (null != closeables && closeables.size() > 0) {
			final ExecutorService closeExecutor = Executors.newFixedThreadPool(closeables.size());
			closeables.forEach(
							closeable -> closeExecutor.submit(
											() -> {
												try {
													closeable.close();
												} catch (final Exception e) {
													throwUncheckedIfInterrupted(e);
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
