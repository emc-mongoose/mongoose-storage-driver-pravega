package com.emc.mongoose.storage.driver.pravega;

import com.emc.mongoose.data.DataInput;
import com.emc.mongoose.exception.InterruptRunException;
import com.emc.mongoose.exception.OmgShootMyFootException;
import com.emc.mongoose.item.DataItem;
import com.emc.mongoose.item.Item;
import com.emc.mongoose.item.ItemFactory;
import com.emc.mongoose.item.PathItem;
import com.emc.mongoose.item.op.OpType;
import com.emc.mongoose.item.op.Operation;
import com.emc.mongoose.item.op.data.DataOperation;
import com.emc.mongoose.item.op.path.PathOperation;
import com.emc.mongoose.logging.LogUtil;
import com.emc.mongoose.logging.Loggers;
import com.emc.mongoose.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

public class PravegaStorageDriver<I extends Item, O extends Operation<I>>
        extends CoopStorageDriverBase<I, O> {

    protected final String uriSchema;

    protected final String[] endpointAddrs;
    protected final int nodePort;
    private final AtomicInteger rrc = new AtomicInteger(0);

    protected int inBuffSize = BUFF_SIZE_MIN;
    protected int outBuffSize = BUFF_SIZE_MAX;

    private StreamManager streamManager;
    private Map<String, Map<String, Stream>> scopeMap = new HashMap<>();

    public PravegaStorageDriver(
            final String uriSchema, final String testStepId, final DataInput dataInput,
            final Config storageConfig, final boolean verifyFlag, final int batchSize
    )
            throws OmgShootMyFootException {
        super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
        this.uriSchema = uriSchema;
        streamManager = StreamManager.create(URI.create(uriSchema));

        final String uid = credential == null ? null : credential.getUid();
        final Config nodeConfig = storageConfig.configVal("net-node");
        nodePort = storageConfig.intVal("net-node-port");
        final List<String> endpointAddrList = nodeConfig.listVal("addrs");
        endpointAddrs = endpointAddrList.toArray(new String[endpointAddrList.size()]);
        requestAuthTokenFunc = null; // do not use
        requestNewPathFunc = null; // do not use
    }

    @Override
    protected String requestNewPath(final String path) {
        throw new AssertionError("Should not be invoked");
    }

    @Override
    protected String requestNewAuthToken(final Credential credential) {
        throw new AssertionError("Should not be invoked");
    }

    @Override
    public List<I> list(
            final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
            final I lastPrevItem, final int count
    )
            throws IOException {
        return null;
    }

    @Override
    public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
        int size;
        if (avgTransferSize < BUFF_SIZE_MIN) {
            size = BUFF_SIZE_MIN;
        } else if (BUFF_SIZE_MAX < avgTransferSize) {
            size = BUFF_SIZE_MAX;
        } else {
            size = (int) avgTransferSize;
        }
        if (OpType.CREATE.equals(opType)) {
            Loggers.MSG.info("Adjust output buffer size: {}", SizeInBytes.formatFixedSize(size));
            outBuffSize = size;
        } else if (OpType.READ.equals(opType)) {
            Loggers.MSG.info("Adjust input buffer size: {}", SizeInBytes.formatFixedSize(size));
            inBuffSize = size;
        }
    }

    @Override
    protected boolean submit(O op) throws InterruptRunException, IllegalStateException {
        try {
            if (op instanceof DataOperation) {
                final DataOperation dataOp = (DataOperation) op;
                final DataItem dataItem = dataOp.item();
                switch (dataOp.type()) {
                    case NOOP:
                        // if(success) return true;
                        break;
                    case CREATE:
                        return submitItemCreate(dataOp, dataItem);
                    case READ:
                        //TODO: EventStreamReader<ByteBuffer>  ->  readNextEvent
                        // if(success) return true;
                        break;
                    default:
                        throw new AssertionError("Operation " + op.type() + " isn't supported");
                }
            } else if (op instanceof PathOperation) {
                final PathOperation pathOp = (PathOperation) op;
                final PathItem pathItem = pathOp.item();
                switch (pathOp.type()) {
                    case NOOP:
                        // if(success) return true;
                        break;
                    case CREATE:
                        //TODO: StreamManager.createStream
                        // if(success) return true;
                        break;
                    case READ:
                        // if(success) return true;
                        break;
                    case DELETE:
                        //TODO: StreamManager.deleteStream
                        // if(success) return true;
                        break;
                    default:
                        throw new AssertionError("Operation " + op.type() + "  isn't supported");
                }
            } else {
                throw new AssertionError("Operation type " + op.type() + " isn't supported");
            }
        } catch (final RuntimeException e) {
            final Throwable cause = e.getCause();

            if (cause instanceof IOException) {
                LogUtil.exception(Level.DEBUG, cause, "Failed IO");
            } else if (cause != null) {
                LogUtil.exception(Level.DEBUG, cause, "Unexpected failure");
            } else {
                LogUtil.exception(Level.DEBUG, e, "Unexpected failure");
            }
        }

        return false;
    }

    protected boolean submitItemCreate(final DataOperation<? extends DataItem> eventOperation, final DataItem eventItem)
    {
        final String path = eventOperation.dstPath();
        final String scope = path.substring(0, path.indexOf("/"));
        final String streamName = path.substring(path.indexOf("/")+1);

        if (scopeMap.get(scope).get(streamName) == null) {
            Loggers.ERR.debug("Failed to create event: the stream {}  not found in the scope {}", streamName, scope);
            eventOperation.status(Operation.Status.RESP_FAIL_UNKNOWN);
            return false;
        };

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scope, streamName, streamConfig);

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, URI.create(uriSchema));
             EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName,
                     null,
                     EventWriterConfig.builder().build())) {

            final long buffSize;
            try {
                buffSize = eventItem.size();
            } catch (IOException e) {
                eventOperation.status(Operation.Status.FAIL_IO);
                Loggers.ERR.debug("Failed to create event: " + e.getMessage());
                return false;
            }

            final DataInput dataInput = eventItem.dataInput();
            final int dataSize = dataInput.getSize();

            final ByteBuffer outBuff = dataInput
                    .getLayer(eventItem.layer())
                    .asReadOnlyBuffer();

            outBuff.position((int)eventItem.offset());

            if (dataSize > outBuff.remaining()){
                outBuff.position(0);
                eventItem.offset(0);
            }
            outBuff.limit(dataSize);

            final CompletableFuture writeFuture = writer.writeEvent(outBuff).thenAccept(result -> {
                eventItem.offset((eventItem.offset() + dataSize) % buffSize);
                eventOperation.countBytesDone(dataSize);
                eventOperation.status(Operation.Status.SUCC);
            });

            try {
                writeFuture.get();
            } catch ( Exception e) {
                Loggers.ERR.debug("Failed to create event: " + e.getMessage());
                eventOperation.status(Operation.Status.RESP_FAIL_CLIENT);
                return false;
            }

            return true;
        }
    }




    @Override
    protected int submit(List<O> ops, int from, int to) throws InterruptRunException, IllegalStateException {
        int i = from;
        for (; i < to; i++) {
            if (!submit(ops.get(i))) break;
        }
        return i - from;
    }

    @Override
    protected int submit(List<O> ops) throws InterruptRunException, IllegalStateException {
        return submit(ops, 0, ops.size());
    }

    @Override
    protected void doClose()
            throws IOException {
        super.doClose();
    }

    @Override
    public String toString() {
        return String.format(super.toString(), "pravega");
    }
}
