package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

/**
 * FetchEntries fetches and return key/value entry.
 * <p>
 * For performance reason use storageEngine.keys() iterator to filter out
 * unwanted keys and then call storageEngine.get() for valid keys.
 * <p>
 */

public class FetchEntriesStreamRequestHandler extends FetchStreamRequestHandler {

    private final ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator;

    public FetchEntriesStreamRequestHandler(FetchPartitionEntriesRequest request,
                                            MetadataStore metadataStore,
                                            ErrorCodeMapper errorCodeMapper,
                                            VoldemortConfig voldemortConfig,
                                            StoreRepository storeRepository,
                                            NetworkClassLoader networkClassLoader) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader);
        entryIterator = storageEngine.entries(partitionList, filter, null);
    }

    @Override
    protected void closeIterator() {
        if(null != entryIterator)
            entryIterator.close();
    }

    @Override
    protected boolean hasNext() {
        return entryIterator.hasNext();
    }

    protected Pair<ByteArray, Versioned<byte[]>> next() {
        return entryIterator.next();
    }

    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!hasNext())
            return StreamRequestHandlerState.COMPLETE;

        Pair<ByteArray, Versioned<byte[]>> entry = next();
        ByteArray key = entry.getFirst();
        Versioned<byte[]> value = entry.getSecond();
        if(validPartition(key.get()) && counter % skipRecords == 0) {
            throttler.maybeThrottle(key.length());
            fetched++;
            VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();

            VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                  .setKey(ProtoUtils.encodeBytes(key))
                                                                                  .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                  .build();
            response.setPartitionEntry(partitionEntry);

            Message message = response.build();
            ProtoUtils.writeMessage(outputStream, message);

            throttler.maybeThrottle(AdminServiceRequestHandler.valueSize(value));
        }

        // log progress
        counter++;

        if(0 == counter % 100000) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            if(logger.isDebugEnabled())
                logger.debug("fetchEntries() scanned " + counter + " entries, fetched " + fetched
                             + " entries for store:" + storageEngine.getName() + " partition:"
                             + partitionList + " in " + totalTime + " s");
        }

        if(hasNext())
            return StreamRequestHandlerState.WRITING;
        else
            return StreamRequestHandlerState.COMPLETE;
    }

}
