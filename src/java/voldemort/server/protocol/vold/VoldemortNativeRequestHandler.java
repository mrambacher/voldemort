package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClockVersionSerializer;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

/**
 * Server-side request handler for voldemort native client protocol
 * 
 * 
 */
public class VoldemortNativeRequestHandler extends AbstractRequestHandler implements RequestHandler {

    private final Logger logger = Logger.getLogger(VoldemortNativeRequestHandler.class);

    // private final RequestFormatType protocol;

    public VoldemortNativeRequestHandler(ErrorCodeMapper errorMapper, StoreRepository repository) {
        super(errorMapper, repository);
    }

    public StreamRequestHandler handleRequest(DataInputStream inputStream,
                                              DataOutputStream outputStream) throws IOException {
        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        RequestRoutingType routingType = getRoutingType(inputStream);

        Store<ByteArray, byte[]> store = getStore(storeName, routingType);
        if(store == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        } else {
            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    handleGet(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    handleGetAll(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    handlePut(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    handleDelete(inputStream, outputStream, store);
                    break;
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    handleGetVersion(inputStream, outputStream, store);
                    break;
                default:
                    throw new IOException("Unknown op code: " + opCode);
            }
        }
        outputStream.flush();
        return null;
    }

    protected RequestRoutingType getRoutingType(DataInputStream inputStream) throws IOException {
        boolean isRouted = inputStream.readBoolean();
        return RequestRoutingType.getRequestRoutingType(isRouted, false);
    }

    private void handleGetVersion(DataInputStream inputStream,
                                  DataOutputStream outputStream,
                                  Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        List<Version> results = null;
        try {
            results = store.getVersions(key);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
        outputStream.writeInt(results.size());
        for(Version version: results) {
            writeVersion(outputStream, version);
        }
    }

    protected void writeVersion(DataOutputStream outputStream, Version version) throws IOException {
        byte[] bytes = VectorClockVersionSerializer.toBytes(version);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    protected void writeVersioned(DataOutputStream outputStream, Versioned<byte[]> versioned)
            throws IOException {
        byte[] bytes = VectorClockVersionSerializer.toBytes(versioned);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    /**
     * This is pretty ugly. We end up mimicking the request logic here, so this
     * needs to stay in sync with handleRequest.
     */

    protected void checkCompleteGetRequest(DataInputStream inputStream) throws IOException {
        VoldemortNativeProtocol.readKey(inputStream);
    }

    protected void checkCompleteGetAllRequest(DataInputStream inputStream) throws IOException {
        int numKeys = inputStream.readInt();

        // Read the keys to skip the bytes.
        for(int i = 0; i < numKeys; i++) {
            VoldemortNativeProtocol.readKey(inputStream);
        }
    }

    protected int checkCompletePutRequest(DataInputStream inputStream) throws IOException {
        VoldemortNativeProtocol.readKey(inputStream);

        int dataSize = inputStream.readInt();
        return dataSize;
    }

    protected int checkCompleteDeleteRequest(DataInputStream inputStream) throws IOException {
        VoldemortNativeProtocol.readKey(inputStream);
        int versionSize = inputStream.readShort();
        return versionSize;
    }

    protected void checkCompleteRequestHeader(DataInputStream inputStream) throws IOException {
        // Read the store name in, but just to skip the bytes.
        inputStream.readUTF();

        // Read the 'is routed' flag in, but just to skip the byte.
        inputStream.readBoolean();
    }

    protected boolean isCompleteRequest(final ByteBuffer buffer, DataInputStream inputStream)
            throws IOException {
        byte opCode = inputStream.readByte();
        int newPosition;
        checkCompleteRequestHeader(inputStream);

        switch(opCode) {
            case VoldemortOpCode.GET_OP_CODE:
            case VoldemortOpCode.GET_VERSION_OP_CODE:
                // Read the key just to skip the bytes.
                checkCompleteGetRequest(inputStream);
                break;
            case VoldemortOpCode.GET_ALL_OP_CODE:
                checkCompleteGetAllRequest(inputStream);
                break;
            case VoldemortOpCode.PUT_OP_CODE:
                int dataSize = checkCompletePutRequest(inputStream);
                newPosition = buffer.position() + dataSize;

                if(newPosition > buffer.limit() || newPosition < 0) {
                    return false;
                    // throw new
                    // Exception("Data inconsistency on put - dataSize: " +
                    // dataSize
                    // + ", position: " + buffer.position() + ", limit: "
                    // + buffer.limit());
                }
                // Here we skip over the data (without reading it in) and
                // move our position to just past it.
                buffer.position(buffer.position() + dataSize);
                break;
            case VoldemortOpCode.DELETE_OP_CODE:
                int versionSize = checkCompleteDeleteRequest(inputStream);
                newPosition = buffer.position() + versionSize;

                if(newPosition > buffer.limit() || newPosition < 0) {
                    return false;
                    // throw new
                    // Exception("Data inconsistency on delete - versionSize: "
                    // + versionSize + ", position: " + buffer.position()
                    // + ", limit: " + buffer.limit());
                }
                // Here we skip over the version (without reading it in) and
                // move our position to just past it.
                buffer.position(buffer.position() + versionSize);
                break;
            default:
                // Do nothing, let the request handler address this...
        }

        // If there aren't any remaining, we've "consumed" all the bytes and
        // thus have a complete request...
        return !buffer.hasRemaining();

    }

    public boolean isCompleteRequest(final ByteBuffer buffer) {
        ByteBufferBackedInputStream bbbis = null;
        DataInputStream inputStream = null;

        try {
            bbbis = new ByteBufferBackedInputStream(buffer);
            inputStream = new DataInputStream(bbbis);
            return isCompleteRequest(buffer, inputStream);
        } catch(Exception e) {
            // This could also occur if the various methods we call into
            // re-throw a corrupted value error as some other type of exception.
            // For example, updating the position on a buffer past its limit
            // throws an InvalidArgumentException.
            if(logger.isDebugEnabled())
                logger.debug("Probable partial read occurred causing exception", e);

            return false;
        } finally {
            tryClose(inputStream);
            tryClose(bbbis);
        }
    }

    protected void tryClose(InputStream is) {
        if(is != null) {
            try {
                is.close();
            } catch(IOException e) {

            }
        }
    }

    protected void writeVersioneds(DataOutputStream outputStream, List<Versioned<byte[]>> versioneds)
            throws IOException {
        outputStream.writeInt(versioneds.size());
        for(Versioned<byte[]> versioned: versioneds) {
            writeVersioned(outputStream, versioned);
        }
    }

    protected Versioned<byte[]> readVersioned(DataInputStream inputStream) throws IOException {
        return VoldemortNativeProtocol.readVersioned(inputStream);
    }

    protected Version readVersion(DataInputStream inputStream) throws IOException {
        return VoldemortNativeProtocol.readVersion(inputStream);
    }

    protected void handleGet(DataInputStream inputStream,
                             DataOutputStream outputStream,
                             Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        List<Versioned<byte[]>> results = null;
        try {
            results = store.get(key);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
        writeVersioneds(outputStream, results);
    }

    protected void handleGetAll(DataInputStream inputStream,
                                DataOutputStream outputStream,
                                Store<ByteArray, byte[]> store) throws IOException {
        // read keys
        int numKeys = inputStream.readInt();
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(VoldemortNativeProtocol.readKey(inputStream));

        // execute the operation
        Map<ByteArray, List<Versioned<byte[]>>> results = null;
        try {
            results = store.getAll(keys);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
            return;
        }

        // write back the results
        outputStream.writeInt(results.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: results.entrySet()) {
            // write the key
            VoldemortNativeProtocol.writeKey(outputStream, entry.getKey());
            // write the values
            writeVersioneds(outputStream, entry.getValue());
        }
    }

    protected void handlePut(DataInputStream inputStream,
                             DataOutputStream outputStream,
                             Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        Versioned<byte[]> versioned = VoldemortNativeProtocol.readVersioned(inputStream);
        try {
            store.put(key, versioned);
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    protected void handleDelete(DataInputStream inputStream,
                                DataOutputStream outputStream,
                                Store<ByteArray, byte[]> store) throws IOException {
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        int versionSize = inputStream.readShort();
        byte[] versionBytes = new byte[versionSize];
        ByteUtils.read(inputStream, versionBytes);
        Version version = VersionFactory.toVersion(versionBytes);
        try {
            boolean succeeded = store.delete(key, version);
            outputStream.writeShort(0);
            outputStream.writeBoolean(succeeded);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    protected void writeException(DataOutputStream stream, VoldemortException ex)
            throws IOException {
        stream.writeShort(this.getErrorMapper().getCode(ex));
        stream.writeUTF(ex.getMessage());
    }
}
