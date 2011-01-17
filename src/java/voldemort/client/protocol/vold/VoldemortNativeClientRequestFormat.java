/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.versioning.VectorClockVersionSerializer;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The {@link voldemort.client.protocol.RequestFormat} for a low-overhead custom
 * binary protocol
 * 
 * 
 */
public class VoldemortNativeClientRequestFormat implements RequestFormat {

    protected final ErrorCodeMapper mapper;

    private final Logger logger = Logger.getLogger(getClass());

    public VoldemortNativeClientRequestFormat() {
        mapper = new ErrorCodeMapper();
    }

    public ClientRequestFormat<List<Versioned<byte[]>>> createGetRequest(final String storeName,
                                                                         final ByteArray key,
                                                                         final byte[] transforms,
                                                                         final RequestRoutingType routing) {
        return new ClientRequestFormat<List<Versioned<byte[]>>>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeGetRequest(outputStream, storeName, key, transforms, routing);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteGetResponse(buffer);
            }

            public List<Versioned<byte[]>> readResponse(DataInputStream inputStream)
                    throws IOException {
                return readGetResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>> createGetAllRequest(final String storeName,
                                                                                            final Iterable<ByteArray> key,
                                                                                            final Map<ByteArray, byte[]> transforms,
                                                                                            final RequestRoutingType routing) {
        return new ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeGetAllRequest(outputStream, storeName, key, transforms, routing);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteGetAllResponse(buffer);
            }

            public Map<ByteArray, List<Versioned<byte[]>>> readResponse(DataInputStream inputStream)
                    throws IOException {
                return readGetAllResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<List<Version>> createGetVersionsRequest(final String storeName,
                                                                       final ByteArray key,
                                                                       final RequestRoutingType routing) {
        return new ClientRequestFormat<List<Version>>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeGetVersionRequest(outputStream, storeName, key, routing);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteGetVersionResponse(buffer);
            }

            public List<Version> readResponse(DataInputStream inputStream) throws IOException {
                return readGetVersionResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<Boolean> createDeleteRequest(final String storeName,
                                                            final ByteArray key,
                                                            final Version version,
                                                            final RequestRoutingType routing) {
        return new ClientRequestFormat<Boolean>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writeDeleteRequest(outputStream, storeName, key, version, routing);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompleteDeleteResponse(buffer);
            }

            public Boolean readResponse(DataInputStream inputStream) throws IOException {
                return readDeleteResponse(inputStream);
            }
        };
    }

    public ClientRequestFormat<Version> createPutRequest(final String storeName,
                                                         final ByteArray key,
                                                         final Versioned<byte[]> value,
                                                         final byte[] transforms,
                                                         final RequestRoutingType routing) {
        return new ClientRequestFormat<Version>() {

            public boolean writeRequest(DataOutputStream outputStream) throws IOException {
                writePutRequest(outputStream, storeName, key, value, transforms, routing);
                return true;
            }

            public boolean isCompleteResponse(ByteBuffer buffer) {
                return isCompletePutResponse(buffer);
            }

            public Version readResponse(DataInputStream inputStream) throws IOException {
                return readPutResponse(inputStream);
            }
        };
    }

    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V1;
    }

    protected void writeMessageHeader(DataOutputStream outputStream,
                                      VoldemortOpCode operation,
                                      String storeName,
                                      RequestRoutingType routingType) throws IOException {
        outputStream.writeByte(operation.asCode());
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(routingType.equals(RequestRoutingType.ROUTED));
    }

    @SuppressWarnings("unused")
    protected int getHeaderSize(VoldemortOpCode operation,
                                String storeName,
                                RequestRoutingType routingType) throws IOException {
        int headerSize = 1 + // Operation is a byte
        VoldemortNativeProtocol.getStringRequestSize(storeName) + 1; // Size of
        // boolean
        return headerSize;
    }

    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   ByteArray key,
                                   Version version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        writeMessageHeader(outputStream, VoldemortOpCode.DELETE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);

        // Unlike other methods, delete used shorts not ints
        outputStream.writeShort(VectorClockVersionSerializer.sizeInBytes(version));
        outputStream.write(VectorClockVersionSerializer.toBytes(version));
    }

    public boolean isCompleteDeleteResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer, VoldemortOpCode.DELETE);
    }

    public boolean readDeleteResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return inputStream.readBoolean();
    }

    protected List<Versioned<byte[]>> readVersioneds(DataInputStream inputStream)
            throws IOException {
        return VoldemortNativeProtocol.readVersioneds(inputStream);
    }

    protected int getVersionSize(Version version) {
        return 4 + VectorClockVersionSerializer.sizeInBytes(version);
    }

    protected Version readVersion(DataInputStream inputStream) throws IOException {
        return VoldemortNativeProtocol.readVersion(inputStream);
    }

    protected void writeVersion(DataOutputStream outputStream, Version version) throws IOException {
        byte[] bytes = VectorClockVersionSerializer.toBytes(version);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    protected int getVersionedSize(Versioned<byte[]> versioned) {
        return 4 + VectorClockVersionSerializer.sizeInBytes(versioned);
    }

    protected int getVersionedsSize(List<Versioned<byte[]>> versioneds) {
        int size = 4;
        for(Versioned<byte[]> v: versioneds) {
            size += getVersionedSize(v);
        }
        return size;
    }

    protected void writeVersioned(DataOutputStream outputStream, Versioned<byte[]> versioned)
            throws IOException {
        byte[] bytes = VectorClockVersionSerializer.toBytes(versioned);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    @SuppressWarnings("unused")
    public void writeGetRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        writeMessageHeader(outputStream, VoldemortOpCode.GET, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return readVersioneds(inputStream);
    }

    public boolean isCompleteGetResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer, VoldemortOpCode.GET);
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   Map<ByteArray, byte[]> transforms,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKeys(keys);
        // write out keys
        List<ByteArray> list = new ArrayList<ByteArray>();
        for(ByteArray key: keys) {
            list.add(key);
        }
        writeGetAllRequest(output, storeName, list, transforms, routingType);
    }

    @SuppressWarnings("unused")
    protected void writeGetAllRequest(DataOutputStream output,
                                      String storeName,
                                      List<ByteArray> keys,
                                      Map<ByteArray, byte[]> transforms,
                                      RequestRoutingType routingType) throws IOException {
        this.writeMessageHeader(output, VoldemortOpCode.GET_ALL, storeName, routingType);
        output.writeInt(keys.size());
        for(ByteArray key: keys) {
            VoldemortNativeProtocol.writeKey(output, key);
        }
    }

    public boolean isCompleteGetAllResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer, VoldemortOpCode.GET_ALL);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream stream)
            throws IOException {
        checkException(stream);
        int numResults = stream.readInt();
        Map<ByteArray, List<Versioned<byte[]>>> results = new HashMap<ByteArray, List<Versioned<byte[]>>>(numResults);
        for(int i = 0; i < numResults; i++) {
            ByteArray key = VoldemortNativeProtocol.readKey(stream);
            results.put(key, readVersioneds(stream));
        }
        return results;
    }

    @SuppressWarnings("unused")
    public void writePutRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                byte[] transforms,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        this.writeMessageHeader(outputStream, VoldemortOpCode.PUT, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);
        writeVersioned(outputStream, versioned);
    }

    public boolean isCompletePutResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer, VoldemortOpCode.PUT);
    }

    public Version readPutResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return null;
    }

    protected VoldemortException getException(short code, DataInputStream inputStream)
            throws IOException {
        if(code != 0) {
            String error = inputStream.readUTF();
            return mapper.getError(code, error);
        } else {
            return null;
        }
    }

    /*
     * If there is an exception, throw it
     */
    protected void checkException(DataInputStream inputStream) throws IOException {
        short retCode = inputStream.readShort();
        if(retCode != 0) {
            throw getException(retCode, inputStream);
        }
    }

    public boolean isCompleteGetVersionResponse(ByteBuffer buffer) {
        return isCompleteResponse(buffer, VoldemortOpCode.GET_VERSION);
    }

    public List<Version> readGetVersionResponse(DataInputStream stream) throws IOException {
        checkException(stream);
        int resultSize = stream.readInt();
        List<Version> results = new ArrayList<Version>(resultSize);
        for(int i = 0; i < resultSize; i++) {
            results.add(readVersion(stream));
        }
        return results;
    }

    public void writeGetVersionRequest(DataOutputStream output,
                                       String storeName,
                                       ByteArray key,
                                       RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        this.writeMessageHeader(output, VoldemortOpCode.GET_VERSION, storeName, routingType);
        VoldemortNativeProtocol.writeKey(output, key);
    }

    private boolean isCompleteResponse(ByteBuffer buffer, VoldemortOpCode operation) {
        DataInputStream inputStream = new DataInputStream(new ByteBufferBackedInputStream(buffer));

        try {
            try {
                switch(operation) {
                    case GET:
                        readGetResponse(inputStream);
                        break;

                    case GET_VERSION:
                        readGetVersionResponse(inputStream);
                        break;

                    case GET_ALL:
                        readGetAllResponse(inputStream);
                        break;

                    case DELETE:
                        readDeleteResponse(inputStream);
                        break;

                    case PUT:
                        readPutResponse(inputStream);
                        break;
                    default:
                        throw new VoldemortException("Unexpected response code " + operation);
                }
            } catch(VoldemortException e) {
                // Ignore application-level exceptions
            }

            // If there aren't any remaining, we've "consumed" all the bytes and
            // thus have a complete request...
            return !buffer.hasRemaining();
        } catch(Exception e) {
            // This could also occur if the various methods we call into
            // re-throw a corrupted value error as some other type of exception.
            // For example, updating the position on a buffer past its limit
            // throws an InvalidArgumentException.
            if(logger.isDebugEnabled())
                logger.debug("Probable partial read occurred causing exception", e);

            return false;
        }
    }
}
