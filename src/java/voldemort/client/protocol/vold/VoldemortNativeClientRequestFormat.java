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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatType;
import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClockVersionSerializer;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The {@link voldemort.client.protocol.RequestFormat} for a low-overhead custom
 * binary protocol
 * 
 * @author jay
 * 
 */
public class VoldemortNativeClientRequestFormat implements RequestFormat {

    protected final ErrorCodeMapper mapper;

    public VoldemortNativeClientRequestFormat() {
        mapper = new ErrorCodeMapper();
    }

    protected RequestFormatType getProtocol() {
        return RequestFormatType.VOLDEMORT_V1;
    }

    protected void writeMessageHeader(DataOutputStream outputStream,
                                      byte operation,
                                      String storeName,
                                      RequestRoutingType routingType) throws IOException {
        outputStream.writeByte(operation);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(routingType.equals(RequestRoutingType.ROUTED));
        /*
         * if(protocol.getVersion() >= 2) { if(operation ==
         * VoldemortOpCode.DELETE_OP_CODE) {
         * outputStream.writeByte(routingType.getRoutingTypeCode()); } else {
         * outputStream.writeUTF(routingType.toString()); } }
         */
    }

    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   ByteArray key,
                                   Version version,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        writeMessageHeader(outputStream, VoldemortOpCode.DELETE_OP_CODE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);

        // Unlike other methods, delete used shorts not ints
        outputStream.writeShort(VectorClockVersionSerializer.sizeInBytes(version));
        outputStream.write(VectorClockVersionSerializer.toBytes(version));
    }

    public boolean readDeleteResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return inputStream.readBoolean();
    }

    protected List<Versioned<byte[]>> readVersioneds(DataInputStream inputStream)
            throws IOException {
        return VoldemortNativeProtocol.readVersioneds(inputStream);
    }

    protected Version readVersion(DataInputStream inputStream) throws IOException {
        return VoldemortNativeProtocol.readVersion(inputStream);
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

    public void writeGetRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        writeMessageHeader(outputStream, VoldemortOpCode.GET_OP_CODE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return readVersioneds(inputStream);
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKeys(keys);
        this.writeMessageHeader(output, VoldemortOpCode.GET_ALL_OP_CODE, storeName, routingType);
        // write out keys
        List<ByteArray> l = new ArrayList<ByteArray>();
        for(ByteArray key: keys)
            l.add(key);
        output.writeInt(l.size());
        for(ByteArray key: keys) {
            VoldemortNativeProtocol.writeKey(output, key);
        }
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

    public void writePutRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                RequestRoutingType routingType) throws IOException {
        StoreUtils.assertValidKey(key);
        this.writeMessageHeader(outputStream, VoldemortOpCode.PUT_OP_CODE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(outputStream, key);
        writeVersioned(outputStream, versioned);
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
        this.writeMessageHeader(output, VoldemortOpCode.GET_VERSION_OP_CODE, storeName, routingType);
        VoldemortNativeProtocol.writeKey(output, key);
    }
}
