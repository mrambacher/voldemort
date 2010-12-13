/*
 * Copyright 2008-2010 LinkedIn, Inc
 * 
 * Portion Copyright 2010 Nokia Corporation. All rights reserved.
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
package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import voldemort.VoldemortException;
import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientSuccessfulNodesException;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClockProtoSerializer;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class VoldemortNativeRequestHandlerV3 extends VoldemortNativeRequestHandlerV2 {

    public VoldemortNativeRequestHandlerV3(ErrorCodeMapper errorMapper, StoreRepository repository) {
        super(errorMapper, repository);
    }

    @Override
    public StreamRequestHandler handleRequest(DataInputStream inputStream,
                                              DataOutputStream outputStream) throws IOException {
        // Read and throw away the request size. It's used by isCompleteRequest
        inputStream.readInt();

        return super.handleRequest(inputStream, outputStream);
    }

    @Override
    protected boolean isCompleteRequest(final ByteBuffer buffer, DataInputStream inputStream)
            throws IOException {
        int requestSize = inputStream.readInt();

        if(requestSize > buffer.limit() || requestSize < 0) {
            return false;
            // throw new
            // Exception("Data inconsistency on delete - versionSize: "
            // + versionSize + ", position: " + buffer.position()
            // + ", limit: " + buffer.limit());
        }
        // Here we skip over the data (without reading it in) and
        // move our position to just past it.
        buffer.position(buffer.position() + requestSize);

        // If there aren't any remaining, we've "consumed" all the bytes and
        // thus have a complete request...
        boolean complete = !buffer.hasRemaining();
        return complete;
    }

    @Override
    protected void handlePut(Store<ByteArray, byte[], byte[]> store,
                             ByteArray key,
                             Versioned<byte[]> versioned,
                             byte[] transforms,
                             DataOutputStream outputStream) throws IOException {
        try {
            Version version = store.put(key, versioned, null);
            outputStream.writeShort(0);
            writeVersion(outputStream, version);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    @Override
    protected void writeVersion(DataOutputStream outputStream, Version version) throws IOException {
        byte[] bytes = VectorClockProtoSerializer.toBytes(version);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    @Override
    protected void writeVersioned(DataOutputStream outputStream, Versioned<byte[]> versioned)
            throws IOException {
        byte[] bytes = VectorClockProtoSerializer.toBytes(versioned);
        outputStream.writeInt(bytes.length);
        outputStream.write(bytes);
    }

    @Override
    protected void writeException(DataOutputStream stream, VoldemortException ex)
            throws IOException {
        super.writeException(stream, ex);
        if(ex instanceof ObsoleteVersionException) {
            ObsoleteVersionException ove = (ObsoleteVersionException) ex;
            Version version = ove.getExistingVersion();
            writeVersion(stream, version);
        } else if(ex instanceof InsufficientSuccessfulNodesException) {
            InsufficientSuccessfulNodesException isne = (InsufficientSuccessfulNodesException) ex;
            stream.writeInt(isne.getAvailable());
            stream.writeInt(isne.getRequired());
            stream.writeInt(isne.getSuccessful());
        } else if(ex instanceof InsufficientOperationalNodesException) {
            InsufficientOperationalNodesException ione = (InsufficientOperationalNodesException) ex;
            stream.writeInt(ione.getAvailable());
            stream.writeInt(ione.getRequired());
        }
    }

    @Override
    protected void handleDelete(DataInputStream inputStream,
                                DataOutputStream outputStream,
                                Store<ByteArray, byte[], byte[]> store) throws IOException {
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        Version version = readVersion(inputStream);
        handleDelete(store, key, version, outputStream);
    }
}
