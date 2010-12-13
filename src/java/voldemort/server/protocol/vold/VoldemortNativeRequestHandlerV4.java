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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.protocol.vold.VoldemortNativeProtocol;
import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Server-side request handler for voldemort native client protocol
 * 
 * 
 */
public class VoldemortNativeRequestHandlerV4 extends VoldemortNativeRequestHandlerV3 {

    public VoldemortNativeRequestHandlerV4(ErrorCodeMapper errorMapper, StoreRepository repository) {
        super(errorMapper, repository);
    }

    private byte[] readTransforms(DataInputStream inputStream) throws IOException {
        int size = inputStream.readInt();
        if(size == 0)
            return null;
        byte[] transforms = new byte[size];
        inputStream.readFully(transforms);
        return transforms;
    }

    @Override
    protected void handleGet(DataInputStream inputStream,
                             DataOutputStream outputStream,
                             Store<ByteArray, byte[], byte[]> store) throws IOException {
        byte[] transforms = null;
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        if(inputStream.readBoolean())
            transforms = readTransforms(inputStream);
        handleGet(store, key, transforms, outputStream);

    }

    @Override
    protected void handleGetAll(DataInputStream inputStream,
                                DataOutputStream outputStream,
                                Store<ByteArray, byte[], byte[]> store) throws IOException {
        // read keys
        int numKeys = inputStream.readInt();
        List<ByteArray> keys = new ArrayList<ByteArray>(numKeys);
        for(int i = 0; i < numKeys; i++)
            keys.add(VoldemortNativeProtocol.readKey(inputStream));

        Map<ByteArray, byte[]> transforms = null;
        if(inputStream.readBoolean()) {
            int size = inputStream.readInt();
            transforms = new HashMap<ByteArray, byte[]>(size);
            for(int i = 0; i < size; i++) {
                transforms.put(VoldemortNativeProtocol.readKey(inputStream),
                               readTransforms(inputStream));
            }
        }
        handleGetAll(store, keys, transforms, outputStream);
    }

    @Override
    protected void handlePut(DataInputStream inputStream,
                             DataOutputStream outputStream,
                             Store<ByteArray, byte[], byte[]> store) throws IOException {
        ByteArray key = VoldemortNativeProtocol.readKey(inputStream);
        Versioned<byte[]> versioned = VoldemortNativeProtocol.readVersioned(inputStream);
        byte[] transforms = null;
        if(inputStream.readBoolean()) {
            transforms = readTransforms(inputStream);
        }
        handlePut(store, key, versioned, transforms, outputStream);
    }
}
