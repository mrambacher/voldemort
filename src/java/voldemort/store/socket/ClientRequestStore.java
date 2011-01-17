/*
 * Copyright 2008-2010 LinkedIn, Inc
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
package voldemort.store.socket;

import java.util.List;
import java.util.Map;

import voldemort.server.RequestRoutingType;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.async.CallableStore;
import voldemort.store.socket.clientrequest.ClientRequest;
import voldemort.store.socket.clientrequest.DeleteClientRequest;
import voldemort.store.socket.clientrequest.GetAllClientRequest;
import voldemort.store.socket.clientrequest.GetClientRequest;
import voldemort.store.socket.clientrequest.GetVersionsClientRequest;
import voldemort.store.socket.clientrequest.PutClientRequest;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ClientRequestStore implements CallableStore<ByteArray, byte[], byte[]> {

    private final String storeName;
    private final RequestRoutingType routingType;

    public ClientRequestStore(String storeName, RequestRoutingType requestRoutingType) {
        this.storeName = Utils.notNull(storeName);
        this.routingType = requestRoutingType;
    }

    public ClientRequest<Boolean> callDelete(ByteArray key, Version version) {
        StoreUtils.assertValidKey(key);
        return new DeleteClientRequest(storeName, key, version, routingType);
    }

    public ClientRequest<List<Versioned<byte[]>>> callGet(ByteArray key, byte[] transform) {
        StoreUtils.assertValidKey(key);
        return new GetClientRequest(storeName, key, transform, routingType);
    }

    public ClientRequest<Map<ByteArray, List<Versioned<byte[]>>>> callGetAll(Iterable<ByteArray> keys,
                                                                             Map<ByteArray, byte[]> transforms) {
        StoreUtils.assertValidKeys(keys);
        return new GetAllClientRequest(storeName, keys, transforms, routingType);
    }

    public ClientRequest<List<Version>> callGetVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        return new GetVersionsClientRequest(storeName, key, routingType);
    }

    public ClientRequest<Version> callPut(ByteArray key, Versioned<byte[]> value, byte[] transform) {
        StoreUtils.assertValidKey(key);
        return new PutClientRequest(storeName, key, value, transform, routingType);
    }

    public void close() {
        // Do nothing
    }

    public String getName() {
        return this.storeName;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }
}
