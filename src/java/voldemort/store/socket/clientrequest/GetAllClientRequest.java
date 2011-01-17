/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.socket.clientrequest;

import java.util.List;
import java.util.Map;

import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetAllClientRequest extends
        AbstractStoreClientRequest<Map<ByteArray, List<Versioned<byte[]>>>> {

    private final Iterable<ByteArray> keys;
    private final Map<ByteArray, byte[]> transforms;

    public GetAllClientRequest(String storeName,
                               Iterable<ByteArray> keys,
                               Map<ByteArray, byte[]> transforms,
                               RequestRoutingType routingType) {
        super(VoldemortOpCode.GET_ALL, storeName, routingType);
        this.keys = keys;
        this.transforms = transforms;
    }

    @Override
    protected ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>> getRequest(RequestFormat format) {
        return format.createGetAllRequest(storeName, keys, transforms, routingType);
    }

}
