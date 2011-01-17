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

import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetClientRequest extends AbstractStoreClientRequest<List<Versioned<byte[]>>> {

    private final ByteArray key;
    private final byte[] transforms;

    public GetClientRequest(String storeName,
                            ByteArray key,
                            byte[] transforms,
                            RequestRoutingType routingType) {
        super(VoldemortOpCode.GET, storeName, routingType);
        this.key = key;
        this.transforms = transforms;
    }

    @Override
    public String toString() {
        return "Request[" + name + "/" + storeName + "(" + new String(key.get()) + ")]";
    }

    @Override
    protected ClientRequestFormat<List<Versioned<byte[]>>> getRequest(RequestFormat format) {
        return format.createGetRequest(storeName, key, transforms, routingType);
    }
}
