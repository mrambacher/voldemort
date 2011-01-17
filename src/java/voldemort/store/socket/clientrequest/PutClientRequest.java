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

import java.io.DataInputStream;
import java.io.IOException;

import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class PutClientRequest extends AbstractStoreClientRequest<Version> {

    private final ByteArray key;

    private final Versioned<byte[]> versioned;

    private final byte[] transforms;

    public PutClientRequest(String storeName,
                            ByteArray key,
                            Versioned<byte[]> versioned,
                            byte[] transforms,
                            RequestRoutingType routingType) {
        super(VoldemortOpCode.PUT, storeName, routingType);
        this.key = key;
        this.versioned = versioned;
        this.transforms = transforms;
    }

    @Override
    public String toString() {
        return "Request[" + name + "/" + storeName + "(" + new String(key.get()) + ")]";
    }

    @Override
    protected ClientRequestFormat<Version> getRequest(RequestFormat format) {
        return format.createPutRequest(storeName, key, versioned, transforms, routingType);
    }

    @Override
    protected Version parseResponseInternal(DataInputStream inputStream) throws IOException {
        Version result = super.parseResponseInternal(inputStream);
        if(result != null) {
            return result;
        } else {
            return versioned.getVersion();
        }
    }
}
