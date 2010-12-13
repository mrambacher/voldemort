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
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import voldemort.client.protocol.RequestFormat;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class PutClientRequest extends AbstractStoreClientRequest<Version> {

    private final ByteArray key;

    private final Versioned<byte[]> versioned;

    private final byte[] transforms;

    public PutClientRequest(String storeName,
                            RequestFormat requestFormat,
                            RequestRoutingType requestRoutingType,
                            ByteArray key,
                            Versioned<byte[]> versioned,
                            byte[] transforms) {
        super(storeName, requestFormat, requestRoutingType);
        this.key = key;
        this.versioned = versioned;
        this.transforms = transforms;
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return requestFormat.isCompletePutResponse(buffer);
    }

    @Override
    protected void formatRequestInternal(DataOutputStream outputStream) throws IOException {
        requestFormat.writePutRequest(outputStream,
                                      storeName,
                                      key,
                                      versioned,
                                      transforms,
                                      requestRoutingType);
    }

    @Override
    protected Version parseResponseInternal(DataInputStream inputStream) throws IOException {
        Version result = requestFormat.readPutResponse(inputStream);
        if(result != null) {
            return result;
        } else {
            return versioned.getVersion();
        }
    }

}
