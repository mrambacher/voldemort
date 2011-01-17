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

import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.RequestRoutingType;

public abstract class AbstractStoreClientRequest<T> extends AbstractClientRequest<T> {

    private ClientRequestFormat<T> request;

    protected final String storeName;
    protected final RequestRoutingType routingType;

    public AbstractStoreClientRequest(VoldemortOpCode request,
                                      String storeName,
                                      RequestRoutingType routingType) {
        super(request.getMethodName());
        this.routingType = routingType;
        this.storeName = storeName;
    }

    @Override
    public String toString() {
        return "Request[" + name + "/" + storeName + "]";
    }

    abstract protected ClientRequestFormat<T> getRequest(RequestFormat format);

    @Override
    protected boolean isCompleteResponseInternal(ByteBuffer buffer) {
        return request.isCompleteResponse(buffer);
    }

    @Override
    protected void formatRequestInternal(RequestFormat format, DataOutputStream outputStream)
            throws IOException {
        request = getRequest(format);
        request.writeRequest(outputStream);
    }

    @Override
    protected T parseResponseInternal(DataInputStream inputStream) throws IOException {
        return request.readResponse(inputStream);
    }
}
