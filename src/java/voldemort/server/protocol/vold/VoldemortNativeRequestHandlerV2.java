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
import java.io.IOException;

import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;

public class VoldemortNativeRequestHandlerV2 extends VoldemortNativeRequestHandler {

    public VoldemortNativeRequestHandlerV2(ErrorCodeMapper errorMapper, StoreRepository repository) {
        super(errorMapper, repository);
    }

    @Override
    protected void checkCompleteRequestHeader(DataInputStream inputStream) throws IOException {
        // Read the store name in, but just to skip the bytes.
        inputStream.readUTF();

        // Read the routing type byte.
        inputStream.readByte();
    }

    @Override
    protected RequestRoutingType getRoutingType(DataInputStream inputStream) throws IOException {
        int routingTypeCode = inputStream.readByte();
        RequestRoutingType routingType = RequestRoutingType.getRequestRoutingType(routingTypeCode);
        return routingType;
    }
}
