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

package voldemort.client.protocol;

import voldemort.client.protocol.pb.ProtoBuffClientRequestFormat;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormat;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormatV2;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormatV3;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormatV4;

/**
 * A factory for producing the appropriate client request format given a
 * {@link voldemort.client.protocol.RequestFormatType}
 * 
 * 
 */
public class RequestFormatFactory {

    /**
     * Returns a request format object of the appropriate type for the input
     * type
     * 
     * @param type The type of the request to return
     * @return The request object represented by that type
     */
    public static RequestFormat getRequestFormat(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT_V1:
                return new VoldemortNativeClientRequestFormat();
            case VOLDEMORT_V2:
                return new VoldemortNativeClientRequestFormatV2();
            case VOLDEMORT_V3:
                return new VoldemortNativeClientRequestFormatV3();
            case VOLDEMORT_V4:
                return new VoldemortNativeClientRequestFormatV4();
            case PROTOCOL_BUFFERS:
                return new ProtoBuffClientRequestFormat();
            default:
                throw new IllegalArgumentException("Unknown request format type: " + type);
        }
    }

}
