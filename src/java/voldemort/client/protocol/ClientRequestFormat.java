/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.client.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import voldemort.VoldemortException;

/**
 * A request object that can be sent from a client to a server using different
 * encodings. Different implementations of this class may have different
 * serialization formats for the same sets of arguments
 */
public interface ClientRequestFormat<T> {

    /**
     * This method serializes the request and sends it over the output stream.
     * 
     * @param outputStream Write the request to this output stream
     */

    public boolean writeRequest(DataOutputStream outputStream) throws IOException;

    /**
     * isCompleteResponse determines if the response that the received thus far
     * is inclusive of the entire response.
     * 
     * @param buffer ByteBuffer containing the data received thus far
     * 
     * @return True if the buffer contains the complete response, false if it
     *         only includes part of the response.
     */
    public boolean isCompleteResponse(ByteBuffer buffer);

    /**
     * Parses the response from the server to turn it into a result. This method
     * will return the result or an exception
     * 
     * @param inputStream InputStream from which to read the response
     * @throws IOException if the data cannot be read
     * @throws VoldemortException if an exception is returned from the server.
     */
    public T readResponse(DataInputStream inputStream) throws IOException;
}
