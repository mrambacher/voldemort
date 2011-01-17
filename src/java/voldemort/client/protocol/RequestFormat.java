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

import java.util.List;
import java.util.Map;

import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Abstracts the serialization mechanism used to write a client request. The
 * companion class on the server side is
 * {@link voldemort.server.protocol.RequestHandler}
 * 
 * 
 */
public interface RequestFormat {

    /**
     * Creates a Get Request for sending to the server
     * 
     * @param storeName The name of the store
     * @param key The key being retrieved
     * @param transforms Transformations for the returned value
     * @param routing How the request should be routed
     * @return The client request
     */
    public ClientRequestFormat<List<Versioned<byte[]>>> createGetRequest(String storeName,
                                                                         ByteArray key,
                                                                         byte[] transforms,
                                                                         RequestRoutingType routing);

    /**
     * Creates a Get Version Request for sending to the server
     * 
     * @param storeName The name of the store
     * @param key The key being retrieved
     * @param routing How the request should be routed
     * @return The client request
     */
    public ClientRequestFormat<List<Version>> createGetVersionsRequest(String storeName,
                                                                       ByteArray key,
                                                                       RequestRoutingType routing);

    /**
     * Creates a Get All Request for sending to the server
     * 
     * @param storeName The name of the store
     * @param keys The key being retrieved
     * @param transforms Transformations for the returned values
     * @param routing How the request should be routed
     * @return The client request
     */
    public ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>> createGetAllRequest(String storeName,
                                                                                            Iterable<ByteArray> key,
                                                                                            Map<ByteArray, byte[]> transforms,
                                                                                            RequestRoutingType routing);

    /**
     * Creates a Put Request for sending to the server
     * 
     * @param storeName The name of the store
     * @param keys The key being updated
     * @param value The value being updated
     * @param transforms Transformations for the updated value
     * @param routing How the request should be routed
     * @return The client request
     */
    public ClientRequestFormat<Version> createPutRequest(String storeName,
                                                         ByteArray key,
                                                         Versioned<byte[]> value,
                                                         byte[] transforms,
                                                         RequestRoutingType routing);

    /**
     * Creates a Delete Request for sending to the server
     * 
     * @param storeName The name of the store
     * @param keys The key being delete
     * @param version The version being deleted
     * @param routing How the request should be routed
     * @return The client request
     */
    public ClientRequestFormat<Boolean> createDeleteRequest(String storeName,
                                                            ByteArray key,
                                                            Version version,
                                                            RequestRoutingType routing);
}
