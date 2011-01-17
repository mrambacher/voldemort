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

package voldemort.store.http;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;

import voldemort.VoldemortException;
import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.server.RequestRoutingType;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A remote store client that transmits operations via HTTP and interacts with
 * the VoldemortHttpServer.
 * 
 */
public class HttpStore implements Store<ByteArray, byte[], byte[]> {

    private final String storeName;
    private final HttpClient httpClient;
    private final RequestFormat requestFormat;
    private final String storeUrl;
    private final RequestRoutingType routingType;

    public HttpStore(String storeName,
                     String host,
                     int port,
                     HttpClient client,
                     RequestFormat format,
                     boolean reroute) {
        this.storeName = storeName;
        this.httpClient = client;
        this.requestFormat = format;
        routingType = RequestRoutingType.getRequestRoutingType(reroute, false);
        this.storeUrl = "http://" + host + ":" + port + "/stores";
    }

    protected <V> V processRequest(ClientRequestFormat<V> request) throws VoldemortException {
        PostMethod method = null;
        try {
            method = new PostMethod(this.storeUrl);
            ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
            request.writeRequest(new DataOutputStream(outputBytes));
            DataInputStream input = executeRequest(method, outputBytes);
            return request.readResponse(input);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        } finally {
            if(method != null)
                method.releaseConnection();
        }

    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return this.processRequest(requestFormat.createDeleteRequest(storeName,
                                                                     key,
                                                                     version,
                                                                     routingType));
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        ClientRequestFormat<List<Versioned<byte[]>>> request = requestFormat.createGetRequest(storeName,
                                                                                              key,
                                                                                              transforms,
                                                                                              routingType);
        return this.processRequest(request);
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return this.processRequest(requestFormat.createGetAllRequest(storeName,
                                                                     keys,
                                                                     transforms,
                                                                     routingType));
    }

    public Version put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Version version = this.processRequest(requestFormat.createPutRequest(storeName,
                                                                             key,
                                                                             versioned,
                                                                             transforms,
                                                                             routingType));
        if(version != null) {
            return version;
        } else {
            return versioned.getVersion();
        }
    }

    private DataInputStream executeRequest(PostMethod method, ByteArrayOutputStream output) {
        try {
            method.setRequestEntity(new ByteArrayRequestEntity(output.toByteArray()));
            int response = httpClient.executeMethod(method);
            if(response != HttpURLConnection.HTTP_OK)
                throw new UnreachableStoreException("HTTP request to store " + storeName
                                                    + " returned status code " + response + " "
                                                    + method.getStatusText());
            return new DataInputStream(method.getResponseBodyAsStream());
        } catch(HttpException e) {
            throw new VoldemortException(e);
        } catch(IOException e) {
            throw new UnreachableStoreException("Could not connect to " + storeUrl + " for "
                                                + storeName, e);
        }
    }

    public void close() {}

    public String getName() {
        return storeName;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        return this.processRequest(requestFormat.createGetVersionsRequest(storeName,
                                                                          key,
                                                                          routingType));
    }
}
