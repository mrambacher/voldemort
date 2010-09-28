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

package voldemort.store.socket;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.VoldemortInterruptedException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.server.RequestRoutingType;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.Timer;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The client implementation of a socket store--translates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * 
 */
public class SocketStore implements Store<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(SocketStore.class);

    private final RequestFormatFactory requestFormatFactory = new RequestFormatFactory();

    private final String name;
    private final SocketPool pool;
    private final SocketDestination destination;
    private final RequestFormat requestFormat;
    private final RequestRoutingType requestType;

    public SocketStore(String name, SocketDestination dest, SocketPool socketPool, boolean reroute) {
        this.name = Utils.notNull(name);
        this.pool = Utils.notNull(socketPool);
        this.destination = dest;
        this.requestFormat = requestFormatFactory.getRequestFormat(dest.getRequestFormatType());
        this.requestType = RequestRoutingType.getRequestRoutingType(reroute, false);
    }

    public SocketStore(String name,
                       SocketDestination dest,
                       SocketPool socketPool,
                       RequestRoutingType requestType) {
        this.name = Utils.notNull(name);
        this.pool = Utils.notNull(socketPool);
        this.destination = dest;
        this.requestFormat = requestFormatFactory.getRequestFormat(dest.getRequestFormatType());
        this.requestType = requestType;
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    private Timer createTimer(String operation, ByteArray key) {
        return new Timer(this.name + "::" + operation + new String(key.get() + ")"),
                         this.pool.getSocketTimeout());
    }

    private VoldemortException translateException(SocketAndStreams sands,
                                                  IOException e,
                                                  String operation) {
        close(sands.getSocket());
        if(e instanceof SocketTimeoutException) {
            return new UnreachableStoreException("Timeout in " + operation + " on " + destination
                                                 + ": " + e.getMessage(), e);

        } else if(e instanceof InterruptedIOException) {
            return new VoldemortInterruptedException("Interrupt during " + operation + " on "
                                                     + destination + ": " + e.getMessage(), e);
        } else {
            return new UnreachableStoreException("Failure in " + operation + " on " + destination
                                                 + ": " + e.getMessage(), e);
        }
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Timer timer = createTimer("delete", key);
        SocketAndStreams sands = pool.checkout(destination);
        timer.checkpoint("Checkout");
        try {
            requestFormat.writeDeleteRequest(sands.getOutputStream(),
                                             name,
                                             key,
                                             version,
                                             requestType);
            sands.getOutputStream().flush();
            timer.checkpoint("Wrote request");
            return requestFormat.readDeleteResponse(sands.getInputStream());
        } catch(IOException e) {
            throw translateException(sands, e, "delete");
        } finally {
            timer.completed(logger);
            pool.checkin(destination, sands);
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Timer timer = createTimer("getAll", null);
        SocketAndStreams sands = pool.checkout(destination);
        timer.checkpoint("Checkout");
        try {
            requestFormat.writeGetAllRequest(sands.getOutputStream(), name, keys, requestType);
            sands.getOutputStream().flush();
            timer.checkpoint("Wrote request");
            return requestFormat.readGetAllResponse(sands.getInputStream());
        } catch(IOException e) {
            throw translateException(sands, e, "getAll");
        } finally {
            timer.completed(logger);
            pool.checkin(destination, sands);
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Timer timer = createTimer("get", key);
        SocketAndStreams sands = pool.checkout(destination);
        timer.checkpoint("Checkout");
        try {
            requestFormat.writeGetRequest(sands.getOutputStream(), name, key, requestType);

            sands.getOutputStream().flush();
            timer.checkpoint("Wrote request");
            return requestFormat.readGetResponse(sands.getInputStream());
        } catch(IOException e) {
            throw translateException(sands, e, "get");
        } finally {
            timer.completed(logger);
            pool.checkin(destination, sands);
        }
    }

    public Version put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        Timer timer = createTimer("put", key);
        SocketAndStreams sands = pool.checkout(destination);
        timer.checkpoint("Checkout");
        try {
            requestFormat.writePutRequest(sands.getOutputStream(),
                                          name,
                                          key,
                                          versioned,
                                          requestType);
            sands.getOutputStream().flush();
            timer.checkpoint("Wrote request");
            Version version = requestFormat.readPutResponse(sands.getInputStream());
            if(version == null) { // If the protocol is old, it might not return
                // a version
                return versioned.getVersion(); // Old protocol, return input
                // version
            } else {
                return version; // New protocol, return the protocol version
            }
        } catch(IOException e) {
            throw translateException(sands, e, "put");
        } finally {
            timer.completed(logger);
            pool.checkin(destination, sands);
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.SOCKET_POOL.equals(capability))
            return this.pool;
        else
            throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return name;
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        Timer timer = createTimer("put", key);
        SocketAndStreams sands = pool.checkout(destination);
        timer.checkpoint("Checkout");
        try {
            requestFormat.writeGetVersionRequest(sands.getOutputStream(), name, key, requestType);
            sands.getOutputStream().flush();
            timer.checkpoint("Wrote request");
            return requestFormat.readGetVersionResponse(sands.getInputStream());
        } catch(IOException e) {
            throw translateException(sands, e, "getVersions");
        } finally {
            timer.completed(logger);
            pool.checkin(destination, sands);
        }
    }
}
