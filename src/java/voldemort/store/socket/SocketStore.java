/*
 * Copyright 2008-2010 LinkedIn, Inc
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

import java.util.concurrent.Callable;

import voldemort.VoldemortException;
import voldemort.server.RequestRoutingType;
import voldemort.store.StoreCapabilityType;
import voldemort.store.async.AsynchronousCallableStore;
import voldemort.store.async.AsynchronousStore;
import voldemort.store.async.StoreFuture;
import voldemort.store.socket.clientrequest.ClientRequest;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

/**
 * The client implementation of a socket store--translates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * <p/>
 * 
 * SocketStore handles both <i>blocking</i> and <i>non-blocking</i> styles of
 * requesting. For non-blocking requests, SocketStore checks out a
 * {@link ClientRequestExecutor} instance from the
 * {@link ClientRequestExecutorPool pool} and adds an appropriate
 * {@link ClientRequest request} to be processed by the NIO thread.
 */

public class SocketStore extends AsynchronousCallableStore<ByteArray, byte[]> {

    private final ClientRequestExecutorPool pool;
    private final SocketDestination destination;

    public SocketStore(String storeName,
                       SocketDestination dest,
                       ClientRequestExecutorPool pool,
                       RequestRoutingType requestRoutingType) {
        super(new ClientRequestStore(storeName, requestRoutingType, dest.getRequestFormatType()));
        this.pool = Utils.notNull(pool);
        this.destination = dest;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.SOCKET_POOL.equals(capability))
            return this.pool;
        else
            return super.getCapability(capability);
    }

    @Override
    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    @Override
    protected <R> StoreFuture<R> submit(AsynchronousStore.Operations operation, Callable<R> task) {
        ClientRequest<R> delegate = (ClientRequest<R>) task;
        ClientRequestExecutor clientRequestExecutor = pool.checkout(destination);
        SocketStoreFuture<R> socketFuture = new SocketStoreFuture<R>(operation.name(),
                                                                     delegate,
                                                                     destination,
                                                                     pool,
                                                                     clientRequestExecutor);
        clientRequestExecutor.addClientRequest(socketFuture);
        return socketFuture;
    }
}
