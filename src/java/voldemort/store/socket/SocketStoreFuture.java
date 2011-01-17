/*
 * Copyright 2010 LinkedIn, Inc
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
package voldemort.store.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.store.UnreachableStoreException;
import voldemort.store.async.StoreFutureTask;
import voldemort.store.socket.clientrequest.ClientRequest;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class SocketStoreFuture<V> extends StoreFutureTask<V> implements ClientRequest<V> {

    private final SocketDestination destination;
    private final ClientRequestExecutorPool pool;
    private Thread thread;
    private V result;
    private VoldemortException exception;
    private boolean cancelled = false;
    private final ClientRequest<V> clientRequest;

    private ClientRequestExecutor clientRequestExecutor = null;

    public SocketStoreFuture(String operation,
                             ClientRequest<V> clientRequest,
                             SocketDestination destination,
                             ClientRequestExecutorPool pool) {
        super(operation);
        this.clientRequest = clientRequest;
        this.destination = destination;
        this.pool = pool;
        this.result = null;
        this.exception = null;
        this.thread = Thread.currentThread();
        long started = System.currentTimeMillis();
        try {
            this.clientRequestExecutor = pool.checkout(destination);
            this.started = System.nanoTime();
        } catch(VoldemortException ex) {
            this.exception = ex;
            this.markAsFailed(ex);
        }
        if(clientRequestExecutor != null) {
            clientRequestExecutor.addCheckpoint("Checkout complete");
            clientRequestExecutor.addClientRequest(this, started, pool.getSocketTimeout());
        }
    }

    public boolean cancel(boolean interruptIfRunning) {
        if(cancelled) {
            return true;
        } else if(isDone()) {
            return false;
        } else if(thread != null && interruptIfRunning) {
            thread.interrupt();
            return true;
        }
        return false;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean isComplete() {
        return isCancelled() || isDone();
    }

    public void markCompleted(Exception reason) {
        try {
            clientRequest.markCompleted(reason);
            result = call();
            super.markAsCompleted(result);
        } catch(VoldemortException ex) {
            exception = ex;
            super.markAsFailed(ex);
        } catch(IOException e) {
            clientRequestExecutor.close();
            exception = new UnreachableStoreException("Failure in " + getOperation() + " on "
                                                      + destination + ": " + e.getMessage(), e);
            super.markAsFailed(exception);
        } finally {
            this.thread = null;
            pool.checkin(destination, clientRequestExecutor);
        }
    }

    public String getName() {
        return clientRequest.getName();
    }

    public V call() throws VoldemortException, IOException {
        return clientRequest.call();
    }

    public boolean formatRequest(RequestFormat formatter, DataOutputStream outputStream) {
        return clientRequest.formatRequest(formatter, outputStream);
    }

    @Override
    public V getResult() throws VoldemortException {
        if(exception != null) {
            throw exception;
        } else {
            return result;
        }
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return clientRequest.isCompleteResponse(buffer);
    }

    public void parseResponse(DataInputStream inputStream) {
        clientRequest.parseResponse(inputStream);
    }

    public boolean hasExpired() {
        return clientRequest.hasExpired();
    }

    public void setExpirationTime(long timeout, TimeUnit units) {
        clientRequest.setExpirationTime(timeout, units);
    }
}
