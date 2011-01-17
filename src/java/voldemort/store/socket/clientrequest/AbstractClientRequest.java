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
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.store.UnreachableStoreException;

/**
 * AbstractClientRequest implements ClientRequest to provide some basic
 * mechanisms that most implementations will need.
 * 
 * @param <T> Return type
 */

public abstract class AbstractClientRequest<T> implements ClientRequest<T> {

    private T result;

    private Exception error;

    private volatile boolean isComplete = false;

    private volatile boolean isParsed = false;

    private long expiration = -1;

    protected final String name;

    protected AbstractClientRequest(String name) {
        this.name = name;
    }

    protected abstract void formatRequestInternal(RequestFormat format,
                                                  DataOutputStream outputStream) throws IOException;

    protected abstract T parseResponseInternal(DataInputStream inputStream) throws IOException;

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Request[" + name + "]";
    }

    public boolean formatRequest(RequestFormat format, DataOutputStream outputStream) {
        try {
            formatRequestInternal(format, outputStream);
        } catch(IOException e) {
            error = e;
            return false;
        } catch(VoldemortException e) {
            error = e;
            return false;
        }

        return true;
    }

    public void parseResponse(DataInputStream inputStream) {
        try {
            result = parseResponseInternal(inputStream);
        } catch(IOException e) {
            error = e;
        } catch(VoldemortException e) {
            error = e;
        } finally {
            isParsed = true;
        }
    }

    public T call() throws VoldemortException, IOException {
        if(!isComplete)
            throw new IllegalStateException("Client response not complete, cannot determine result");
        if(error != null) {
            if(error instanceof IOException) {
                throw (IOException) error;
            } else if(error instanceof VoldemortException) {
                throw (VoldemortException) error;
            } else {
                throw new VoldemortException(error);
            }
        }
        if(!isParsed)
            throw new UnreachableStoreException("Client response not read/parsed, cannot determine result");

        return result;
    }

    abstract protected boolean isCompleteResponseInternal(ByteBuffer buffer);

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return isCompleteResponseInternal(buffer);
    }

    public void markCompleted(Exception ex) {
        if(!isComplete) {
            isComplete = true;
            if(ex != null) {
                this.error = ex;
            }
        }
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void setExpirationTime(long timeout, TimeUnit units) {
        if(timeout > 0) {
            expiration = System.nanoTime() + units.convert(timeout, TimeUnit.NANOSECONDS);
        } else {
            expiration = -1;
        }
    }

    public boolean hasExpired() {
        if(isComplete() || expiration <= 0) {
            return false;
        } else if(System.nanoTime() <= expiration) {
            return false;
        } else {
            return true;
        }
    }

}
