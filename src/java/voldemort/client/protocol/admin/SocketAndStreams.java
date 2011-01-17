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

package voldemort.client.protocol.admin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.ByteUtils;

/**
 * A wrapper class that wraps a socket with its DataInputStream and
 * DataOutputStream
 * 
 * 
 */
public class SocketAndStreams {

    private static final int DEFAULT_BUFFER_SIZE = 1000;
    private boolean initialized;
    private final Socket socket;
    private final RequestFormatType requestFormatType;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final long createTimestamp;

    public SocketAndStreams(Socket socket, RequestFormatType requestFormatType) throws IOException {
        this(socket, DEFAULT_BUFFER_SIZE, requestFormatType);
    }

    public SocketAndStreams(Socket socket, int bufferSizeBytes, RequestFormatType type)
                                                                                       throws IOException {
        this.socket = socket;
        this.inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                       bufferSizeBytes));
        this.outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                          bufferSizeBytes));
        this.requestFormatType = type;
        this.createTimestamp = System.nanoTime();
        this.initialized = false;
    }

    @Override
    protected void finalize() throws Throwable {
        this.outputStream.close();
        this.inputStream.close();
        super.finalize();
    }

    public void close() throws IOException {
        socket.close();
    }

    public boolean isValid() {
        boolean isValid = !socket.isClosed() && socket.isBound() && socket.isConnected();
        return isValid;
    }

    /*
     * public Socket getSocket() { return socket; }
     */
    public DataInputStream getInputStream() {
        return inputStream;
    }

    public DataOutputStream getOutputStream() {
        return outputStream;
    }

    public RequestFormatType getRequestFormatType() {
        return this.requestFormatType;
    }

    /**
     * Returns the nanosecond-based timestamp of when this socket was created.
     * 
     * @return Nanosecond-based timestamp of socket creation
     * 
     * @see SocketResourceFactory#validate(SocketDestination, SocketAndStreams)
     */

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void negotiateProtocol() throws IOException {
        if(!initialized) {
            int version = negotiateProtocol(requestFormatType.getCode());
            if(version == requestFormatType.getVersion()) {
                initialized = true;
            }
        }
    }

    public int negotiateProtocol(String protocol) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        byte[] proposal = ByteUtils.getBytes(protocol, "UTF-8");
        outputStream.write(proposal);
        outputStream.flush();
        DataInputStream inputStream = getInputStream();
        byte[] responseBytes = new byte[2];
        inputStream.readFully(responseBytes);
        String response = ByteUtils.getString(responseBytes, "UTF-8");
        if(response.equals("ok")) {
            return requestFormatType.getVersion();
        } else if(response.equals("no")) {
            throw new VoldemortException(requestFormatType.getDisplayName()
                                         + " is not an acceptable protocol for the server.");
        } else {
            try {
                return Integer.parseInt(response);
            } catch(Exception e) {
                throw new VoldemortException("Unknown server response: " + response);
            }
        }
    }
}
