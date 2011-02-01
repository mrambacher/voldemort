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

package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.utils.ByteUtils;
import voldemort.utils.SelectorManager;
import voldemort.utils.SelectorManagerWorker;
import voldemort.utils.Timer;

/**
 * ClientRequestExecutor represents a persistent link between a client and
 * server and is used by the {@link ClientRequestExecutorPool} to execute
 * {@link ClientRequest requests} for the client.
 * 
 * Instances are maintained in a pool by {@link ClientRequestExecutorPool} using
 * a checkout/checkin pattern. When an instance is checked out, the calling code
 * has exclusive access to that instance. Then the
 * {@link #addClientRequest(ClientRequest) request can be executed}.
 * 
 * @see SelectorManagerWorker
 * @see ClientRequestExecutorPool
 */

public class ClientRequestExecutor extends SelectorManagerWorker {

    private ClientRequest<?> currentRequest;
    private RequestFormat requestFormat = null;
    private final RequestFormatType requestFormatType;
    private Timer requestTimer;

    public ClientRequestExecutor(SelectorManager manager,
                                 SocketChannel socketChannel,
                                 int socketBufferSize,
                                 long socketTimeoutMs,
                                 RequestFormatType formatType) {
        super(manager, socketChannel, socketBufferSize, socketTimeoutMs);
        this.requestFormatType = formatType;
        this.requestFormat = null;
        this.requestTimer = null;
        currentRequest = null;
    }

    /**
     * Returns the socket channel associated with this request
     * 
     * @return
     */
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * Returns whether or not the requestor is still valid. A requestor is still
     * valid if the socket is not closed and if it is connected or pending
     * connection.
     * 
     * @return true if the connection is still valid and false otherwise
     */
    public boolean isValid() {
        if(isClosed()) {
            return false;
        }

        Socket s = socketChannel.socket();
        if(socketChannel.isConnectionPending()) {
            return !s.isClosed();
        } else {
            return !s.isClosed() && s.isBound() && s.isConnected();
        }
    }

    @Override
    public void addCheckpoint(String point) {
        if(requestTimer != null) {
            requestTimer.checkpoint(point);
        } else {
            super.addCheckpoint(point);
        }
    }

    /**
     * Starts the protocol negotiation with the server.
     */
    public void startNegotiateProtocol() {
        addCheckpoint("Negotiating Protocol");
        this.writeClientRequest();
    }

    /**
     * Completes the protocol negotiation with the server.
     * 
     * @param inputStream The stream containing the server response
     * @throws IOException If there was an error reading from the stream.
     */
    public void finishNegotiateProtocol(DataInputStream inputStream) throws IOException {
        byte[] responseBytes = new byte[2];
        inputStream.readFully(responseBytes);
        String result = ByteUtils.getString(responseBytes, "UTF-8");
        addCheckpoint("Negotiation Complete");
        if(result.equals("ok")) {
            this.requestFormat = RequestFormatFactory.getRequestFormat(requestFormatType);
        } else if(result.equals("no")) {
            throw new VoldemortException(requestFormatType.getDisplayName()
                                         + " is not an acceptable protcol for the server.");
        } else {
            throw new VoldemortException("Unknown server response: " + result);
        }
        // There was a request waiting for negotiation to complete
        // Copy the current timer and reset it
        // Then send the pending request
        if(this.currentRequest != null) {
            requestTimer.checkpoint(timer);
            timer.reset();
            this.writeClientRequest();
        }
    }

    private boolean timeoutHasExpired() {
        boolean hasExpired = currentRequest != null && currentRequest.hasExpired();
        if(hasExpired) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Client request associated with " + socketChannel.socket()
                            + " timed out");
        }
        return hasExpired;
    }

    /**
     * Attempt to complete the socket connection.
     * 
     * @return true if the socket is now connected, false otherwise
     * @throws IOException If an error occurs on the socket
     */
    public boolean finishConnect() throws IOException {
        try {
            SocketChannel socketChannel = getSocketChannel();
            boolean connected;
            if(socketChannel.isConnected()) {
                connected = true;
                if(logger.isDebugEnabled())
                    logger.debug("Finishing connect on a socket that is already connected ("
                                 + socketChannel.isConnectionPending() + ")"
                                 + socketChannel.socket());
            } else if(socketChannel.isConnectionPending()) {
                connected = socketChannel.finishConnect();
            } else {
                connected = false;
                if(logger.isDebugEnabled())
                    logger.debug("Not connected or pending " + socketChannel.socket());
            }
            if(connected) {
                addCheckpoint("Socket Connected");
                if(logger.isDebugEnabled())
                    logger.debug("Connected socket to "
                                 + socketChannel.socket().getRemoteSocketAddress());

                // check buffer sizes--you often don't get out what you put
                // in!
                if(socketChannel.socket().getReceiveBufferSize() != socketBufferSize)
                    logger.debug("Requested socket receive buffer size was " + socketBufferSize
                                 + " bytes but actual size is "
                                 + socketChannel.socket().getReceiveBufferSize() + " bytes.");

                if(socketChannel.socket().getSendBufferSize() != socketBufferSize)
                    logger.debug("Requested socket send buffer size was " + socketBufferSize
                                 + " bytes but actual size is "
                                 + socketChannel.socket().getSendBufferSize() + " bytes.");
            }
            return connected;
        } catch(IOException e) {
            logger.error("Failed to connect socket " + socketChannel.socket() + " after "
                         + getDuration() + " ms - " + e.getMessage(), e);
            this.completeClientRequest(e);
            close();
            throw e;
        }
    }

    /**
     * Writes the client request to the socket. This method sends either the
     * protocol negotiation or client message to the server. This method puts
     * the output request into a buffer and writes it to the server. Upon
     * completion, the selector is put into the correct state to either complete
     * the request or read the response
     * 
     * @return True if the message was successfully built and sent, false
     *         otherwise.
     */
    protected boolean writeClientRequest() {
        outputStream.getBuffer().clear();

        boolean wasSuccessful = true;
        if(requestFormat == null) {
            try {
                DataOutputStream dos = new DataOutputStream(outputStream);
                dos.write(ByteUtils.getBytes(requestFormatType.getCode(), "UTF-8"));
            } catch(IOException e) {
                wasSuccessful = false;
            }
        } else {
            wasSuccessful = currentRequest.formatRequest(requestFormat,
                                                         new DataOutputStream(outputStream));
        }
        this.resetInputBuffer();

        outputStream.getBuffer().flip();

        if(wasSuccessful) {
            try {
                if(write()) {
                    // If the write is complete, mark the channel for read
                    manager.markForRead(this.socketChannel);
                } else {
                    // If the write is not done, mark the channel for write
                    manager.markForWrite(this.socketChannel);
                }
            } catch(IOException e) {

            }
        } else {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn("Client associated with " + socketChannel.socket()
                            + " did not successfully buffer output for request");

            completeClientRequest(null);
        }
        return wasSuccessful;
    }

    /**
     * Sets the client request currently associated with this executor.
     * 
     * @param clientRequest The client request to send to the server.
     * @param started The time at which the server request is starting
     * @param expiration How long (in milliseconds) the request is allowed to
     *        take.
     */
    public synchronized void addClientRequest(ClientRequest<?> clientRequest, Timer requestTimer) {
        if(logger.isTraceEnabled())
            logger.trace("Associating client with " + socketChannel.socket());

        this.requestTimer = requestTimer;
        // timer.reset(started);
        // timer.setExpiration(expiration);
        // timer.setName(clientRequest.toString());
        currentRequest = clientRequest;
        if(this.requestFormat != null) {
            // If protocol negotiation is complete, send the request now.
            // Copy the current timer and reset it
            // Then send the pending request
            requestTimer.checkpoint(timer);
            timer.reset();
            writeClientRequest();
        }
    }

    @Override
    protected void closeInternal() {
        completeClientRequest(null);
        super.closeInternal();
    }

    @Override
    protected boolean isCompleteRequest(ByteBuffer inputBuffer, int position) throws IOException {
        if(this.requestFormat == null) {
            return true;
        } else if(timeoutHasExpired()) {
            return true;
        } else if(currentRequest.isCompleteResponse(inputBuffer)) {
            return true;
        } else {
            // Ouch - we're missing some data for a full request, so handle that
            // and return.
            handleIncompleteRequest(inputBuffer, position);
            return false;
        }
    }

    @Override
    protected void runRequest() throws IOException {
        if(timeoutHasExpired()) {

        }
        // At this point we have the full request (and it's not streaming), so
        // rewind the buffer for reading and execute the request.
        inputStream.getBuffer().rewind();

        if(logger.isTraceEnabled())
            logger.trace("Starting read for " + socketChannel.socket());

        if(this.requestFormat == null) {
            finishNegotiateProtocol(new DataInputStream(inputStream));
        } else {
            currentRequest.parseResponse(new DataInputStream(inputStream));

            // At this point we've completed a full stand-alone request. So
            // clear
            // our input buffer and prepare for outputting back to the client.
            if(logger.isTraceEnabled())
                logger.trace("Finished read for " + socketChannel.socket());
            this.manager.markForOperation(socketChannel, 0); // Mark the socket
                                                             // as
            // not in use
            completeClientRequest(null);
        }
    }

    /**
     * Null out our client request *before* calling complete because of the case
     * where complete will cause a ClientRequestExecutor check-in (in
     * SocketStore.NonblockingStoreCallbackClientRequest) and we'll end up
     * recursing back here again when close is called in which case we'll try to
     * check in the instance again which causes problems for the pool
     * maintenance.
     */

    private synchronized void completeClientRequest(Exception ex) {
        if(currentRequest == null) {
            if(socketChannel.isConnected()) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("No client associated with " + socketChannel.socket());
            }
            return;
        }

        // Sorry about this - please see the method comments...
        ClientRequest<?> local = currentRequest;
        currentRequest = null;

        local.markCompleted(ex);
        if(logger.isTraceEnabled())
            logger.trace("Marked client associated with " + socketChannel.socket() + " as complete");
    }
}
