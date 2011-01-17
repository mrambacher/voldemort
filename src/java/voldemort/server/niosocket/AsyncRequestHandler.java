/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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

package voldemort.server.niosocket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestDirection;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestHandlerState;
import voldemort.utils.ByteUtils;
import voldemort.utils.SelectorManager;
import voldemort.utils.SelectorManagerWorker;

/**
 * AsyncRequestHandler manages a Selector, SocketChannel, and RequestHandler
 * implementation. At the point that the run method is invoked, the Selector
 * with which the (socket) Channel has been registered has notified us that the
 * socket has data to read or write.
 * <p/>
 * The bulk of the complexity in this class surrounds partial reads and writes,
 * as well as determining when all the data needed for the request has been
 * read.
 * 
 * 
 * @see voldemort.server.protocol.RequestHandler
 */

public class AsyncRequestHandler extends SelectorManagerWorker {

    private final RequestHandlerFactory requestHandlerFactory;

    private RequestHandler requestHandler;

    private StreamRequestHandler streamRequestHandler;
    private long readStartedTime;

    /**
     * Creates a new request handler for the NIO service
     * 
     * @param manager The SelectorManager this request is associated with
     * @param socketChannel The socketChannel this request is associated with
     * @param requestHandlerFactory The factory for handling requests
     * @param socketBufferSize The size of the socket buffer
     * @param requestTimeoutMs The maximum time requests are expected to take
     */
    public AsyncRequestHandler(SelectorManager manager,
                               SocketChannel socketChannel,
                               RequestHandlerFactory requestHandlerFactory,
                               int socketBufferSize,
                               long requestTimeoutMs) {
        super(manager, socketChannel, socketBufferSize, requestTimeoutMs);
        this.requestHandlerFactory = requestHandlerFactory;
        this.readStartedTime = 0;
    }

    /**
     * Gets the state of the request handler This method is used to determine if
     * a request is complete or not
     * 
     * @return The state of the streaming request
     */
    private StreamRequestHandlerState getStreamRequestHandlerState() {
        try {
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            StreamRequestHandlerState state = streamRequestHandler.getRequestState(dataInputStream);
            return state;
        } catch(IOException e) {
            return StreamRequestHandlerState.INCOMPLETE_READ;
        }
    }

    /**
     * Marks the request as complete. Used to note how long a request took to
     * complete
     */
    @Override
    protected void markCompleted(String message) {
        readStartedTime = 0;
        super.markCompleted(message);
    }

    /**
     * Reads a request from the input stream If this is the first read (since
     * the request was last completed), the timer is reset.
     */
    @Override
    public boolean read() throws IOException {
        if(readStartedTime == 0) {
            readStartedTime = System.currentTimeMillis();
            timer.reset();
        }
        return super.read();
    }

    /**
     * Determines if the inputBuffer represents a complete request or not
     */
    @Override
    protected boolean isCompleteRequest(ByteBuffer inputBuffer, int position) throws IOException {

        // We have to do this on the first request as we don't know the protocol
        // yet.
        if(requestHandler == null) {
            return true;
        } else if(this.streamRequestHandler != null) {
            // We are processing a streaming request. Check its state
            StreamRequestHandlerState state = getStreamRequestHandlerState();
            return (state != StreamRequestHandlerState.INCOMPLETE_READ);
        } else if(requestHandler.isCompleteRequest(inputBuffer)) {
            return true;
        } else {
            // Ouch - we're missing some data for a full request, so handle that
            // and return.
            handleIncompleteRequest(inputBuffer, position);
            return false;
        }

    }

    /**
     * Runs the request and sends the response.
     */
    @Override
    protected void runRequest() throws IOException {
        addCheckpoint("Handling request");
        StreamRequestHandlerState state = handleRequest();
        addCheckpoint("Handled request");
        if(state == StreamRequestHandlerState.COMPLETE
           || state == StreamRequestHandlerState.WRITING) {
            if(this.write()) { // If the write is complete, mark the channel for
                               // read
                markCompleted("");
                manager.markForRead(this.socketChannel);
            } else { // If the write is not done, mark the channel for write
                manager.markForWrite(this.socketChannel);
            }
        } else if(state == StreamRequestHandlerState.READING
                  || state == StreamRequestHandlerState.INCOMPLETE_READ) {
            manager.markForRead(this.socketChannel);
        }
    }

    /**
     * Handles the request from the client. This may involve the negotiating a
     * protocol, starting a new request, or handling the continuation of a
     * streaming request.
     * 
     * @return
     */
    protected StreamRequestHandlerState handleRequest() {
        try {
            if(this.requestHandler == null) {
                return initRequestHandler();
            } else if(this.streamRequestHandler != null) {
                // We're continuing an existing streaming request from our last
                // pass
                // through. So handle it and return.
                // In the case of a StreamRequestHandler, we handle that
                // separately (attempting to process multiple "segments").
                StreamRequestHandlerState state = handleStreamRequest();
                return state;
            } else {
                // If we have the full request, flip the buffer for reading
                // and execute the request
                ByteBuffer inputBuffer = inputStream.getBuffer();
                inputBuffer.rewind();

                if(logger.isDebugEnabled())
                    logger.debug("Starting execution for " + getRemoteSocketAddress());
                streamRequestHandler = requestHandler.handleRequest(new DataInputStream(inputStream),
                                                                    new DataOutputStream(outputStream));
                if(streamRequestHandler != null) {
                    // In the case of a StreamRequestHandler, we handle that
                    // separately (attempting to process multiple "segments").
                    StreamRequestHandlerState state = handleStreamRequest();
                    return state;
                }
                if(logger.isDebugEnabled())
                    logger.debug("Finished execution for " + getRemoteSocketAddress());
                // We've written to the buffer in the handleRequest
                // invocation,so
                // we're done with the input and can reset/resize, flip the
                // output
                // buffer, and let the Selector know we're ready to write.
                resetInputBuffer();
            }
            outputStream.getBuffer().flip();
            return StreamRequestHandlerState.COMPLETE;
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
            return null;
        }
    }

    /**
     * Writes the request to the socketChannel. This method overrides the base
     * class to support streaming requests.
     */
    @Override
    public boolean write() throws IOException {
        boolean isComplete = super.write();
        if(isComplete) {
            if(streamRequestHandler != null
               && streamRequestHandler.getDirection() == StreamRequestDirection.WRITING) {
                // In the case of streaming writes, it's possible we can process
                // another segment of the stream. We process streaming writes
                // this
                // way because there won't be any other notification for us to
                // do
                // work as we won't be notified via reads.
                if(logger.isTraceEnabled())
                    logger.trace("Request is streaming for " + getRemoteSocketAddress());
                StreamRequestHandlerState completed = handleStreamRequest();
                if(completed == null) {
                    // Ignore the state of the stream request (except on error)
                    // as
                    // we need to send the last bunch out on the next pass
                } else {
                    isComplete = false;
                }
            }
        }
        return isComplete;
    }

    private StreamRequestHandlerState handleStreamRequest() throws IOException {
        // You are not expected to understand this.
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        // We need to keep track of the last known starting index *before* we
        // attempt to service the next segment. This is needed in case of
        // partial reads so that we can revert back to this point.
        int preRequestPosition = inputStream.getBuffer().position();

        StreamRequestHandlerState state = handleStreamRequestInternal(dataInputStream,
                                                                      dataOutputStream);

        if(state == StreamRequestHandlerState.READING) {
            // We've read our request and handled one segment, but we aren't
            // ready to write anything just yet as we're streaming reads from
            // the client. So let's keep executing segments as much as we can
            // until we're no longer reading anything.
            do {
                preRequestPosition = inputStream.getBuffer().position();
                state = handleStreamRequestInternal(dataInputStream, dataOutputStream);
            } while(state == StreamRequestHandlerState.READING);
        } else if(state == StreamRequestHandlerState.WRITING) {
            // We've read our request and written one segment, but we're still
            // ready to stream writes to the client. So let's keep executing
            // segments as much as we can until we're there's nothing more to do
            // or until we blow past our buffer.
            do {
                state = handleStreamRequestInternal(dataInputStream, dataOutputStream);
            } while(state == StreamRequestHandlerState.WRITING && !outputStream.wasExpanded());

            if(state != StreamRequestHandlerState.COMPLETE) {
                // We've read our request and are ready to start streaming
                // writes to the client.
                prepareOutputBuffer();
            }
        }

        if(state == null) {
            // We got an error...
        } else if(state == StreamRequestHandlerState.INCOMPLETE_READ) {
            // We need the data that's in there so far and aren't ready to write
            // anything out yet, so don't clear the input buffer or signal that
            // we're ready to write. But we do want to compact the buffer as we
            // don't want it to trigger an increase in the buffer if we don't
            // need to do so.

            // We need to do the following steps...
            //
            // a) ...figure out where we are in the buffer...
            int currentPosition = inputStream.getBuffer().position();

            // b) ...position ourselves at the start of the incomplete
            // "segment"...
            inputStream.getBuffer().position(preRequestPosition);

            // c) ...then copy the data starting from preRequestPosition's data
            // is at index 0...
            inputStream.getBuffer().compact();

            // d) ...and reset the position to be ready for the rest of the
            // reads and the limit to allow more data.
            handleIncompleteRequest(inputStream.getBuffer(), currentPosition - preRequestPosition);
        } else if(state == StreamRequestHandlerState.COMPLETE) {
            streamRequestHandler.close(dataOutputStream);
            streamRequestHandler = null;

            // Treat this as a normal request. Assume that all completed
            // requests want to write something back to the client.
            prepareOutputBuffer();
        }
        return state;
    }

    private StreamRequestHandlerState handleStreamRequestInternal(DataInputStream dataInputStream,
                                                                  DataOutputStream dataOutputStream)
            throws IOException {
        StreamRequestHandlerState state = null;

        try {
            if(logger.isTraceEnabled())
                traceInputBufferState(inputStream.getBuffer(), "Before streaming request handler");

            state = streamRequestHandler.handleRequest(dataInputStream, dataOutputStream);

            if(logger.isTraceEnabled())
                traceInputBufferState(inputStream.getBuffer(), "After streaming request handler");
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);

            VoldemortException error = e instanceof VoldemortException ? (VoldemortException) e
                                                                      : new VoldemortException(e);
            streamRequestHandler.handleError(dataOutputStream, error);
            streamRequestHandler.close(dataOutputStream);
            streamRequestHandler = null;

            prepareOutputBuffer();

            close();
        }

        return state;
    }

    /**
     * Handles protocol negiotation of the initial request
     * 
     * @return complete if the read was complete and incomplete otherwise.
     */

    private StreamRequestHandlerState initRequestHandler() {
        ByteBuffer inputBuffer = inputStream.getBuffer();
        int remaining = inputBuffer.remaining();

        // Don't have enough bytes to determine the protocol yet...
        if(remaining < 3)
            return StreamRequestHandlerState.INCOMPLETE_READ;

        try {
            byte[] protoBytes = new byte[] { inputBuffer.get(0), inputBuffer.get(1) };
            byte[] versionBytes = { inputBuffer.get(2) };

            String proto = ByteUtils.getString(protoBytes, "UTF-8");
            int version = Integer.parseInt(ByteUtils.getString(versionBytes, "UTF-8"));
            RequestFormatType requestFormat = RequestFormatType.fromCode(proto, version);
            requestHandler = requestHandlerFactory.getRequestHandler(requestFormat);

            if(logger.isInfoEnabled())
                logger.info("Protocol negotiated for " + socketChannel.socket() + ": "
                            + requestFormat.getDisplayName());

            // The protocol negotiation is the first request, so respond by
            // sticking the bytes in the output buffer, signaling the Selector,
            // and returning false to denote no further processing is needed.
            if(version == requestFormat.getVersion()) {
                outputStream.getBuffer().put(ByteUtils.getBytes("ok", "UTF-8"));
            } else {
                outputStream.getBuffer().put(ByteUtils.getBytes(requestFormat.getVersionAsString(),
                                                                "UTF-8"));
            }
            prepareOutputBuffer();
            return StreamRequestHandlerState.COMPLETE;
        } catch(IllegalArgumentException e) {
            outputStream.getBuffer().put(ByteUtils.getBytes("no", "UTF-8"));
            return StreamRequestHandlerState.COMPLETE;
        }
    }

}
