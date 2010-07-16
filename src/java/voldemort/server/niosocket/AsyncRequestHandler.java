/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestDirection;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestHandlerState;
import voldemort.utils.ByteBufferBackedInputStream;
import voldemort.utils.ByteBufferBackedOutputStream;
import voldemort.utils.ByteUtils;

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

public class AsyncRequestHandler {

    private final SelectorManager manager;

    private final SocketChannel socketChannel;

    private final RequestHandlerFactory requestHandlerFactory;

    private final int socketBufferSize;

    private final int resizeThreshold;

    private final ByteBufferBackedInputStream inputStream;

    private final ByteBufferBackedOutputStream outputStream;

    private RequestHandler requestHandler;
    private long readCompletedTime;

    private StreamRequestHandler streamRequestHandler;

    private final Logger logger = Logger.getLogger(getClass());

    public AsyncRequestHandler(SelectorManager manager,
                               SocketChannel socketChannel,
                               RequestHandlerFactory requestHandlerFactory,
                               int socketBufferSize) {
        this.manager = manager;
        this.socketChannel = socketChannel;
        this.requestHandlerFactory = requestHandlerFactory;
        this.socketBufferSize = socketBufferSize;
        this.resizeThreshold = socketBufferSize * 2; // This is arbitrary...

        inputStream = new ByteBufferBackedInputStream(ByteBuffer.allocate(socketBufferSize));
        outputStream = new ByteBufferBackedOutputStream(ByteBuffer.allocate(socketBufferSize));

        if(logger.isInfoEnabled())
            logger.info("Accepting remote connection from "
                        + socketChannel.socket().getRemoteSocketAddress());
    }

    @Override
    protected void finalize() throws Throwable {
        outputStream.close();
        inputStream.close();
        super.finalize();
    }

    public SocketAddress getRemoteSocketAddress() {
        return socketChannel.socket().getRemoteSocketAddress();
    }

    public long getReceivedTime() {
        return this.readCompletedTime;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public boolean isInitialized() {
        return requestHandler != null;
    }

    public void run() {
        try {
            StreamRequestHandlerState state = handleRequest();
            if(state == StreamRequestHandlerState.COMPLETE
               || state == StreamRequestHandlerState.WRITING) {
                state = this.write();
                if(state == StreamRequestHandlerState.WRITING) {
                    manager.markForWrite(this.socketChannel);
                } else if(state == StreamRequestHandlerState.COMPLETE) {
                    manager.markForRead(this.socketChannel);
                }
            } else if(state == StreamRequestHandlerState.READING
                      || state == StreamRequestHandlerState.INCOMPLETE_READ) {
                manager.markForRead(this.socketChannel);
            }
        } catch(ClosedByInterruptException e) {
            close();
        } catch(CancelledKeyException e) {
            close();
        } catch(EOFException e) {
            close();
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
            close();
        }
    }

    public StreamRequestHandlerState read() throws IOException {
        int count = 0;

        readCompletedTime = 0;
        ByteBuffer inputBuffer = inputStream.getBuffer();
        if((count = socketChannel.read(inputBuffer)) == -1)
            throw new EOFException("EOF for " + getRemoteSocketAddress());

        if(logger.isTraceEnabled())
            traceInputBufferState(inputBuffer, "Read " + count + " bytes");

        if(count == 0) {
            return StreamRequestHandlerState.INCOMPLETE_READ;
        }
        // Take note of the position after we read the bytes. We'll need it in
        // case of incomplete reads later on down the method.
        final int position = inputBuffer.position();

        // Flip the buffer, set our limit to the current position and then set
        // the position to 0 in preparation for reading in the RequestHandler.
        inputBuffer.flip();

        // We have to do this on the first request as we don't know the protocol
        // yet.
        // We have to do this on the first request.
        if(requestHandler == null) {
            this.readCompletedTime = System.currentTimeMillis();
            return StreamRequestHandlerState.READING;
        } else if(this.streamRequestHandler != null) {
            StreamRequestHandlerState state = getStreamRequestHandlerState();
            if(state != StreamRequestHandlerState.INCOMPLETE_READ) {
                this.readCompletedTime = System.currentTimeMillis();
            }
            return state;
        } else if(requestHandler.isCompleteRequest(inputBuffer)) {
            this.readCompletedTime = System.currentTimeMillis();
            return StreamRequestHandlerState.READING;
        } else {
            // Ouch - we're missing some data for a full request, so handle that
            // and return.
            handleIncompleteRequest(inputBuffer, position);
            return StreamRequestHandlerState.INCOMPLETE_READ;
        }
    }

    private StreamRequestHandlerState getStreamRequestHandlerState() {
        try {
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            StreamRequestHandlerState state = streamRequestHandler.getRequestState(dataInputStream);
            return state;
        } catch(IOException e) {
            return StreamRequestHandlerState.INCOMPLETE_READ;
        }
    }

    public StreamRequestHandlerState handleRequest() {
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

    public StreamRequestHandlerState write() throws IOException {
        StreamRequestHandlerState state = writeBuffer();
        if(state == StreamRequestHandlerState.COMPLETE) {
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
                    // we need to send the last bunch out
                    // on the next pass
                    return null;
                }
                state = StreamRequestHandlerState.WRITING;
            }
        }
        return state;
    }

    public StreamRequestHandlerState writeBuffer() throws IOException {
        ByteBuffer outputBuffer = outputStream.getBuffer();
        if(outputBuffer.hasRemaining()) {
            // If we have data, write what we can now...
            int count = socketChannel.write(outputBuffer);

            if(logger.isTraceEnabled()) {
                logger.trace("Wrote " + count + " bytes, remaining: " + outputBuffer.remaining()
                             + " for " + getRemoteSocketAddress());
            }
        } else if(logger.isTraceEnabled()) {
            logger.trace("Wrote no bytes for " + getRemoteSocketAddress());
        }
        // If there's more to write but we didn't write it, we'll take that to
        // mean that we're done here. We don't clear or reset anything. We leave
        // our buffer state where it is and try our luck next time.
        if(outputBuffer.hasRemaining()) {
            return StreamRequestHandlerState.WRITING;
        } else {
            // If we don't have anything else to write, that means we're done
            // with the request! So clear the buffers (resizing if necessary).
            if(outputBuffer.capacity() >= resizeThreshold) {
                outputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
            } else {
                outputBuffer.clear();
            }
            return StreamRequestHandlerState.COMPLETE;
        }
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
            ByteBuffer inputBuffer = inputStream.getBuffer();
            // We need the data that's in there so far and aren't ready to write
            // anything out yet, so don't clear the input buffer or signal that
            // we're ready to write. But we do want to compact the buffer as we
            // don't want it to trigger an increase in the buffer if we don't
            // need to do so.

            // We need to do the following steps...
            //
            // a) ...figure out where we are in the buffer...
            int currentPosition = inputBuffer.position();

            // b) ...position ourselves at the start of the incomplete
            // "segment"...
            inputBuffer.position(preRequestPosition);

            // c) ...then copy the data starting from preRequestPosition's data
            // is at index 0...
            inputBuffer.compact();

            // d) ...and reset the position to be ready for the rest of the
            // reads and the limit to allow more data.
            handleIncompleteRequest(inputBuffer, currentPosition - preRequestPosition);
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

            VoldemortException error = (e instanceof VoldemortException) ? (VoldemortException) e
                                                                        : new VoldemortException(e);
            streamRequestHandler.handleError(dataOutputStream, error);
            streamRequestHandler.close(dataOutputStream);
            streamRequestHandler = null;

            resetInputBuffer();
            close();
        }

        return state;
    }

    private void resetInputBuffer() {
        ByteBuffer inputBuffer = inputStream.getBuffer();
        if(logger.isTraceEnabled())
            traceInputBufferState(inputBuffer, "About to clear read buffer");

        if(inputBuffer.capacity() >= resizeThreshold) {
            inputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
        } else {
            inputBuffer.clear();
        }
    }

    /**
     * Flips the output buffer, and lets the Selector know we're ready to write.
     * 
     * @param selectionKey
     */
    private void prepareOutputBuffer() {
        resetInputBuffer();

        outputStream.getBuffer().flip();
    }

    private void handleIncompleteRequest(ByteBuffer inputBuffer, int newPosition) {
        if(logger.isTraceEnabled())
            traceInputBufferState(inputBuffer, "Incomplete read request detected, before update");

        inputBuffer.position(newPosition);
        inputBuffer.limit(inputBuffer.capacity());

        if(logger.isTraceEnabled())
            traceInputBufferState(inputBuffer, "Incomplete read request detected, after update");

        if(!inputBuffer.hasRemaining()) {
            // We haven't read all the data needed for the request AND we
            // don't have enough data in our buffer. So expand it. Note:
            // doubling the current buffer size is arbitrary.
            inputStream.setBuffer(ByteUtils.expand(inputBuffer, inputBuffer.capacity() * 2));

            if(logger.isTraceEnabled())
                traceInputBufferState(inputStream.getBuffer(), "Expanded input buffer");
        }
    }

    public void close() {
        manager.markForClose(socketChannel);
        if(logger.isInfoEnabled())
            logger.info("Closing remote connection from "
                        + socketChannel.socket().getRemoteSocketAddress());

        try {
            socketChannel.socket().close();
        } catch(IOException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            socketChannel.close();
        } catch(IOException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

    }

    /**
     * Returns true if the request should continue.
     * 
     * @return
     */
    private StreamRequestHandlerState initRequestHandler() {
        ByteBuffer inputBuffer = inputStream.getBuffer();
        int remaining = inputBuffer.remaining();

        // Don't have enough bytes to determine the protocol yet...
        if(remaining < 3)
            return StreamRequestHandlerState.INCOMPLETE_READ;

        byte[] protoBytes = { inputBuffer.get(0), inputBuffer.get(1), inputBuffer.get(2) };

        try {
            String proto = ByteUtils.getString(protoBytes, "UTF-8");
            RequestFormatType requestFormatType = RequestFormatType.fromCode(proto);
            requestHandler = requestHandlerFactory.getRequestHandler(requestFormatType);

            if(logger.isInfoEnabled())
                logger.info("Protocol negotiated for " + getRemoteSocketAddress() + ": "
                            + requestFormatType.getDisplayName());

            // The protocol negotiation is the first request, so respond by
            // sticking the bytes in the output buffer, signaling the Selector,
            // and returning false to denote no further processing is needed.
            outputStream.getBuffer().put(ByteUtils.getBytes("ok", "UTF-8"));
            prepareOutputBuffer();

            return StreamRequestHandlerState.COMPLETE;
        } catch(IllegalArgumentException e) {
            // okay we got some nonsense. For backwards compatibility,
            // assume this is an old client who does not know how to negotiate
            RequestFormatType requestFormatType = RequestFormatType.VOLDEMORT_V0;
            requestHandler = requestHandlerFactory.getRequestHandler(requestFormatType);

            if(logger.isInfoEnabled())
                logger.info("No protocol proposal given for " + getRemoteSocketAddress()
                            + ", assuming " + requestFormatType.getDisplayName());
            return StreamRequestHandlerState.COMPLETE;
        }
    }

    private void traceInputBufferState(ByteBuffer buffer, String preamble) {
        logger.trace(preamble + " - position: " + buffer.position() + ", limit: " + buffer.limit()
                     + ", remaining: " + buffer.remaining() + ", capacity: " + buffer.capacity()
                     + " - for " + getRemoteSocketAddress());
    }
}
