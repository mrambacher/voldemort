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

package voldemort.utils;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * SelectorManagerWorker manages a Selector, SocketChannel, and IO streams
 * implementation. At the point that the run method is invoked, the Selector
 * with which the (socket) Channel has been registered has notified us that the
 * socket has data to read or write.
 * <p/>
 * The bulk of the complexity in this class surrounds partial reads and writes,
 * as well as determining when all the data needed for the request has been
 * read.
 */

public abstract class SelectorManagerWorker implements Runnable {

    protected Timer timer;
    protected final SelectorManager manager;

    protected final SocketChannel socketChannel;

    protected final int socketBufferSize;

    private final int resizeThreshold;

    protected final ByteBufferBackedInputStream inputStream;

    protected final ByteBufferBackedOutputStream outputStream;

    protected final long createTimestamp;

    protected final AtomicBoolean isClosed;

    protected final Logger logger = Logger.getLogger(getClass());

    public SelectorManagerWorker(SelectorManager manager,
                                 SocketChannel socketChannel,
                                 int socketBufferSize,
                                 long requestTimeoutMs) {
        this.manager = manager;
        this.socketChannel = socketChannel;
        this.socketBufferSize = socketBufferSize;
        this.resizeThreshold = socketBufferSize * 2; // This is arbitrary...
        this.inputStream = new ByteBufferBackedInputStream(ByteBuffer.allocate(socketBufferSize));
        this.outputStream = new ByteBufferBackedOutputStream(ByteBuffer.allocate(socketBufferSize));
        this.createTimestamp = System.nanoTime();
        this.isClosed = new AtomicBoolean(false);
        this.timer = new Timer("Request[" + socketChannel.socket().getRemoteSocketAddress() + "]",
                               requestTimeoutMs);

        if(logger.isDebugEnabled())
            logger.debug("Accepting remote connection from " + socketChannel.socket());
    }

    public SocketAddress getRemoteSocketAddress() {
        return socketChannel.socket().getRemoteSocketAddress();
    }

    public void addCheckpoint(String point) {
        timer.checkpoint(point);
    }

    protected void markCompleted(String message) {
        timer.completed(message, logger);
        timer.reset();
    }

    protected long getDuration() {
        return timer.getDuration();
    }

    public boolean read() throws IOException {
        int count = 0;
        ByteBuffer inputBuffer = inputStream.getBuffer();
        if((count = socketChannel.read(inputBuffer)) == -1)
            throw new EOFException("EOF for " + getRemoteSocketAddress());

        if(logger.isTraceEnabled())
            traceInputBufferState(inputBuffer, "Read " + count + " bytes");

        if(count == 0) {
            return false;
        }
        // Take note of the position after we read the bytes. We'll need it in
        // case of incomplete reads later on down the method.
        final int position = inputBuffer.position();
        // Flip the buffer, set our limit to the current position and then set
        // the position to 0 in preparation for reading in the RequestHandler.
        inputBuffer.flip();
        if(isCompleteRequest(inputBuffer, position)) {
            addCheckpoint("Read Complete");
            inputBuffer.rewind();
            return true;
        } else {
            addCheckpoint("Read " + count + "/" + inputBuffer.capacity() + "/"
                          + inputBuffer.remaining());
            return false;
        }
    }

    abstract protected void runRequest() throws IOException;

    public void run() {
        try {
            addCheckpoint("Running request");
            runRequest();
            if(!timer.isComplete()) {
                addCheckpoint("Request Complete");
            }
        } catch(ClosedChannelException e) {
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

    abstract protected boolean isCompleteRequest(ByteBuffer buffer, int position)
            throws IOException;

    public boolean write() throws IOException {
        ByteBuffer outputBuffer = outputStream.getBuffer();
        if(outputBuffer.hasRemaining()) {
            // If we have data, write what we can now...
            int count = socketChannel.write(outputBuffer);
            addCheckpoint("Wrote " + count);

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
            return false;
        } else {
            // If we don't have anything else to write, that means we're done
            // with the request! So clear the buffers (resizing if necessary).
            if(outputBuffer.capacity() >= resizeThreshold) {
                outputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
            } else {
                outputBuffer.clear();
            }
            addCheckpoint("Write Complete");
            return true;
        }
    }

    /**
     * Returns the nanosecond-based timestamp of when this was created.
     * 
     * @return Nanosecond-based timestamp of creation
     */

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void close() {
        // Due to certain code paths, close may be called in a recursive
        // fashion. Rather than trying to handle all of the cases, simply keep
        // track of whether we've been called before and only perform the logic
        // once.
        if(!isClosed.compareAndSet(false, true))
            return;

        closeInternal();
    }

    protected void closeInternal() {
        if(logger.isInfoEnabled())
            logger.info("Closing remote connection from " + socketChannel.socket());

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
        if(socketChannel.isRegistered()) {
            manager.markForClose(socketChannel);
        }
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Flips the output buffer, and lets the Selector know we're ready to write.
     * 
     * @param selectionKey
     */

    protected void resetInputBuffer() {
        ByteBuffer inputBuffer = inputStream.getBuffer();
        if(logger.isTraceEnabled())
            traceInputBufferState(inputBuffer, "About to clear read buffer");

        if(inputBuffer.capacity() >= resizeThreshold) {
            inputStream.setBuffer(ByteBuffer.allocate(socketBufferSize));
        } else {
            inputBuffer.clear();
        }
        if(logger.isTraceEnabled())
            traceInputBufferState(inputStream.getBuffer(), "Cleared read buffer");
    }

    protected void prepareOutputBuffer() {
        resetInputBuffer();

        outputStream.getBuffer().flip();
    }

    protected int getRecommendedRequestCapacity(ByteBuffer inputBuffer) {
        return inputBuffer.capacity() * 2;
    }

    protected void handleIncompleteRequest(ByteBuffer inputBuffer, int newPosition) {
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
            inputStream.setBuffer(ByteUtils.expand(inputStream.getBuffer(),
                                                   getRecommendedRequestCapacity(inputBuffer)));

            if(logger.isTraceEnabled())
                traceInputBufferState(inputStream.getBuffer(), "Expanded input buffer");
        }
    }

    protected void traceInputBufferState(ByteBuffer inputBuffer, String preamble) {
        logger.trace(preamble + " - position: " + inputBuffer.position() + ", limit: "
                     + inputBuffer.limit() + ", remaining: " + inputBuffer.remaining()
                     + ", capacity: " + inputBuffer.capacity() + " - for " + socketChannel.socket());
    }

}
