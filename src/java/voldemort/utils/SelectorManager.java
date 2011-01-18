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
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;

/**
 * SelectorManager handles the non-blocking polling of IO events using the
 * Selector/SelectionKey APIs from NIO.
 * <p/>
 * This is probably not the way to write NIO code, but it's much faster than the
 * documented way. All the documentation on NIO suggested that a single Selector
 * be used for all open sockets and then individual IO requests for selected
 * keys be stuck in a thread pool and executed asynchronously. This seems
 * logical and works fine. However, it was very slow, for two reasons.
 * <p>
 * First, the thread processing the event calls interestOps() on the
 * SelectionKey to update what types of events it's interested in. In fact, it
 * does this twice - first before any processing occurs it disables all events
 * (so that the same channel isn't selected concurrently (similar to disabling
 * interrupts)) and secondly after processing is completed to re-enable interest
 * in events. Understandably, interestOps() has some internal state that it
 * needs to update, and so the thread must grab a lock on the Selector to do
 * internal interest state modifications. With hundreds/thousands of threads,
 * this lock is very heavily contended as backed up by profiling and empirical
 * testing.
 * <p/>
 * The second reason the thread pool approach was slow was that after calling
 * interestOps() to re-enable events, the threads in the thread pool had to
 * invoke the Selector API's wakeup() method or else the state change would go
 * unnoticed (it's similar to notifyAll for basic thread synchronization). This
 * causes the select() method to return immediately and process whatever
 * requests are immediately available. However, with so many threads in play,
 * this lead to a near constant spinning of the select()/wakeup() cycling.
 * <p>
 * Astonishingly it was found to be about 25% faster to simply execute all IO
 * synchronously/serially as it eliminated the context switching, lock
 * contention, etc. However, we actually have N simultaneous SelectorManager
 * instances in play, which are round-robin-ed by the caller (NioSocketService).
 * <p>
 * In terms of the number of SelectorManager instances to use in parallel, the
 * configuration defaults to the number of active CPUs (multi-cores count). This
 * helps to balance out the load a little and help with the serial nature of
 * processing.
 * <p>
 * Of course, potential problems exist.
 * <p>
 * First of all, I still can't believe my eyes that processing these serially is
 * faster than in parallel. There may be something about my environment that is
 * causing inaccurate reporting. At some point, with enough requests I would
 * imagine this will start to slow down.
 * <p/>
 * Another potential problem is that a given SelectorManager could become
 * overloaded. As new socket channels are established, they're distributed to a
 * SelectorManager in a round-robin fashion. However, there's no re-balancing
 * logic in case a disproportionate number of clients on one SelectorManager
 * disconnect.
 * <p/>
 * For instance, let's say we have two SelectorManager instances and four
 * connections. Connection 1 goes to SelectorManager A, connection 2 to
 * SelectorManager B, 3 to A, and 4 to B. However, later on let's say that both
 * connections 1 and 3 disconnect. This leaves SelectorManager B with two
 * connections and SelectorManager A with none. There's no provision to
 * re-balance the remaining requests evenly.
 */

public class SelectorManager implements Runnable {

    public static final int SELECT_OP_CLOSE = -1;
    public static final int SELECTOR_POLL_MS = 500;

    protected final Selector selector;

    protected final AtomicBoolean isClosed;

    private long myThreadId;

    private final ConcurrentMap<SocketChannel, Integer> pendingSelectors;

    protected final Logger logger = Logger.getLogger(getClass());

    public SelectorManager() {
        try {
            this.selector = Selector.open();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        this.pendingSelectors = new ConcurrentHashMap<SocketChannel, Integer>();
        this.isClosed = new AtomicBoolean(false);
    }

    public boolean isOpen() {
        return !isClosed.get();
    }

    protected void processClose() {
        try {
            if(selector.isOpen()) {
                for(SelectionKey sk: selector.keys()) {
                    try {
                        if(logger.isTraceEnabled())
                            logger.trace("Closing SelectionKey's channel");
                        sk.channel().close();
                    } catch(Exception e) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn(e.getMessage(), e);
                    }

                    try {
                        if(logger.isTraceEnabled())
                            logger.trace("Cancelling SelectionKey");

                        sk.cancel();
                    } catch(Exception e) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn(e.getMessage(), e);
                    }
                }
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage() + myThreadId, e);
        }

        try {
            selector.close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }
    }

    public void close() {
        // Attempt to close, but if already closed, then we've been beaten to
        // the punch...
        if(!isClosed.compareAndSet(false, true)) {
            return;
        }
        // While the selector is open, wait for it to close.
        // This is necessary as only the main thread should be messing with the
        // selector,
        // of exceptions (such as ConcurrentModificationExceptions) might be
        // thrown.
        while(selector.isOpen()) {
            try {
                selector.wakeup();
                if(logger.isEnabledFor(Level.INFO))
                    logger.info("Waiting for selector to close");
                Thread.sleep(10);
            } catch(InterruptedException e) {

            }
        }
    }

    /**
     * This method toggles the state of the pending socket select operations.
     * This method runs through the pending socket select operators and updates
     * the state of the selection key. This method is run by the Selector
     * thread.
     */

    protected void processEvents() {
        for(SocketChannel socketChannel: pendingSelectors.keySet()) {
            int operation = pendingSelectors.get(socketChannel);
            if(this.markForOperation(socketChannel, operation)) {
                pendingSelectors.remove(socketChannel, operation);
            }
        }
    }

    protected void processRequest(SelectionKey selectionKey, SelectorManagerWorker worker)
            throws Exception {
        if(selectionKey.isReadable()) {
            if(worker.read()) {
                worker.run();
            }
        } else if(selectionKey.isWritable()) {
            if(worker.write()) {
                SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                this.markForRead(socketChannel);
            }
        } else if(!selectionKey.isValid()) {
            worker.close();
        }
    }

    protected void processSelectionKey(SelectionKey selectionKey) {
        SelectorManagerWorker worker = (SelectorManagerWorker) selectionKey.attachment();
        try {
            processRequest(selectionKey, worker);
        } catch(AsynchronousCloseException e) {
            worker.close();
        } catch(CancelledKeyException e) {
            worker.close();
        } catch(EOFException e) {
            worker.close();
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);

            worker.close();
        }

    }

    /**
     * The main thread method for the SelectorManager. Loops until an error
     * occurs or until it is closed processing requests from the selector.
     */
    public void run() {
        myThreadId = Thread.currentThread().getId();
        try {
            while(true) {
                if(isClosed.get()) {
                    if(logger.isInfoEnabled())
                        logger.info("Closed, exiting");
                    break;
                }

                // Update any events waiting for this selector
                processEvents();

                try {
                    // Perform a select.
                    int selected = selector.select(SELECTOR_POLL_MS);

                    if(isClosed.get()) {
                        // Break out if the selector was closed
                        if(logger.isInfoEnabled())
                            logger.info("Closed, exiting");

                        break;
                    }

                    if(selected > 0) {
                        Iterator<SelectionKey> i = selector.selectedKeys().iterator();

                        // Loop through each selected key and process its
                        // actions
                        while(isOpen() && i.hasNext()) {
                            SelectionKey selectionKey = i.next();
                            i.remove();
                            this.processSelectionKey(selectionKey);
                        }
                    }
                } catch(ClosedSelectorException e) {
                    if(logger.isDebugEnabled())
                        logger.debug("Selector is closed, exiting");

                    break;
                } catch(Throwable t) {
                    if(logger.isEnabledFor(Level.ERROR))
                        logger.error(t.getMessage(), t);
                }
            }
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);
        } finally {
            try {
                if(logger.isEnabledFor(Level.DEBUG))
                    logger.debug("Closing select manager");
                processClose();
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e.getMessage(), e);
            }
        }
        if(logger.isEnabledFor(Level.DEBUG))
            logger.debug("Select manager is exiting");
    }

    /**
     * Marks the socket channel as available for read
     * 
     * @param socketChannel The socket channel being updated
     * @return
     */
    public boolean markForRead(SocketChannel socketChannel) {
        return markForOperation(socketChannel, SelectionKey.OP_READ);
    }

    /**
     * Marks the socket channel as available for write
     * 
     * @param socketChannel The socket channel being updated
     * @return
     */
    public boolean markForWrite(SocketChannel socketChannel) {
        return markForOperation(socketChannel, SelectionKey.OP_WRITE);
    }

    /**
     * Marks the socket channel as available for close
     * 
     * @param socketChannel The socket channel being updated
     * @return
     */
    public boolean markForClose(SocketChannel socketChannel) {
        return markForOperation(socketChannel, SELECT_OP_CLOSE);
    }

    /**
     * Updates the state of the channel with the selector
     * 
     * @param operation The operation for the selector for this channel.
     * @param socketChannel The socket channel being updated
     * @return
     */
    public boolean markForOperation(SocketChannel socketChannel, int operation) {
        // If the selector is already closed, just return immediately
        if(isClosed.get()) {
            if(logger.isInfoEnabled())
                logger.info("Skipping mark for " + socketChannel.socket() + " on closed selector");
            return true;
        } else if(this.myThreadId != Thread.currentThread().getId()) {
            // If the operation is not being performed by the selector thread,
            // add it to the queue
            // and kick the selector.
            this.pendingSelectors.put(socketChannel, operation);
            this.selector.wakeup();
            return true;
        } else {
            // The operation is being performed by the selector thread,
            // Update the selection key.
            // Because of the non-thread-safe nature of selectors and keys, this
            // work must be done
            // by the selector thread.
            try {
                SelectionKey selectionKey = socketChannel.keyFor(selector);
                if(selectionKey != null) {
                    // On close, detach the key from the selector and cancel
                    // events
                    if(operation == SELECT_OP_CLOSE) {
                        selectionKey.attach(null);
                        selectionKey.cancel();
                    } else {
                        // Not close, just flip the interested operations
                        selectionKey.interestOps(operation);
                    }
                    return true;
                }
            } catch(CancelledKeyException e) {

            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e.getMessage(), e);
            }
            return false;
        }
    }
}
