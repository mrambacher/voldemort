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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.store.socket.SocketDestination;
import voldemort.utils.DaemonThreadFactory;
import voldemort.utils.SelectorManager;
import voldemort.utils.SelectorManagerWorker;
import voldemort.utils.Time;
import voldemort.utils.pool.ResourceFactory;

/**
 * A Factory for creating ClientRequestExecutor instances.
 */

public class ClientRequestExecutorFactory implements
        ResourceFactory<SocketDestination, ClientRequestExecutor> {

    private static final int SHUTDOWN_TIMEOUT_MS = 15000;
    private final int connectTimeoutMs;
    private final int soTimeoutMs;
    private final int socketBufferSize;
    private final AtomicInteger created;
    private final AtomicInteger destroyed;
    private final boolean socketKeepAlive;
    private final ClientRequestSelectorManager[] selectorManagers;
    private final ExecutorService selectorManagerThreadPool;
    private final AtomicInteger counter = new AtomicInteger();
    private final Map<SocketDestination, Long> lastClosedTimestamps;
    private final Logger logger = Logger.getLogger(getClass());

    public ClientRequestExecutorFactory(int selectors,
                                        int connectTimeoutMs,
                                        int soTimeoutMs,
                                        int socketBufferSize,
                                        boolean socketKeepAlive) {
        this.connectTimeoutMs = connectTimeoutMs;
        this.soTimeoutMs = soTimeoutMs;
        this.created = new AtomicInteger(0);
        this.destroyed = new AtomicInteger(0);
        this.socketBufferSize = socketBufferSize;
        this.socketKeepAlive = socketKeepAlive;

        this.selectorManagers = new ClientRequestSelectorManager[selectors];
        this.selectorManagerThreadPool = Executors.newFixedThreadPool(selectorManagers.length,
                                                                      new DaemonThreadFactory("voldemort-niosocket-client-"));

        for(int i = 0; i < selectorManagers.length; i++) {
            selectorManagers[i] = new ClientRequestSelectorManager();
            selectorManagerThreadPool.execute(selectorManagers[i]);
        }

        this.lastClosedTimestamps = new ConcurrentHashMap<SocketDestination, Long>();
    }

    /**
     * Close the ClientRequestExecutor.
     */

    public void destroy(SocketDestination dest, ClientRequestExecutor clientRequestExecutor)
            throws Exception {
        clientRequestExecutor.close();
        int numDestroyed = destroyed.incrementAndGet();

        if(logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    /**
     * Create a ClientRequestExecutor for the given {@link SocketDestination}.
     * 
     * @param dest {@link SocketDestination}
     */

    public ClientRequestExecutor create(SocketDestination dest) throws Exception {
        int numCreated = created.incrementAndGet();

        if(logger.isDebugEnabled())
            logger.debug("Creating socket " + numCreated + " for " + dest.getHost() + ":"
                         + dest.getPort() + " using protocol "
                         + dest.getRequestFormatType().getCode());

        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.socket().setReceiveBufferSize(this.socketBufferSize);
        socketChannel.socket().setSendBufferSize(this.socketBufferSize);
        socketChannel.socket().setTcpNoDelay(true);
        socketChannel.socket().setSoTimeout(soTimeoutMs);
        socketChannel.socket().setKeepAlive(this.socketKeepAlive);
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(dest.getHost(), dest.getPort()));
        ClientRequestSelectorManager selectorManager = selectorManagers[counter.getAndIncrement()
                                                                        % selectorManagers.length];

        Selector selector = selectorManager.getSelector();
        ClientRequestExecutor clientRequestExecutor = new ClientRequestExecutor(selectorManager,
                                                                                socketChannel,
                                                                                socketBufferSize,
                                                                                soTimeoutMs,
                                                                                dest.getRequestFormatType());
        // If the connection timeout is set, sleep-spin here waiting for the
        // connect to complete.
        // If the connection timeout is 0, then the connect will be handled via
        // NIO and
        // the selector will handle the OP_CONNECT case.
        if(this.connectTimeoutMs > 0) {
            long startTime = System.currentTimeMillis();
            long duration = 0;
            long currWaitTime = 1;
            long prevWaitTime = 1;
            if(logger.isTraceEnabled())
                logger.trace("Waiting " + connectTimeoutMs + " ms for socket " + dest.getHost()
                             + ":" + dest.getPort() + "to finish connect");
            // Since we're non-blocking and it takes a non-zero amount of time
            // to connect, invoke finishConnect and loop.
            while(!clientRequestExecutor.finishConnect()) {
                duration = System.currentTimeMillis() - startTime;
                long remaining = this.connectTimeoutMs - duration;
                if(remaining < 0) {
                    throw new ConnectException("Cannot connect socket " + numCreated + " for "
                                               + dest.getHost() + ":" + dest.getPort() + " after "
                                               + duration + " ms");
                }

                if(logger.isTraceEnabled())
                    logger.trace("Still creating socket " + numCreated + " for " + dest.getHost()
                                 + ":" + dest.getPort() + ", " + remaining
                                 + " ms. remaining to connect");

                try {
                    // Break up the connection timeout into smaller units,
                    // employing a Fibonacci-style back-off (1, 2, 3, 5, 8, ...)
                    Thread.sleep(Math.min(remaining, currWaitTime));
                    currWaitTime = Math.min(currWaitTime + prevWaitTime, 50);
                    prevWaitTime = currWaitTime - prevWaitTime;
                } catch(InterruptedException e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e, e);
                }
            }
        }
        selectorManager.registrationQueue.add(clientRequestExecutor);
        selector.wakeup();

        return clientRequestExecutor;
    }

    public boolean validate(SocketDestination dest, ClientRequestExecutor clientRequestExecutor) {
        /**
         * Keep track of the last time that we closed the sockets for a specific
         * SocketDestination. That way we know which sockets were created
         * *before* the SocketDestination was closed. For any sockets in the
         * pool at time of closure of the SocketDestination, these are shut down
         * immediately. For in-flight sockets that aren't in the pool at time of
         * closure of the SocketDestination, these are caught when they're
         * checked in via validate noting the relation of the timestamps.
         * 
         * See bug #222.
         */
        long lastClosedTimestamp = getLastClosedTimestamp(dest);

        if(clientRequestExecutor.getCreateTimestamp() <= lastClosedTimestamp) {
            if(logger.isDebugEnabled())
                logger.debug("Socket connection "
                             + clientRequestExecutor
                             + " was created on "
                             + new Date(clientRequestExecutor.getCreateTimestamp() / Time.NS_PER_MS)
                             + " before socket pool was closed and re-created (on "
                             + new Date(lastClosedTimestamp / Time.NS_PER_MS) + ")");
            return false;
        }

        boolean isValid = clientRequestExecutor.isValid();
        if(!isValid && logger.isDebugEnabled())
            logger.debug("Client request executor connection " + clientRequestExecutor
                         + " is no longer valid, closing.");

        return isValid;
    }

    public int getTimeout() {
        return this.soTimeoutMs;
    }

    public int getNumberCreated() {
        return this.created.get();
    }

    public int getNumberDestroyed() {
        return this.destroyed.get();
    }

    public void close() {
        try {
            // We close instead of interrupting the thread pool. Why? Because as
            // of 0.70, the SelectorManager services RequestHandler in the same
            // thread as itself. So, if we interrupt the SelectorManager in the
            // thread pool, we interrupt the request. In some RequestHandler
            // implementations interruptions are not handled gracefully and/or
            // indicate other errors which cause odd side effects. So we
            // implement a non-interrupt-based shutdown via close.
            for(int i = 0; i < selectorManagers.length; i++) {
                try {
                    selectorManagers[i].close();
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }

            // As per the above comment - we use shutdown and *not* shutdownNow
            // to avoid using interrupts to signal shutdown.
            selectorManagerThreadPool.shutdown();

            if(logger.isTraceEnabled())
                logger.trace("Shut down SelectorManager thread pool acceptor, waiting "
                             + SHUTDOWN_TIMEOUT_MS + " ms for termination");

            boolean terminated = selectorManagerThreadPool.awaitTermination(SHUTDOWN_TIMEOUT_MS,
                                                                            TimeUnit.MILLISECONDS);

            if(!terminated) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn("SelectorManager thread pool did not stop cleanly after "
                                + SHUTDOWN_TIMEOUT_MS + " ms");
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }
    }

    private class ClientRequestSelectorManager extends SelectorManager {

        private final Queue<ClientRequestExecutor> registrationQueue = new ConcurrentLinkedQueue<ClientRequestExecutor>();

        public Selector getSelector() {
            return selector;
        }

        @Override
        protected void processRequest(SelectionKey selectionKey, SelectorManagerWorker worker)
                throws Exception {

            // In the case of a connect event, clear the bit, finish the
            // connection, and start to negotiate the protocol.
            if(selectionKey.isConnectable()) {
                ClientRequestExecutor executor = (ClientRequestExecutor) worker;
                SocketChannel socketChannel = executor.getSocketChannel();
                this.markForOperation(socketChannel, 0);
                try {
                    if(executor.finishConnect()) {
                        executor.startNegotiateProtocol();
                    } else {
                        logger.debug("Channel was marked for connect but failed "
                                     + socketChannel.socket());
                        markForOperation(socketChannel, SelectionKey.OP_CONNECT);
                    }
                } catch(IOException e) {
                    logger.error("Failed to connect to executor ", e);
                }
            } else {
                // Not a connect event, handle it normally
                super.processRequest(selectionKey, worker);
            }
        }

        private void registerRequests() {
            try {
                ClientRequestExecutor clientRequestExecutor = null;

                while((clientRequestExecutor = registrationQueue.poll()) != null) {
                    if(isClosed.get()) {
                        if(logger.isDebugEnabled())
                            logger.debug("Closed, exiting");

                        break;
                    }

                    try {
                        SocketChannel socketChannel = clientRequestExecutor.getSocketChannel();
                        if(logger.isDebugEnabled())
                            logger.debug("Registering connection from " + socketChannel.socket());
                        // Check if the connection is already complete.
                        // This handles the case of waiting earlier for the
                        // connect to complete
                        // or a quick connect and avoids going through an
                        // additional selector if
                        // not necessary
                        if(socketChannel.isConnected()) {
                            // Connection is already complete.
                            // Register it and start negotiating the protocol
                            socketChannel.register(selector, 0, clientRequestExecutor);
                            clientRequestExecutor.startNegotiateProtocol();
                        } else {
                            try {
                                if(clientRequestExecutor.finishConnect()) {
                                    // Connection is complete. Register it and
                                    // start
                                    // negotiating the protocol
                                    socketChannel.register(selector, 0, clientRequestExecutor);
                                    clientRequestExecutor.startNegotiateProtocol();
                                } else if(socketChannel.isOpen()) {
                                    // Connection is complete. Register for
                                    // connectionevents
                                    socketChannel.register(selector,
                                                           SelectionKey.OP_CONNECT,
                                                           clientRequestExecutor);
                                } else {
                                    logger.warn("Channel "
                                                + socketChannel.socket().getRemoteSocketAddress()
                                                + " is already closed");
                                }
                            } catch(IOException e) {
                                logger.error("Failed to register connect executor "
                                                     + socketChannel.socket(),
                                             e);
                            }
                        }
                    } catch(ClosedSelectorException e) {
                        if(logger.isDebugEnabled())
                            logger.debug("Selector is closed, exiting");

                        close();

                        break;
                    } catch(Exception e) {
                        if(logger.isEnabledFor(Level.ERROR))
                            logger.error(e.getMessage(), e);
                    }
                }
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error(e.getMessage(), e);
            }
        }

        /**
         * Process the {@link ClientRequestExecutor} registrations which are
         * made inside {@link ClientRequestExecutorFactory} on creation of a new
         * {@link ClientRequestExecutor}.
         */

        @Override
        protected void processEvents() {
            super.processEvents();
            registerRequests();
        }
    }

    /**
     * Returns the nanosecond-based timestamp of when this socket destination
     * was last closed. SocketDestination objects can be closed when their node
     * is marked as unavailable if the node goes down (temporarily or
     * otherwise). This timestamp is used to determine when sockets related to
     * the SocketDestination should be closed.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via setLastClosedTimestamp each
     * time the node is marked as unavailable.
     * 
     * @return Nanosecond-based timestamp of last close
     */

    private long getLastClosedTimestamp(SocketDestination socketDestination) {
        Long lastClosedTimestamp = lastClosedTimestamps.get(socketDestination);
        return lastClosedTimestamp != null ? lastClosedTimestamp.longValue() : 0;
    }

    /**
     * Assigns the last closed timestamp based on the current time in
     * nanoseconds.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via this method each time the
     * node is marked as unavailable.
     */

    public void setLastClosedTimestamp(SocketDestination socketDestination) {
        lastClosedTimestamps.put(socketDestination, System.nanoTime());
    }

}
