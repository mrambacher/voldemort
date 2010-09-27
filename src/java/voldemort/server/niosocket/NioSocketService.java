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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.AbstractSocketService;
import voldemort.server.ServiceType;
import voldemort.server.StatusManager;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.utils.DaemonThreadFactory;

/**
 * NioSocketService is an NIO-based socket service, comparable to the
 * blocking-IO-based socket service.
 * <p/>
 * The NIO server is enabled in the server.properties file by setting the
 * "enable.nio.connector" property to "true". If you want to adjust the number
 * of SelectorManager instances that are used, change "nio.connector.selectors"
 * to a positive integer value. Otherwise, the number of selectors will be equal
 * to the number of CPUs visible to the JVM.
 * <p/>
 * This code uses the NIO APIs directly. It would be a good idea to consider
 * some of the NIO frameworks to handle this more cleanly, efficiently, and to
 * handle corner cases.
 * 
 * 
 * @see voldemort.server.socket.SocketService
 */

public class NioSocketService extends AbstractSocketService {

    private static final int SHUTDOWN_TIMEOUT_MS = 15000;

    private final RequestHandlerFactory requestHandlerFactory;

    private final ServerSocketChannel serverSocketChannel;

    private final SelectorManager[] selectorManagers;

    private LinkedBlockingQueue<AsyncRequestHandler> pendingRequests;

    private final ExecutorService selectorManagerThreadPool;

    private final int socketBufferSize;

    private final int socketListenQueueLength;

    private final int workers;

    private final StatusManager statusManager;

    private Thread acceptorThread = null;

    private final ExecutorService workerThreads;

    private final Logger logger = Logger.getLogger(getClass());

    public NioSocketService(RequestHandlerFactory requestHandlerFactory,
                            int port,
                            String serviceName,
                            VoldemortConfig config) {
        this(requestHandlerFactory,
             port,
             config.getSocketBufferSize(),
             config.getNioConnectorSelectors(),
             config.getNioWorkerThreads(),
             config.getSocketListenQueueLength(),
             serviceName,
             config.isJmxEnabled());
    }

    public NioSocketService(RequestHandlerFactory requestHandlerFactory,
                            int port,
                            int socketBufferSize,
                            int selectors,
                            int workers,
                            int socketListenQueueLength,
                            String serviceName,
                            boolean enableJmx) {
        super(ServiceType.SOCKET, port, serviceName, enableJmx);
        this.requestHandlerFactory = requestHandlerFactory;
        this.socketBufferSize = socketBufferSize;

        try {
            this.serverSocketChannel = ServerSocketChannel.open();
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        this.selectorManagers = new SelectorManager[selectors];
        this.selectorManagerThreadPool = Executors.newFixedThreadPool(selectorManagers.length,
                                                                      new DaemonThreadFactory("voldemort-niosocket-server"));
        this.workers = workers;
        if(workers > 0) {
            this.workerThreads = Executors.newFixedThreadPool(workers,
                                                              new DaemonThreadFactory("voldemort-niosocket-worker"));
            this.pendingRequests = new LinkedBlockingQueue<AsyncRequestHandler>();
        } else {
            pendingRequests = null;
            workerThreads = null;
        }
        this.statusManager = new StatusManager((ThreadPoolExecutor) this.selectorManagerThreadPool);
        this.socketListenQueueLength = socketListenQueueLength;
    }

    @Override
    public StatusManager getStatusManager() {
        return statusManager;
    }

    @Override
    protected void startInner() {
        if(logger.isEnabledFor(Level.INFO))
            logger.info("Starting Voldemort NIO socket server (" + serviceName + ") on port "
                        + port);
        try {
            for(int i = 0; i < workers; i++) {
                this.workerThreads.execute(new NioWorkerThread(this.pendingRequests));
            }

            InetSocketAddress endpoint = new InetSocketAddress(port);
            for(int i = 0; i < selectorManagers.length; i++) {
                selectorManagers[i] = new SelectorManager(requestHandlerFactory,
                                                          socketBufferSize,
                                                          pendingRequests);
                selectorManagerThreadPool.execute(selectorManagers[i]);
            }

            if(socketListenQueueLength > 0) {
                serverSocketChannel.socket().bind(endpoint, socketListenQueueLength);
            } else {
                // let jdk default kick in for queue length
                serverSocketChannel.socket().bind(endpoint);
            }
            serverSocketChannel.socket().setReceiveBufferSize(socketBufferSize);
            serverSocketChannel.socket().setReuseAddress(true);

            acceptorThread = new Thread(new Acceptor());
            acceptorThread.start();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(e.getMessage(), e);
            throw new VoldemortException(e.getMessage(), e);
        }

        enableJmx(this);
    }

    @Override
    protected void stopInner() {
        if(logger.isEnabledFor(Level.INFO))
            logger.info("Stopping Voldemort NIO socket server (" + serviceName + ") on port "
                        + port);

        try {
            // Signal the thread to stop accepting new connections...
            if(logger.isTraceEnabled())
                logger.trace("Interrupted acceptor thread, waiting " + SHUTDOWN_TIMEOUT_MS
                             + " ms for termination");

            if(acceptorThread != null) {
                acceptorThread.interrupt();
                acceptorThread.join(SHUTDOWN_TIMEOUT_MS);

                if(acceptorThread.isAlive()) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn("Acceptor thread pool did not stop cleanly after "
                                    + SHUTDOWN_TIMEOUT_MS + " ms");
                }
            }
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        if(workerThreads != null) {
            this.workerThreads.shutdown();
        }
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

        try {
            serverSocketChannel.socket().close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            serverSocketChannel.close();
        } catch(Exception e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }
    }

    private class Acceptor implements Runnable {

        private Acceptor() {}

        public void run() {
            if(logger.isInfoEnabled())
                logger.info("Server now listening for connections on " + port);

            AtomicInteger counter = new AtomicInteger();

            while(true) {
                if(Thread.currentThread().isInterrupted()) {
                    if(logger.isInfoEnabled())
                        logger.info("Acceptor thread interrupted");

                    break;
                }

                try {
                    SocketChannel socketChannel = serverSocketChannel.accept();

                    if(socketChannel == null) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn("Claimed accept but nothing to select");

                        continue;
                    }

                    SelectorManager selectorManager = selectorManagers[counter.getAndIncrement()
                                                                       % selectorManagers.length];
                    selectorManager.accept(socketChannel);
                } catch(ClosedByInterruptException e) {
                    // If you're *really* interested...
                    if(logger.isTraceEnabled())
                        logger.trace("Acceptor thread interrupted, closing");

                    break;
                } catch(Exception e) {
                    if(logger.isEnabledFor(Level.WARN))
                        logger.warn(e.getMessage(), e);
                }
            }

            if(logger.isInfoEnabled())
                logger.info("Server has stopped listening for connections on " + port);
        }

    }

}
