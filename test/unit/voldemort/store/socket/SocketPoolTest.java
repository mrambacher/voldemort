package voldemort.store.socket;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.VoldemortClientException;
import voldemort.client.VoldemortInterruptedException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.socket.SocketServer;
import voldemort.store.UnreachableStoreException;

public class SocketPoolTest extends TestCase {

    private SocketPool pool;
    private SocketServer socketServer;
    private int socketPort;

    @Before
    @Override
    public void setUp() throws Exception {
        pool = new SocketPool(1, 10000, 10000, 10000);
        this.socketPort = ServerTestUtils.findFreePort();
        RequestHandlerFactory factory = new SocketRequestHandlerFactory(null,
                                                                        null,
                                                                        null,
                                                                        null,
                                                                        null,
                                                                        null);
        socketServer = new SocketServer(socketPort,
                                        1000,
                                        0,
                                        (ThreadPoolExecutor) Executors.newFixedThreadPool(2),
                                        factory,
                                        "server-test");
        socketServer.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        pool.close();
        socketServer.shutdown();
    }

    @Test
    public void testCheckoutTimeout() {
        SocketDestination destination = new SocketDestination("localhost",
                                                              socketPort,
                                                              RequestFormatType.VOLDEMORT_V3);
        SocketAndStreams s1 = pool.checkout(destination);
        try {
            SocketAndStreams s2 = pool.checkout(destination);
            pool.checkin(destination, s2);
            fail("Expected exception");
        } catch(Exception e) {
            assertEquals("Unexpected exception", VoldemortClientException.class, e.getClass());
        } finally {
            pool.checkin(destination, s1);
            System.out.println("Pool has " + pool.getNumberOfActiveConnections() + " connections");
        }
    }

    @Test
    public void testCheckoutInterrupt() {
        final SocketDestination destination = new SocketDestination("localhost",
                                                                    socketPort,
                                                                    RequestFormatType.VOLDEMORT_V3);
        SocketAndStreams s1 = pool.checkout(destination);
        try {
            Thread child = new Thread(new Runnable() {

                public void run() {
                    try {
                        SocketAndStreams s2 = pool.checkout(destination);
                        pool.checkin(destination, s2);
                        fail("Expected exception");
                    } catch(Exception e) {
                        assertEquals("Unexpected exception",
                                     VoldemortInterruptedException.class,
                                     e.getClass());
                    }
                }
            });
            child.start();
            Thread.sleep(100);
            child.interrupt();
            child.join();
        } catch(Exception e) {
            fail("Unexpected exception [" + e.getClass() + "]: " + e.getMessage());
        } finally {
            pool.checkin(destination, s1);
        }
    }

    @Test
    public void testCheckoutUnreachable() {
        int freePort = ServerTestUtils.findFreePort();
        SocketDestination destination = new SocketDestination("localhost",
                                                              freePort,
                                                              RequestFormatType.VOLDEMORT_V3);
        try {
            SocketAndStreams s1 = pool.checkout(destination);
            fail("Expected exception");
        } catch(Exception e) {
            assertEquals("Unexpected exception", UnreachableStoreException.class, e.getClass());
        } finally {}
    }
}
