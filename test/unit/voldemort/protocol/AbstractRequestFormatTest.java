package voldemort.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.client.protocol.ClientRequestFormat;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public abstract class AbstractRequestFormatTest extends TestCase {

    private final String storeName;
    private final RequestFormat clientWireFormat;
    private final RequestHandler serverWireFormat;
    private final InMemoryStorageEngine<ByteArray, byte[], byte[]> store;
    private final RequestFormatType formatType;

    public AbstractRequestFormatTest(RequestFormatType type) {
        this.formatType = type;
        this.storeName = "test";
        this.store = new InMemoryStorageEngine<ByteArray, byte[], byte[]>(storeName);
        StoreRepository repository = new StoreRepository();
        repository.addLocalStore(store);
        repository.addRoutedStore(store);
        this.clientWireFormat = RequestFormatFactory.getRequestFormat(type);
        this.serverWireFormat = ServerTestUtils.getSocketRequestHandlerFactory(repository)
                                               .getRequestHandler(type);
    }

    @Test
    public void testNullKeys() throws Exception {
        try {
            testGetRequest(null, null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testGetAllRequest(new ByteArray[] { null }, null, null, null, new boolean[] { false });
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testPutRequest(null, null, null, null);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
        try {
            testDeleteRequest(null, null, null, false);
            fail("Null key allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    @Test
    public void testGetRequests() throws Exception {
        testGetRequest(TestUtils.toByteArray("hello"), null, null, null, false);
        testGetRequest(TestUtils.toByteArray("hello"), "".getBytes(), null, new VectorClock(), true);
        testGetRequest(TestUtils.toByteArray("hello"),
                       "abc".getBytes(),
                       null,
                       TestUtils.getClock(1, 2, 2, 3),
                       true);
        testGetRequest(TestUtils.toByteArray("hello"),
                       "abcasdf".getBytes(),
                       null,
                       TestUtils.getClock(1, 3, 4, 5),
                       true);

    }

    protected <V> V processRequest(ClientRequestFormat<V> request) throws IOException {
        ByteArrayOutputStream requestStream = new ByteArrayOutputStream();
        request.writeRequest(new DataOutputStream(requestStream));
        ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
        this.serverWireFormat.handleRequest(inputStream(requestStream),
                                            new DataOutputStream(responseStream));
        return request.readResponse(inputStream(responseStream));

    }

    public void testGetRequest(ByteArray key,
                               byte[] value,
                               byte[] transforms,
                               Version version,
                               boolean isPresent) throws Exception {
        try {
            if(isPresent)
                store.put(key, Versioned.value(value, version), null);
            ClientRequestFormat<List<Versioned<byte[]>>> request = this.clientWireFormat.createGetRequest(storeName,
                                                                                                          key,
                                                                                                          transforms,
                                                                                                          RequestRoutingType.NORMAL);
            List<Versioned<byte[]>> values = processRequest(request);
            if(isPresent) {
                assertEquals(1, values.size());
                Versioned<byte[]> v = values.get(0);
                assertEquals(version, v.getVersion());
                assertTrue(Arrays.equals(v.getValue(), value));
            } else {
                assertEquals(0, values.size());
            }
        } finally {
            this.store.deleteAll();
        }
    }

    @Test
    public void testGetAllRequests() throws Exception {
        testGetAllRequest(new ByteArray[] {},
                          new byte[][] {},
                          null,
                          new Version[] {},
                          new boolean[] {});

        testGetAllRequest(new ByteArray[] { new ByteArray() },
                          new byte[][] { new byte[] {} },
                          null,
                          new Version[] { new VectorClock() },
                          new boolean[] { true });

        testGetAllRequest(new ByteArray[] { TestUtils.toByteArray("hello") },
                          new byte[][] { "world".getBytes() },
                          null,
                          new Version[] { new VectorClock() },
                          new boolean[] { true });

        testGetAllRequest(new ByteArray[] { TestUtils.toByteArray("hello"),
                                  TestUtils.toByteArray("holly") },
                          new byte[][] { "world".getBytes(), "cow".getBytes() },
                          null,
                          new Version[] { TestUtils.getClock(1, 1), TestUtils.getClock(1, 2) },
                          new boolean[] { true, false });
    }

    public void testGetAllRequest(ByteArray[] keys,
                                  byte[][] values,
                                  Map<ByteArray, byte[]> transforms,
                                  Version[] versions,
                                  boolean[] isFound) throws Exception {
        try {
            for(int i = 0; i < keys.length; i++) {
                if(isFound[i])
                    store.put(keys[i], Versioned.value(values[i], versions[i]), null);
            }

            ClientRequestFormat<Map<ByteArray, List<Versioned<byte[]>>>> request = this.clientWireFormat.createGetAllRequest(store.getName(),
                                                                                                                             Arrays.asList(keys),
                                                                                                                             transforms,
                                                                                                                             RequestRoutingType.NORMAL);
            Map<ByteArray, List<Versioned<byte[]>>> found = this.processRequest(request);
            for(int i = 0; i < keys.length; i++) {
                if(isFound[i]) {
                    assertTrue(keys[i] + " is not in the found set.", found.containsKey(keys[i]));
                    assertEquals(1, found.get(keys[i]).size());
                    Versioned<byte[]> versioned = found.get(keys[i]).get(0);
                    assertEquals(versions[i], versioned.getVersion());
                    assertTrue(Arrays.equals(values[i], versioned.getValue()));
                } else {
                    assertTrue(keys[i] + " is in the found set but should not be.",
                               !found.containsKey(keys[i]));
                }
            }
        } finally {
            this.store.deleteAll();
        }
    }

    @Test
    public void testExceptions() throws Exception {
        // test obsolete exception
        Version saved = this.store.put(TestUtils.toByteArray("hello"),
                                       new Versioned<byte[]>("world".getBytes(),
                                                             TestUtils.getClock(1, 1)),
                                       null);
        Version obsolete = testPutRequest(TestUtils.toByteArray("hello"),
                                          "world".getBytes(),
                                          TestUtils.getClock(1),
                                          null,
                                          ObsoleteVersionException.class);
        if(this.supportsMetadata()) {
            assertEquals("Saved equals obsolete", saved, obsolete);
        }
        testPutStoreRequest("",
                            TestUtils.toByteArray("hello"),
                            new Versioned<byte[]>("world".getBytes(), TestUtils.getClock(1)),
                            null,
                            VoldemortException.class);
    }

    @Test
    public void testPutRequests() throws Exception {
        testPutRequest(new ByteArray(), new byte[0], new VectorClock(), null, null);
        testPutRequest(TestUtils.toByteArray("hello"),
                       "world".getBytes(),
                       new VectorClock(),
                       null,
                       null);

        // test obsolete exception
        Version stored = store.put(TestUtils.toByteArray("hello"),
                                   new Versioned<byte[]>("world".getBytes(), new VectorClock()),
                                   null);
        testPutRequest(TestUtils.toByteArray("hello"),
                       "world".getBytes(),
                       stored,
                       null,
                       ObsoleteVersionException.class);
    }

    public Version testPutRequest(ByteArray key,
                                  byte[] value,
                                  Version version,
                                  byte[] transforms,
                                  Class<? extends VoldemortException> exception) throws Exception {
        Versioned<byte[]> versioned = new Versioned<byte[]>(value, version);
        return testPutRequest(key, versioned, transforms, exception);
    }

    public Version testPutRequest(ByteArray key,
                                  Versioned<byte[]> versioned,
                                  byte[] transforms,
                                  Class<? extends VoldemortException> exception) throws Exception {
        return testPutStoreRequest(this.storeName, key, versioned, transforms, exception);
    }

    public Version testPutStoreRequest(String name,
                                       ByteArray key,
                                       Versioned<byte[]> versioned,
                                       byte[] transforms,
                                       Class<? extends VoldemortException> exception)
            throws Exception {
        Version result = null;
        try {
            ClientRequestFormat<Version> request = this.clientWireFormat.createPutRequest(name,
                                                                                          key,
                                                                                          versioned,
                                                                                          transforms,
                                                                                          RequestRoutingType.NORMAL);
            result = processRequest(request);
            if(this.formatType.getVersion() < RequestFormatType.VOLDEMORT_V3.getVersion()) {
                result = versioned.getVersion();
            }
            TestUtils.assertContains(this.store, key, result, versioned);
        } catch(IllegalArgumentException e) {
            throw e;
        } catch(Exception e) {
            assertEquals("Unexpected exception " + e.getClass().getName(), e.getClass(), exception);
            if(supportsMetadata()) {
                if(e instanceof ObsoleteVersionException) {
                    ObsoleteVersionException ove = (ObsoleteVersionException) e;
                    assertNotNull("Obsolete version set", ove.getExistingVersion());
                    result = ove.getExistingVersion();
                }
            }
        } finally {
            this.store.deleteAll();
        }
        return result;
    }

    protected boolean supportsMetadata() {
        if(formatType.equals(RequestFormatType.PROTOCOL_BUFFERS)) {
            return true;
        } else if(formatType.getCode().startsWith("vp")) {
            return formatType.getVersion() >= RequestFormatType.VOLDEMORT_V3.getVersion();
        } else {
            return false;
        }

    }

    @Test
    public void testMetadata() throws Exception {
        if(supportsMetadata()) {
            Versioned<byte[]> versioned = new Versioned<byte[]>("world".getBytes(),
                                                                new VectorClock());
            versioned.getMetadata().setProperty("test", "metadata");
            testPutRequest(TestUtils.toByteArray("hello"), versioned, null, null);
        }
    }

    @Test
    public void testDeleteRequests() throws Exception {
        // test pre-existing are deleted
        testDeleteRequest(new ByteArray(),
                          new VectorClock(),
                          new Versioned<byte[]>("hello".getBytes()),
                          true);
        testDeleteRequest(TestUtils.toByteArray("hello"),
                          new VectorClock(),
                          new Versioned<byte[]>("world".getBytes()),
                          true);

        // test non-existant aren't deleted
        testDeleteRequest(TestUtils.toByteArray("hello"), new VectorClock(), null, false);
    }

    public void testDeleteRequest(ByteArray key,
                                  Version version,
                                  Versioned<byte[]> existingValue,
                                  boolean isDeleted) throws Exception {
        try {
            if(existingValue != null)
                this.store.put(key, existingValue, null);
            ClientRequestFormat<Boolean> request = this.clientWireFormat.createDeleteRequest(store.getName(),
                                                                                             key,
                                                                                             version,
                                                                                             RequestRoutingType.NORMAL);
            boolean wasDeleted = processRequest(request);
            assertEquals(isDeleted, wasDeleted);
        } finally {
            this.store.deleteAll();
        }
    }

    public DataInputStream inputStream(ByteArrayOutputStream output) {
        return new DataInputStream(new ByteArrayInputStream(output.toByteArray()));
    }
}
