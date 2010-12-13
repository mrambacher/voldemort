package voldemort.store.stats;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.socket.ProtocolBuffersSocketStoreTest;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

@RunWith(Parameterized.class)
public class StatTrackingStoreTest extends AbstractByteArrayStoreTest {

    private static final Logger logger = Logger.getLogger(StatTrackingStore.class);

    AbstractByteArrayStoreTest inner;
    int count;
    Random random;
    Map<ByteArray, Version> versions;
    int numThreads;
    int iterations;

    public StatTrackingStoreTest(AbstractByteArrayStoreTest inner, int count) {
        super("test");
        this.inner = inner;
        this.count = count;
        numThreads = 10;
        this.iterations = count / numThreads;
        random = new Random();
        versions = new ConcurrentHashMap<ByteArray, Version>();

    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { new ProtocolBuffersSocketStoreTest(true), 1000 } });
    }

    @Before
    @Override
    public void setUp() throws Exception {
        inner.setUp();
        super.setUp();
        populateStore();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        inner.tearDown();
        super.tearDown();
    }

    @Override
    public StatTrackingStore<ByteArray, byte[], byte[]> createStore(String name) {
        Store<ByteArray, byte[], byte[]> store = inner.createStore(name);
        return new StatTrackingStore<ByteArray, byte[], byte[]>(store, null);
    }

    @Override
    public StatTrackingStore<ByteArray, byte[], byte[]> getStore() {
        return (StatTrackingStore<ByteArray, byte[], byte[]>) super.getStore();
    }

    @Override
    protected boolean supportsSizes(int keySize, int valueSize) {
        return valueSize <= (48 * 1024 * 1024);
    }

    protected Version getVersion(ByteArray key) {
        Version version = this.versions.get(key);
        if(version == null) {
            version = VersionFactory.newVersion();
            versions.put(key, version);
        }
        version.incrementClock(1);
        return version;
    }

    protected ByteArray getKey(int idx) {
        String key = Integer.toString(idx);
        return new ByteArray(key.getBytes());
    }

    public enum StoreCommand {
        PUT,
        GET,
        DELETE
    }

    private class ThreadedStore implements Runnable {

        private int threadId;
        private StoreCommand command;
        private long times[];
        StatTrackingStore<ByteArray, byte[], byte[]> store;

        public ThreadedStore(StatTrackingStore<ByteArray, byte[], byte[]> store,
                             StoreCommand command,
                             int threadId,
                             long[] times) {
            this.store = store;
            this.times = times;
            this.threadId = threadId;
            this.command = command;
        }

        public int getKeyIndex(int idx) {
            return random.nextInt(count);
        }

        public void putValue(ByteArray key) {
            Version version = getVersion(key); // Get the version
            byte[] data = new byte[1024]; // Get the data
            Versioned<byte[]> versioned = new Versioned<byte[]>(data, version);
            try {
                version = store.put(key, versioned, null); // Store the value
                versions.put(key, version); // Record the new version
            } catch(ObsoleteVersionException e) {
                versions.put(key, e.getExistingVersion());
            }
        }

        public void deleteValue(ByteArray key) {
            Version version = getVersion(key); // Get the version
            store.delete(key, version);
        }

        public void getValue(ByteArray key) {
            store.get(key, null); // Get the value
        }

        public void run() {
            for(int i = 0; i < times.length; i++) { // Loop on iterations
                int keyIdx = getKeyIndex(i); // Get the index of the key
                ByteArray key = getKey(keyIdx); // Get the key to save
                long start = System.currentTimeMillis(); // Get the start time
                switch(command) {
                    case PUT:
                        putValue(key);
                        break;
                    case GET:
                        getValue(key);
                        break;
                    case DELETE:
                        deleteValue(key);
                        break;
                }
                times[i] = System.currentTimeMillis() - start;// Record
                // the
                // time
            }
        }
    }

    protected void runThreads(Thread[] threads) {
        for(int threadIdx = 0; threadIdx < threads.length; threadIdx++) {
            threads[threadIdx].start();
        }
        for(int threadIdx = 0; threadIdx < threads.length; threadIdx++) {
            try {
                threads[threadIdx].join();
            } catch(Exception e) {}
        }
    }

    protected void populateStore() {
        populateStore(100);
    }

    protected void populateStore(int percent) {
        StatTrackingStore<ByteArray, byte[], byte[]> store = getStore();
        Thread[] threads = new Thread[numThreads];
        int initial = percent * iterations / 100; // What percent of the records
        // are already filled
        long times[][] = new long[numThreads][initial];
        for(int threadId = 0; threadId < numThreads; threadId++) {
            threads[threadId] = new Thread(new ThreadedStore(store,
                                                             StoreCommand.PUT,
                                                             threadId,
                                                             times[threadId]));
        }
        runThreads(threads);
        store.resetStatistics();
    }

    private void checkStats(StatTrackingStore<ByteArray, byte[], byte[]> store,
                            StoreCommand command,
                            long[][] times) {
        long total = 0;
        long completed = 0;
        for(int i = 0; i < times.length; i++) {
            completed += times[i].length;
            for(int j = 0; j < times[i].length; j++) {
                total += times[i][j];
            }
        }
    }

    private void testCommand(StoreCommand command, int percent, int threadCount, int numGets) {
        StatTrackingStore<ByteArray, byte[], byte[]> store = getStore();
        populateStore(percent);
        Thread[] threads = new Thread[threadCount];
        int passes = numGets / threadCount;
        long times[][] = new long[threadCount][passes];
        for(int threadId = 0; threadId < threadCount; threadId++) {
            threads[threadId] = new Thread(new ThreadedStore(store,
                                                             command,
                                                             threadId,
                                                             times[threadId]));
        }
        runThreads(threads);
        checkStats(store, command, times);
    }

    private void testGets(int percent, int threadCount, int numGets) {
        testCommand(StoreCommand.GET, percent, threadCount, numGets);
    }

    private void testPuts(int percent, int threadCount, int numPuts) {
        testCommand(StoreCommand.PUT, percent, threadCount, numPuts);
    }

    private void testDeletes(int percent, int threadCount, int numDeletes) {
        testCommand(StoreCommand.DELETE, percent, threadCount, numDeletes);
    }

    @Test
    public void testRandomGets() {
        testGets(100, numThreads, count / numThreads);
    }

    @Test
    public void testRandomPuts() {
        testPuts(50, numThreads, count / numThreads);
    }

    @Test
    public void testRandomUpdates() {
        testPuts(100, numThreads, count / numThreads);
    }

    @Test
    public void testRandomDeletes() {
        testDeletes(90, numThreads, count / numThreads);
    }
}
