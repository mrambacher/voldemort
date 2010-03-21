package voldemort.store.limiting;

import org.junit.Test;

import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class LimitingStoreTest extends AbstractByteArrayStoreTest {
    public LimitingStoreTest() {
        super("test");
    }

    @Override
    public Store<ByteArray, byte[]> createStore(String name) {
        Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>(name);
        return new LimitingStore(store, 300, 30 * 1024 * 1024, 200);
    }

    @Test
    @Override
    public void testSixtyMegabyteSizes() {
        testValueSizes("250-byte keys and with value size = 60*1024*1024 bytes (60 MB).",
                60 * 1024 * 1024,
                250, LimitExceededException.class);
    }

    @Test
    public void testLargeKeySize() {
        testValueSizes("1-K keys and with value size = 1024 bytes.",
                1024,
                1024, LimitExceededException.class);
    }

    @Test
    public void testLargeMetadata() {
        String key = "123";
        byte[] data = key.getBytes();
        Versioned<byte[]> value = new Versioned<byte[]>(data);
        try {
            for (int i = 0; i < 100; i++) {
                String name = Integer.toString(i);
                value.getMetadata().setProperty(name, name);
            }
            Store<ByteArray, byte[]> store = getStore();
            store.put(new ByteArray(data), value);
            fail("Expected metadata size limit exceeded");
        } catch (Exception e) {
            assertEquals("Unexpected exception", LimitExceededException.class, e.getClass());
        }
    }
}
