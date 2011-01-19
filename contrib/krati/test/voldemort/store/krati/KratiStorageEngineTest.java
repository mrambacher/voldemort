package voldemort.store.krati;

import java.io.File;

import krati.cds.impl.segment.MappedSegmentFactory;
import voldemort.TestUtils;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;

public class KratiStorageEngineTest extends AbstractStorageEngineTest {

    private File storeDir;

    public KratiStorageEngineTest() {
        super("storeName", KratiStorageConfiguration.TYPE_NAME);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        storeDir = TestUtils.createTempDir();
        storeDir.mkdirs();
        storeDir.deleteOnExit();
    }

    @Override
    public StorageEngine<ByteArray, byte[], byte[]> createStorageEngine(StoreDefinition storeDef) {
        return new KratiStorageEngine(storeDef,
                                      new MappedSegmentFactory(),
                                      10,
                                      10,
                                      0.75,
                                      0,
                                      storeDir);
    }

    @Override
    protected void closeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        engine.truncate();
        super.closeStorageEngine(engine);
    }

}
