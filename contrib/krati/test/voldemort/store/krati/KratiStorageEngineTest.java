package voldemort.store.krati;

import java.io.File;

import voldemort.TestUtils;
import voldemort.server.VoldemortConfig;
import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;

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
    protected Props getServerProperties() {
        Props props = super.getServerProperties();
        props.with("voldemort.home", storeDir.getAbsolutePath());
        props.with("krati.segment.filesize.mb", 10);
        props.with("krati.lock.stripes", 10);
        props.with("krati.load.factor", 0.75);
        props.with("krati.initlevel", 2);

        return props;
    }

    @Override
    public StorageConfiguration createStorageConfiguration(VoldemortConfig config) {
        return new KratiStorageConfiguration(config);
    }

    @Override
    protected void closeStorageEngine(StorageEngine<ByteArray, byte[], byte[]> engine) {
        engine.truncate();
        super.closeStorageEngine(engine);
    }

}
