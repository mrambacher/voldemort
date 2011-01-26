package voldemort.store.krati;

import java.io.File;

import krati.cds.impl.segment.MappedSegmentFactory;
import krati.cds.impl.segment.SegmentFactory;
import voldemort.server.VoldemortConfig;
import voldemort.store.AbstractStorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;

public class KratiStorageConfiguration extends AbstractStorageConfiguration {

    public static final String TYPE_NAME = "krati";

    private final String dataDirectory;
    private final int lockStripes;
    private final int segmentFileSizeMb;
    private final int initLevel;
    private final double hashLoadFactor;
    private final Object lock = new Object();
    private final Class<?> factoryClass;

    public KratiStorageConfiguration(VoldemortConfig config) {
        super(config);
        Props props = config.getAllProps();
        File kratiDir = new File(config.getDataDirectory(), "krati");
        kratiDir.mkdirs();
        this.dataDirectory = kratiDir.getAbsolutePath();
        this.segmentFileSizeMb = props.getInt("krati.segment.filesize.mb", 256);
        this.hashLoadFactor = props.getDouble("krati.load.factor", 0.75);
        this.initLevel = props.getInt("krati.initlevel", 2);
        this.lockStripes = props.getInt("krati.lock.stripes", 50);
        this.factoryClass = ReflectUtils.loadClass(props.getString("krati.segment.factory.class",
                                                                   MappedSegmentFactory.class.getName()));
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef) {
        synchronized(lock) {
            String storeName = storeDef.getName();
            File storeDir = new File(dataDirectory, storeName);
            if(!storeDir.exists()) {
                logger.info("Creating krati data directory '" + storeDir.getAbsolutePath() + "'.");
                storeDir.mkdirs();
            }

            SegmentFactory segmentFactory = (SegmentFactory) ReflectUtils.callConstructor(factoryClass);
            return new KratiStorageEngine(storeDef,
                                          this,
                                          segmentFactory,
                                          segmentFileSizeMb,
                                          lockStripes,
                                          hashLoadFactor,
                                          initLevel,
                                          storeDir);
        }
    }

    public String getType() {
        return TYPE_NAME;
    }

}
