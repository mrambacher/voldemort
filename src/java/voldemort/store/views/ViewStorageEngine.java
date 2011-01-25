package voldemort.store.views;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.annotations.Experimental;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.serialization.Serializer;
import voldemort.store.DelegatingStorageEngine;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

/**
 * Views are transformations of other stores
 * 
 * 
 */
@Experimental
public class ViewStorageEngine extends DelegatingStorageEngine<ByteArray, byte[], byte[]> {

    private final StoreDefinition viewDef;
    private final Store<Object, Object, Object> serializingStore;
    private final Serializer<Object> valSerializer;
    private final Serializer<Object> transformSerializer;
    private final Serializer<Object> targetKeySerializer;
    private final Serializer<Object> targetValSerializer;
    private final View<Object, Object, Object, Object> view;
    private final CompressionStrategy valueCompressionStrategy;

    @SuppressWarnings("unchecked")
    public ViewStorageEngine(StoreDefinition viewDef,
                             StorageEngine<ByteArray, byte[], byte[]> target,
                             Serializer<?> valSerializer,
                             Serializer<?> transformSerializer,
                             Serializer<?> targetKeySerializer,
                             Serializer<?> targetValSerializer,
                             CompressionStrategy valueCompressionStrategy,
                             View<?, ?, ?, ?> valueTrans) {
        super(target);
        this.viewDef = viewDef;
        this.serializingStore = new SerializingStore(target,
                                                     targetKeySerializer,
                                                     targetValSerializer,
                                                     null);
        this.valSerializer = (Serializer<Object>) valSerializer;
        this.transformSerializer = (Serializer<Object>) transformSerializer;
        this.targetKeySerializer = (Serializer<Object>) targetKeySerializer;
        this.targetValSerializer = (Serializer<Object>) targetValSerializer;
        this.view = (View<Object, Object, Object, Object>) valueTrans;
        this.valueCompressionStrategy = valueCompressionStrategy;
        if(valueTrans == null)
            throw new IllegalArgumentException("View without either a key transformation or a value transformation.");
    }

    @Override
    public StoreDefinition getStoreDefinition() {
        return viewDef;
    }

    private List<Versioned<byte[]>> inflateValues(List<Versioned<byte[]>> result) {
        List<Versioned<byte[]>> inflated = new ArrayList<Versioned<byte[]>>(result.size());
        for(Versioned<byte[]> item: result) {
            inflated.add(inflateValue(item));
        }
        return inflated;
    }

    private List<Versioned<byte[]>> deflateValues(List<Versioned<byte[]>> values) {
        List<Versioned<byte[]>> deflated = new ArrayList<Versioned<byte[]>>(values.size());
        for(Versioned<byte[]> item: values) {
            deflated.add(deflateValue(item));
        }
        return deflated;
    }

    private Versioned<byte[]> deflateValue(Versioned<byte[]> versioned) throws VoldemortException {
        byte[] deflatedData = null;
        try {
            deflatedData = valueCompressionStrategy.deflate(versioned.getValue());
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        return new Versioned<byte[]>(deflatedData, versioned.getVersion());
    }

    private Versioned<byte[]> inflateValue(Versioned<byte[]> versioned) throws VoldemortException {
        byte[] inflatedData = null;
        try {
            inflatedData = valueCompressionStrategy.inflate(versioned.getValue());
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
        return new Versioned<byte[]>(inflatedData, versioned.getVersion());
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        List<Versioned<byte[]>> values = super.get(key, null);

        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();

        if(valueCompressionStrategy != null)
            values = inflateValues(values);

        for(Versioned<byte[]> v: values) {
            results.add(new Versioned<byte[]>(valueToViewSchema(key, v.getValue(), transforms),
                                              v.getVersion()));
        }

        if(valueCompressionStrategy != null)
            results = deflateValues(results);

        return results;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        return StoreUtils.getAll(this, keys, transforms);
    }

    @Override
    public String getName() {
        return viewDef.getName();
    }

    @Override
    public Version put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        if(valueCompressionStrategy != null)
            value = inflateValue(value);
        Versioned<byte[]> result = Versioned.value(valueFromViewSchema(key,
                                                                       value.getValue(),
                                                                       transforms),
                                                   value.getVersion());
        if(valueCompressionStrategy != null)
            result = deflateValue(result);
        return super.put(key, result, null);
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(Collection<Integer> partitions,
                                                                        VoldemortFilter<ByteArray, byte[]> filter,
                                                                        byte[] transforms) {
        return new ViewIterator(super.entries(partitions, filter, null), transforms);
    }

    @Override
    public void truncate() {
        ViewIterator iterator = new ViewIterator(super.entries(null,
                                                               new DefaultVoldemortFilter<ByteArray, byte[]>(),
                                                               null),
                                                 null);
        while(iterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> pair = iterator.next();
            delete(pair.getFirst(), pair.getSecond().getVersion());
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.VIEW_TARGET)
            return this.getInnerEngine();
        else
            return super.getCapability(capability);
    }

    @Override
    public void close() throws VoldemortException {}

    private byte[] valueFromViewSchema(ByteArray key, byte[] value, byte[] transforms) {
        return this.targetValSerializer.toBytes(this.view.viewToStore(this.serializingStore,
                                                                      this.targetKeySerializer.toObject(key.get()),
                                                                      this.valSerializer.toObject(value),
                                                                      (transformSerializer != null && transforms != null) ? this.transformSerializer.toObject(transforms)
                                                                                                                         : null));
    }

    private byte[] valueToViewSchema(ByteArray key, byte[] value, byte[] transforms) {
        return this.valSerializer.toBytes(this.view.storeToView(this.serializingStore,
                                                                this.targetKeySerializer.toObject(key.get()),
                                                                this.targetValSerializer.toObject(value),
                                                                (transformSerializer != null && transforms != null) ? this.transformSerializer.toObject(transforms)
                                                                                                                   : null));
    }

    private class ViewIterator extends AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>
            implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private final ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> inner;
        private final byte[] transforms;

        public ViewIterator(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> inner,
                            byte[] transforms) {
            this.inner = inner;
            this.transforms = transforms;
        }

        public void close() {
            this.inner.close();
        }

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
            Pair<ByteArray, Versioned<byte[]>> p = inner.next();
            Versioned<byte[]> newVal = Versioned.value(valueToViewSchema(p.getFirst(),
                                                                         p.getSecond().getValue(),
                                                                         null),
                                                       p.getSecond().getVersion());
            return Pair.create(p.getFirst(), newVal);
        }
    }
}
