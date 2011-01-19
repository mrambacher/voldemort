package voldemort.store.memory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class InMemoryStore<K, V, T> implements Store<K, V, T> {

    final ConcurrentMap<K, List<Versioned<V>>> map;
    private final String name;

    public static <K, V, T> InMemoryStore<K, V, T> create(String name) {
        return new InMemoryStore<K, V, T>(name);
    }

    public InMemoryStore(String name) {
        this.name = Utils.notNull(name);
        this.map = new ConcurrentHashMap<K, List<Versioned<V>>>();
    }

    public InMemoryStore(String name, ConcurrentMap<K, List<Versioned<V>>> map) {
        this.name = Utils.notNull(name);
        this.map = Utils.notNull(map);
    }

    public void deleteAll() {
        this.map.clear();
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

    public boolean delete(K key, Version version) {
        StoreUtils.assertValidKey(key);

        if(version == null)
            return map.remove(key) != null;

        List<Versioned<V>> values = map.get(key);
        if(values == null) {
            return false;
        }
        synchronized(values) {
            boolean deletedSomething = false;
            Iterator<Versioned<V>> iterator = values.iterator();
            while(iterator.hasNext()) {
                Versioned<V> item = iterator.next();
                if(item.getVersion().compare(version) == Occured.BEFORE) {
                    iterator.remove();
                    deletedSomething = true;
                }
            }
            if(values.size() == 0) {
                // If this remove fails, then another delete operation got
                // there before this one
                if(!map.remove(key, values))
                    return false;
            }

            return deletedSomething;
        }
    }

    public List<Version> getVersions(K key) {
        return StoreUtils.getVersions(get(key, null));
    }

    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> results = map.get(key);
        if(results == null) {
            return new ArrayList<Versioned<V>>(0);
        }
        synchronized(results) {
            return new ArrayList<Versioned<V>>(results);
        }
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, transforms);
    }

    public Version put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        Version version = value.getVersion();
        boolean success = false;
        while(!success) {
            List<Versioned<V>> items = map.get(key);
            // If we have no value, optimistically try to add one
            if(items == null) {
                items = new ArrayList<Versioned<V>>();
                items.add(value.cloneVersioned());
                success = map.putIfAbsent(key, items) == null;
            } else {
                synchronized(items) {
                    // if this check fails, items has been removed from the map
                    // by delete, so we try again.
                    if(map.get(key) != items)
                        continue;

                    // Check for existing versions - remember which items to
                    // remove in case of success
                    List<Versioned<V>> itemsToRemove = new ArrayList<Versioned<V>>(items.size());
                    for(Versioned<V> versioned: items) {
                        Occured occured = value.getVersion().compare(versioned.getVersion());
                        if(occured == Occured.BEFORE) {
                            throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                                       + "': " + value.getVersion(),
                                                               versioned.getVersion());
                        } else if(occured == Occured.AFTER) {
                            itemsToRemove.add(versioned);
                        }
                    }
                    items.removeAll(itemsToRemove);
                    items.add(value.cloneVersioned());
                }
                success = true;
            }
        }
        return version;
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return toString(15);
    }

    public String toString(int size) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int count = 0;
        for(Entry<K, List<Versioned<V>>> entry: map.entrySet()) {
            if(count > size) {
                builder.append("...");
                break;
            }
            builder.append(entry.getKey());
            builder.append(':');
            builder.append(entry.getValue());
            builder.append(',');
        }
        builder.append('}');
        return builder.toString();
    }

    public void close() {
        // nothing to do here
    }
}
