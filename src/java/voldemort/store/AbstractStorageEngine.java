/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Portion Copyright 2010 Nokia Corporation. All rights reserved.
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

package voldemort.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.routing.RoutingStrategy;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableFilterIterator;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public abstract class AbstractStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    protected final Logger logger = Logger.getLogger(getClass());
    private static final Hex hexCodec = new Hex();
    private final StoreDefinition storeDef;

    protected AbstractStorageEngine(StoreDefinition def) {
        this.storeDef = Utils.notNull(def);
    }

    public StoreDefinition getStoreDefinition() {
        return storeDef;
    }

    public String getName() {
        return storeDef.getName();
    }

    abstract protected StoreTransaction<Version> startTransaction(ByteArray key)
            throws PersistenceFailureException;

    abstract protected StoreRow getRowsForKey(ByteArray key) throws PersistenceFailureException;

    protected ClosableIterator<Versioned<byte[]>> getVersionedIterator(ByteArray key)
            throws PersistenceFailureException {
        StoreRow rows = getRowsForKey(key);
        return new StoreVersionedIterator(rows);
    }

    protected ClosableIterator<Version> getVersionIterator(ByteArray key)
            throws PersistenceFailureException {
        StoreRow rows = getRowsForKey(key);
        return new StoreVersionIterator(rows);
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        List<Version> results = Lists.newArrayList();
        ClosableIterator<Version> iter = null;
        try {
            iter = getVersionIterator(key);
            while(iter.hasNext()) {
                results.add(iter.next());
            }
        } finally {
            attemptClose(iter);
        }
        return results;
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        ClosableIterator<Versioned<byte[]>> iter = null;
        try {
            List<Versioned<byte[]>> results = Lists.newArrayList();
            iter = getVersionedIterator(key);
            while(iter.hasNext()) {
                results.add(iter.next());
            }
            return results;
        } finally {
            attemptClose(iter);
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils.newEmptyHashMap(keys);
        for(ByteArray key: keys) {

            List<Versioned<byte[]>> values;
            if(transforms != null) {
                values = get(key, transforms.get(key));
            } else {
                values = get(key, null);
            }
            if(!values.isEmpty()) {
                result.put(key, values);
            }
        }
        return result;
    }

    public Version put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);

        boolean succeeded = false;
        StoreTransaction<Version> transaction = null;
        try {
            transaction = startTransaction(key);
            StoreIterator<Version> iter = transaction.getIterator();
            boolean updated = false;
            // Check existing values
            // if there is a version obsoleted by this value delete it
            // if there is a version later than this one, throw an exception
            while(iter.hasNext()) {
                Version clock = iter.next();
                Occured occured = value.getVersion().compare(clock);
                if(occured == Occured.BEFORE) {
                    throw new ObsoleteVersionException("Key "
                                                               + new String(hexCodec.encode(key.get()))
                                                               + " "
                                                               + value.getVersion().toString()
                                                               + " is obsolete, it is no greater than the current version of "
                                                               + clock + ".",
                                                       clock);
                } else if(occured == Occured.AFTER) {
                    if(updated) {
                        // best effort delete of obsolete previous value!
                        iter.remove();
                    } else {
                        transaction.update(iter, value);
                        updated = true;
                    }
                }
            }

            // Okay so we cleaned up all the prior stuff, so now we are good to
            // insert the new thing
            if(!updated) {
                transaction.insert(iter, value);
            }
            succeeded = true;
            return value.getVersion();
        } catch(PersistenceFailureException e) {
            logger.error(e);
            throw e;
        } finally {
            // attemptClose(iter);
            attemptClose(transaction, succeeded);
        }
    }

    public boolean delete(ByteArray key, Version version) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        boolean deletedSomething = false;
        boolean success = false;
        StoreTransaction<Version> transaction = startTransaction(key);
        ClosableIterator<Version> iter = transaction.getIterator();
        try {
            while(iter.hasNext()) {
                Version stored = iter.next();
                if(stored.compare(version) == Occured.BEFORE) {
                    iter.remove();
                    deletedSomething = true;
                }
            }
            success = true;
            return deletedSomething;
        } catch(PersistenceFailureException e) {
            logger.error(e);
            throw new PersistenceFailureException(e);
        } finally {
            attemptClose(iter);
            attemptClose(transaction, success);
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public ClosableIterator<ByteArray> keys(ClosableIterator<ByteArray> iter,
                                            final VoldemortFilter filter) {
        if(filter == null) {
            // If there is no filter, return the input
            return iter;
        } else {
            return new ClosableFilterIterator<ByteArray>(iter) {

                @Override
                public boolean matches(ByteArray key) {
                    return filter.accept(key, null);
                }
            };
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter,
                                                                        final VoldemortFilter filter) {
        if(filter == null) {
            // If there is no filter, return the input
            return iter;
        } else {
            return new ClosableFilterIterator<Pair<ByteArray, Versioned<byte[]>>>(iter) {

                @Override
                public boolean matches(Pair<ByteArray, Versioned<byte[]>> entry) {
                    return filter.accept(entry.getFirst(), entry.getSecond());
                }
            };
        }
    }

    public ClosableIterator<ByteArray> keys(ClosableIterator<ByteArray> iter,
                                            final Collection<Integer> partitions) {
        // If there are no partitions, just return the input value
        if(partitions == null || partitions.size() <= 0) {
            return iter;
        } else {
            final RoutingStrategy strategy = storeDef.getRoutingStrategy();
            return new ClosableFilterIterator<ByteArray>(iter) {

                @Override
                public boolean matches(ByteArray key) {
                    int partition = strategy.getPrimaryPartition(key.get());
                    return partitions.contains(partition);
                }
            };
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iter,
                                                                        final Collection<Integer> partitions) {
        // If there are no partitions, just return the input value
        if(partitions == null || partitions.size() <= 0) {
            return iter;
        } else {
            final RoutingStrategy strategy = storeDef.getRoutingStrategy();
            return new ClosableFilterIterator<Pair<ByteArray, Versioned<byte[]>>>(iter) {

                @Override
                public boolean matches(Pair<ByteArray, Versioned<byte[]>> entry) {
                    ByteArray key = entry.getFirst();
                    int partition = strategy.getPrimaryPartition(key.get());
                    return partitions.contains(partition);
                }
            };
        }
    }

    protected <T> void attemptClose(StoreTransaction<T> transaction, boolean success) {
        try {
            if(transaction != null) {
                transaction.close(success);
            }
        } catch(Exception e) {
            logger.error("Close failed!", e);
        }
    }

    protected <T> void attemptClose(ClosableIterator<T> iter) {
        try {
            if(iter != null)
                iter.close();
        } catch(Exception e) {
            logger.error("Close failed!", e);
        }
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(o == null || !Store.class.isAssignableFrom(o.getClass()))
            return false;
        Store<?, ?, ?> s = (Store<?, ?, ?>) o;
        return s.getName().equals(s.getName());
    }
}
