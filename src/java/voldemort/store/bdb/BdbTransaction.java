package voldemort.store.bdb;

import java.util.ArrayList;
import java.util.List;

import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.VersionedSerializer;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreIterator;
import voldemort.store.StoreTransaction;
import voldemort.store.StoreVersionIterator;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BdbTransaction implements StoreTransaction<Version> {

    private static final VersionedSerializer<byte[]> serializer = new VersionedSerializer<byte[]>(new IdentitySerializer());
    private Transaction transaction;
    private Cursor cursor;
    private ByteArray key;
    private List<Versioned<byte[]>> updates;

    public BdbTransaction(Environment environment, Database database, ByteArray key)
                                                                                    throws DatabaseException {
        this.transaction = environment.beginTransaction(null, null);
        this.cursor = database.openCursor(transaction, null);
        this.key = key;
        this.updates = null;
    }

    public StoreIterator<Version> getIterator() throws PersistenceFailureException {
        BdbStoreRow rows = new BdbStoreKeyedRow(key, cursor, LockMode.RMW);
        return new StoreVersionIterator(rows);
    }

    @Override
    protected void finalize() {
        closeCursor();
    }

    private void closeCursor() {
        try {
            if(cursor != null) {
                cursor.close();
            }
        } catch(DatabaseException e) {

        }
        cursor = null;
    }

    public static void commitOrAbort(Transaction transaction, boolean commitIt)
            throws PersistenceFailureException {
        if(transaction != null) {
            try {
                if(commitIt) {
                    transaction.commit();
                } else {
                    transaction.abort();
                }
            } catch(DatabaseException e) {
                throw new PersistenceFailureException(e);
            }
        }
    }

    public void commitOrAbort(boolean commitIt) throws PersistenceFailureException {
        commitOrAbort(this.transaction, commitIt);
    }

    public void close(boolean commitIt) throws PersistenceFailureException {
        try {
            if(commitIt && updates != null) {
                for(Versioned<byte[]> update: updates) {
                    this.insert(this.cursor, update);
                }
                updates.clear();
            }
            closeCursor();
            commitOrAbort(commitIt);
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        }
    }

    private void insert(Cursor cursor, Versioned<byte[]> value) throws PersistenceFailureException {
        try {
            DatabaseEntry valueEntry = new DatabaseEntry(serializer.toBytes(value));
            DatabaseEntry keyEntry = new DatabaseEntry(key.get());
            OperationStatus status = cursor.put(keyEntry, valueEntry);
            if(status != OperationStatus.SUCCESS) {
                throw new PersistenceFailureException("Put operation failed with status: " + status);
            }
        } catch(DatabaseException e) {
            throw new PersistenceFailureException(e);
        }
    }

    public void insert(StoreIterator<Version> iter, Versioned<byte[]> value)
            throws PersistenceFailureException {
        insert(cursor, value);
    }

    public void update(StoreIterator<Version> iter, Versioned<byte[]> value)
            throws PersistenceFailureException {
        if(updates == null) {
            updates = new ArrayList<Versioned<byte[]>>();
        }
        iter.remove();
        this.updates.add(value);
    }
}