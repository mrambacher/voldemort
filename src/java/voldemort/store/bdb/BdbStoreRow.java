package voldemort.store.bdb;

import org.apache.log4j.Logger;

import voldemort.serialization.IdentitySerializer;
import voldemort.serialization.VersionedSerializer;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StoreRow;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.VersionFactory;
import voldemort.versioning.Versioned;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BdbStoreRow implements StoreRow {

    private static final Logger logger = Logger.getLogger(BdbStorageEngine.class);

    protected DatabaseEntry value = new DatabaseEntry();
    protected DatabaseEntry key = new DatabaseEntry();
    private static final VersionedSerializer<byte[]> serializer = new VersionedSerializer<byte[]>(new IdentitySerializer());
    protected Cursor cursor;
    private boolean ownsCursor = false;
    protected final LockMode lockMode;
    private boolean advanced = false;
    private boolean first = true;
    private boolean hasMore = false;

    public BdbStoreRow(Database db, LockMode mode) throws DatabaseException {
        this.cursor = db.openCursor(null, null);
        this.ownsCursor = true;
        this.lockMode = mode;
    }

    public BdbStoreRow(Cursor cursor, LockMode mode) {
        this.cursor = cursor;
        this.lockMode = mode;
    }

    @Override
    protected void finalize() {
        close();
    }

    public void remove() {
        if(cursor == null) {
            throw new PersistenceFailureException("Call to remove() on a closed iterator.");
        } else if(advanced) {
            throw new PersistenceFailureException("Call to remove() on an advanced iterator.");
        } else {
            try {
                cursor.delete();
            } catch(DatabaseException e) {
                logger.error(e);
                throw new PersistenceFailureException(e);
            }
        }
    }

    public void close() {
        if(cursor != null && ownsCursor) {
            try {
                cursor.close();
                cursor = null;
            } catch(DatabaseException e) {
                logger.error(e);
            }
        }
    }

    protected OperationStatus getFirst(DatabaseEntry key, DatabaseEntry value, LockMode mode)
            throws DatabaseException {
        return cursor.getFirst(key, value, mode);
    }

    protected OperationStatus getNext(DatabaseEntry key, DatabaseEntry value, LockMode mode)
            throws DatabaseException {
        return cursor.getNext(key, value, mode);
    }

    public boolean advance() {
        try {
            if(advanced) {
                advanced = false;
                return true;
            } else {
                OperationStatus status;
                if(first) {
                    status = getFirst(key, value, lockMode);
                    first = false;
                } else {
                    status = getNext(key, value, lockMode);
                }
                return status == OperationStatus.SUCCESS;
            }
        } catch(DatabaseException e) {
            throw new PersistenceFailureException("Failed to advance cursor", e);
        }
    }

    public boolean hasNext() {
        if(cursor == null) {
            return false;
        } else if(!advanced) {
            hasMore = this.advance();
            advanced = hasMore;
        }
        return hasMore;
    }

    public Versioned<byte[]> getVersioned() {
        return serializer.toObject(value.getData());
    }

    public Version getVersion() {
        return VersionFactory.toVersion(value.getData());
    }

    public ByteArray getKey() {
        return new ByteArray(key.getData());
    }
}
