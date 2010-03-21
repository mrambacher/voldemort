package voldemort.store.bdb;

import voldemort.utils.ByteArray;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class BdbStoreKeyedRow extends BdbStoreRow {

    public BdbStoreKeyedRow(ByteArray searchKey, Database db, LockMode mode)
                                                                            throws DatabaseException {
        super(db, mode);
        this.key.setData(searchKey.get());
    }

    public BdbStoreKeyedRow(ByteArray searchKey, Cursor cursor, LockMode mode) {
        super(cursor, mode);
        this.key.setData(searchKey.get());
    }

    @Override
    protected OperationStatus getFirst(DatabaseEntry key, DatabaseEntry value, LockMode mode)
            throws DatabaseException {
        return cursor.getSearchKey(key, value, mode);
    }

    @Override
    protected OperationStatus getNext(DatabaseEntry key, DatabaseEntry value, LockMode mode)
            throws DatabaseException {
        return cursor.getNextDup(key, value, mode);
    }
}
