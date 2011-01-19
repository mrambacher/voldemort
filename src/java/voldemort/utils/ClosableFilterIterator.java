package voldemort.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

abstract public class ClosableFilterIterator<R> implements ClosableIterator<R> {

    private Iterator<R> iter;
    private R current;

    protected ClosableFilterIterator(Iterator<R> iter) {
        this.iter = iter;
        this.current = null;
    }

    public void close() {
        if(iter instanceof ClosableIterator) {
            ClosableIterator<R> closable = (ClosableIterator<R>) iter;
            closable.close();
        }
    }

    public R next() {
        if(hasNext()) {
            R result = current;
            current = null;
            return result;
        } else {
            throw new NoSuchElementException("Called next() when no more entries exist");
        }
    }

    abstract protected boolean matches(R match);

    public boolean hasNext() {
        if(current != null) {
            return true;
        } else {
            while(iter.hasNext()) {
                R next = iter.next();
                if(matches(next)) {
                    current = next;
                    return true;
                }
            }
        }
        return false;
    }

    public void remove() {
        throw new UnsupportedOperationException("Remove() not supported by filtering iterator");
    }
}
