package voldemort.store.limiting;

import voldemort.VoldemortException;

public class LimitExceededException extends VoldemortException {
    private static final long serialVersionUID = 1L;

    public LimitExceededException(String message) {
        super(message);
    }
}
