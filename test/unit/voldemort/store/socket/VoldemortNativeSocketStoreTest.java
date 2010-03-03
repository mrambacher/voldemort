package voldemort.store.socket;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.client.protocol.RequestFormatType;

/**
 * Voldemort native socket store tests
 * 
 * @author jay
 * 
 */

@RunWith(Parameterized.class)
public class VoldemortNativeSocketStoreTest extends AbstractSocketStoreTest {

    public VoldemortNativeSocketStoreTest(RequestFormatType format, boolean useNio) {
        super(format, useNio);
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { RequestFormatType.VOLDEMORT_V1, true },
                { RequestFormatType.VOLDEMORT_V1, false },
                { RequestFormatType.VOLDEMORT_V3, true }, { RequestFormatType.VOLDEMORT_V3, false } });
    }

    @Override
    public boolean supportsMetadata() {
        return this.requestFormatType.getVersion() >= RequestFormatType.VOLDEMORT_V3.getVersion();
    }

}
