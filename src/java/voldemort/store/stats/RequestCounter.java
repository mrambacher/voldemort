package voldemort.store.stats;

import java.util.concurrent.atomic.AtomicReference;

import javax.management.AttributeChangeNotification;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import voldemort.utils.Time;

/**
 * A thread-safe request counter that calculates throughput for a specified
 * duration of time.
 * 
 * 
 */
public class RequestCounter extends NotificationBroadcasterSupport implements RequestCounterMBean {

    /**
     * Used to divide the TimeNS that a transaction took. The result will be
     * used as index into an array that keeps track of the total number of tx.
     * For example a value of: 34000 ns represents index=3 (int)(34000/10^4)
     * 38000 ns represents index=3 (int)(38000/10^4) 375000 ns represents
     * index=37 (int)(375000/10^4)
     * 
     * Basically this will knock down 5 digits from time NS. I doubt anyone
     * cares to be more accurate than 1ms, so that would be a reasonable
     * default.
     * 
     */
    private final static int DISTRIBUTION_FACTOR = 10000;

    /**
     * Normalize the index back to millis.
     */
    private final static float NORMILIZE_TO_MILLIS = DISTRIBUTION_FACTOR / 1000000.0F;

    /**
     * Separator use in the message format sent as part of a notification event.
     */
    private final static String CSV = ", ";

    /**
     * Sequential number associated to the message being sent as a result of
     * firing a notification event.
     */
    private long sequenceMsgId;

    private final AtomicReference<Accumulator> values;
    private final int durationMS;

    /**
     * @param durationMS specifies for how long you want to maintain this
     *        counter (in milliseconds).
     */
    public RequestCounter(int durationMS) {
        this.values = new AtomicReference<Accumulator>(new Accumulator());
        this.durationMS = durationMS;
    }

    public long getCount() {
        return getValidAccumulator().count;
    }

    public long getTotalCount() {
        return getValidAccumulator().total;
    }

    public float getThroughput() {
        Accumulator oldv = getValidAccumulator();
        double elapsed = (System.currentTimeMillis() - oldv.startTimeMS)
                         / (double) Time.MS_PER_SECOND;
        if(elapsed > 0f) {
            return (float) (oldv.count / elapsed);
        } else {
            return -1f;
        }
    }

    public String getDisplayThroughput() {
        return String.format("%.2f", getThroughput());
    }

    public double getAverageTimeInMs() {
        return getValidAccumulator().getAverageTimeNS() / Time.NS_PER_MS;
    }

    public String getDisplayAverageTimeInMs() {
        return String.format("%.4f", getAverageTimeInMs());
    }

    public int getDuration() {
        return durationMS;
    }

    private Accumulator getValidAccumulator() {
        Accumulator accum = values.get();
        long now = System.currentTimeMillis();

        /*
         * if still in the window, just return it
         */
        if(now - accum.startTimeMS <= durationMS) {
            return accum;
        }

        /*
         * try to set. if we fail, then someone else set it, so just return that
         * new one
         */

        Accumulator newWithTotal = accum.newWithTotal();

        if(values.compareAndSet(accum, newWithTotal)) {
            // The time-window has expired. Process the notification
            final Accumulator periodAccum = accum.deepCopy();
            final String notificationMsg = createNotificationMsg(periodAccum);
            sendNotification(notificationMsg);
            return newWithTotal;
        }

        return values.get();
    }

    /*
     * Updates the stats accumulator with another operation. We need to make
     * sure that the request is only added to a non-expired pair. If so, start a
     * new counter pair with recent time. We'll only try to do this 3 times - if
     * other threads keep modifying while we're doing our own work, just bail.
     * 
     * @param timeNS time of operation, in nanoseconds
     */
    public void addRequest(long timeNS) {

        int index = (int) (timeNS / DISTRIBUTION_FACTOR);

        for(int i = 0; i < 3; i++) {
            Accumulator oldv = getValidAccumulator();

            long startTimeMS = oldv.startTimeMS;
            long count = oldv.count + 1;
            long totalTimeNS = oldv.totalTimeNS + timeNS;
            long total = oldv.total + 1;
            int[] latencies = oldv.latencies;

            // Updates the counter that keeps track of the number of
            // transactions that took the same amount of time.
            //
            // I doubt anyone cares to be more accurate than 1ms, so that would
            // be a
            // reasonable default
            //
            // If the index is bigger than Accumulator.MAX_ARRAY it means that
            // the
            // transaction took such a big time that can be considered a rare
            // case,
            // For example, let's consider:
            //
            // timeNS = 10.000.000.000 (10 seconds)
            // index = timeNS / DISTRIBUTION_FACTOR
            // index = 10.000.000.000 / 10.000
            // index = 1.000.000
            if(index < Accumulator.MAX_ARRAY) {
                latencies[index] = getSafeNextCounter(latencies[index]);
            } else {
                // Put this value in the last bucket.
                latencies[Accumulator.MAX_ARRAY] = getSafeNextCounter(latencies[Accumulator.MAX_ARRAY]);
            }
            // System.out.println("timeNS:" + timeNS + ", latencies[" +index +
            // "]:" + latencies[index]);

            if(values.compareAndSet(oldv, new Accumulator(startTimeMS,
                                                          count,
                                                          totalTimeNS,
                                                          total,
                                                          latencies))) {
                return;
            }
        }
    }

    private int getSafeNextCounter(int value) {
        // reset the counter if maximum value is reached, unlikely to
        // happen...
        int nextValue = value + 1;
        if(nextValue < 2147483640)
            return nextValue;
        else
            return 0;
    }

    /**
     * Creates a notification message that is about to be sent as a result of
     * firing the event.
     * 
     * @param expiredAccum Accumulator that has expired.
     * 
     * @return notification message.
     */
    private String createNotificationMsg(final Accumulator expiredAccum) {
        if(expiredAccum == null)
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append("startPeriodMS=").append(expiredAccum.startTimeMS).append(CSV);
        sb.append("stopPeriodMS=").append(System.currentTimeMillis()).append(CSV);
        sb.append("count=").append(expiredAccum.count).append(CSV);
        sb.append("total=").append(expiredAccum.total).append(CSV);
        sb.append("totalTimeNS=").append(expiredAccum.totalTimeNS).append(CSV);
        sb.append("latency=")
          .append(getAllLatency(expiredAccum.latencies, expiredAccum.total))
          .append(CSV);
        return sb.toString();
    }

    private void sendNotification(String notificationMsg) {
        Notification n = new AttributeChangeNotification(this,
                                                         sequenceMsgId++,
                                                         System.currentTimeMillis(),
                                                         notificationMsg,
                                                         "reset",
                                                         "String",
                                                         "nn",
                                                         notificationMsg);

        // Now send the notification using the sendNotification method
        // inherited from the parent class NotificationBroadcasterSupport.
        sendNotification(n);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        String[] types = new String[] { AttributeChangeNotification.ATTRIBUTE_CHANGE };
        String name = AttributeChangeNotification.class.getName();
        String description = "An attribute of this MBean has changed";
        MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description);
        return new MBeanNotificationInfo[] { info };
    }

    private static class Accumulator {

        public final static int MAX_ARRAY = 100000;
        private final static int MAX_ARRAY_PLUS_ONE = MAX_ARRAY + 1;
        final int[] latencies;
        final long startTimeMS;
        final long count;
        final long totalTimeNS;
        final long total;

        public Accumulator() {
            this(System.currentTimeMillis(), 0, 0, 0, new int[MAX_ARRAY_PLUS_ONE]);
        }

        public Accumulator newWithTotal() {
            return new Accumulator(System.currentTimeMillis(),
                                   0,
                                   0,
                                   total,
                                   new int[MAX_ARRAY_PLUS_ONE]);
        }

        public Accumulator deepCopy() {
            return new Accumulator(startTimeMS, count, totalTimeNS, total, latencies);
        }

        public Accumulator(long startTimeMS,
                           long count,
                           long totalTimeNS,
                           long total,
                           int[] latencies) {
            this.startTimeMS = startTimeMS;
            this.count = count;
            this.totalTimeNS = totalTimeNS;
            this.total = total;
            this.latencies = latencies;
        }

        public double getAverageTimeNS() {
            return count > 0 ? 1f * totalTimeNS / count : -0f;
        }
    }

    private int findIndexByPercentage(final float percentage, final long totalTx, int[] latencies) {
        float countTx = (totalTx * percentage);
        long found = 0;
        for(int index = 0; index < Accumulator.MAX_ARRAY; index++) {
            found = latencies[index] + found;
            if(found > countTx) {
                return index;
            }
        }

        return 0;
    }

    public String getLatency(final float percentile) {
        final Accumulator accum = getValidAccumulator();
        final int index = findIndexByPercentage(percentile, accum.count, accum.latencies);
        final float p = index * NORMILIZE_TO_MILLIS;
        return String.format("%.2f", p);
    }

    private String getAllLatency(final int[] latencies, long totalTx) {
        int index50 = findIndexByPercentage(0.50F, totalTx, latencies);
        int index90 = findIndexByPercentage(0.90F, totalTx, latencies);
        int index99 = findIndexByPercentage(0.99F, totalTx, latencies);
        int index995 = findIndexByPercentage(0.995F, totalTx, latencies);
        int index999 = findIndexByPercentage(0.999F, totalTx, latencies);

        // Calculate the percentiles in Millis.
        float p50MS = index50 * NORMILIZE_TO_MILLIS;
        float p90MS = index90 * NORMILIZE_TO_MILLIS;
        float p99MS = index99 * NORMILIZE_TO_MILLIS;
        float p995MS = index995 * NORMILIZE_TO_MILLIS;
        float p999MS = index999 * NORMILIZE_TO_MILLIS;

        return "50=" + String.format("%.2f", p50MS) + ", 90=" + String.format("%.2f", p90MS)
               + ", 99=" + String.format("%.2f", p99MS) + ", 99.5=" + String.format("%.2f", p995MS)
               + ", 99.9=" + String.format("%.2f", p999MS);
    }

}
