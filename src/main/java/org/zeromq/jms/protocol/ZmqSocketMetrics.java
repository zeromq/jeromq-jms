package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.Serializable;

/**
 * Metric functionality for Zero MQ Sockets. The metric contains a total count for send and review messages,
 * but also a bucket of counters to enable temporal counting.
 */
public class ZmqSocketMetrics implements Serializable {

    private static final long serialVersionUID = 3377715287843053265L;
    private final String addr;

    private ZmqSocketStatus status;

    private long sendCount = 0;
    private long receiveCount = 0;

    private long sendTime;
    private long receiveTime;

    private final int bucketCount;
    private final int bucketInterval;

    private int sendBucketIndex = 0;
    private int receiveBucketIndex = 0;

    private final long[] sendBucketCounts;
    private final long[] receiveBucketCounts;

    /**
     * Construct the metric based on what is going to be logged.
     * @param addr            the address
     * @param bucketCount     the number of counter buckets
     * @param bucketInterval  the bucket interval in milliseconds
     * @param logSend         the log send indicator
     * @param logReceive      the log receive indicator
     */
    public ZmqSocketMetrics(final String addr, final int bucketCount, final int bucketInterval, final boolean logSend, final boolean logReceive) {
        this.addr = addr;

        this.bucketCount = bucketCount;
        this.bucketInterval = bucketInterval;
        this.sendBucketCounts = (logSend) ? new long[bucketCount] : null;
        this.receiveBucketCounts = (logReceive) ? new long[bucketCount] : null;
    }

    /**
     * Reset the buckets between now and last increment.
     * @param index         the current index based on current time
     * @param bucketIndex   the last bucket index ever incremented
     * @param bucketCounts  the bucket counts
     */
    private void resetBuckets(final int index, final int bucketIndex, final long[] bucketCounts) {
        // NOTE: this routine is not thread safe.
        int skipCount = index - bucketIndex;

        if (skipCount < 0) {
            skipCount = bucketCount - bucketIndex + index;
        }

        for (int i = 0; i < skipCount; i++) {
            final int skipIndex = (((bucketIndex + skipCount) % bucketCount) + bucketCount) % bucketCount;
            bucketCounts[skipIndex] = 0;
        }
    }

    /**
     * Increment the bucker counter based on time, and previous index.
     * @param currentTime   the current time
     * @param bucketIndex   the previous bucket index
     * @param bucketCounts  the bucket counts
     * @return              return the new bucket index (can be the same and previous depending on interval size).
     */
    protected int increment(final long currentTime, final int bucketIndex, final long[] bucketCounts) {
        final int index = (int) ((currentTime / bucketInterval) % bucketCount);

        // New index so reset bucket counter(s)
        if (bucketIndex != index) {
            resetBuckets(index, bucketIndex, bucketCounts);
        }

        bucketCounts[index]++;

        return index;
    }

    /**
     * @return  return the address of the socket
     */
    public String getAddr() {
        return addr;
    }

    /**
     * @return  return the audience
     */
    public boolean isActive() {
        return status == ZmqSocketStatus.ERROR;
    }

    /**
     * @return  return the current status for the socket, i.e. RUNNING ,STOPPED, etc...
     */
    public ZmqSocketStatus getStatus() {
        return status;
    }

    /**
     * Set the current status for the socket, i.e. RUNNING ,STOPPED, etc...
     * @param status  the new status
     */
    public void setStatus(final ZmqSocketStatus status) {
        this.status = status;
    }

    /**
     * @return  return the message sent count
     */
    public long getSendCount() {
        return sendCount;
    }

    /**
     * @return  return the last time a message was sent
     */
    public long getLastSendTime() {
        return sendTime;
    }

    /**
     * Increment the message sent count.
     */
    public void incrementSend() {
        if (sendBucketCounts == null) {
            throw new IllegalStateException("Socket statistics no setup to count sends.");
        }

        sendTime = System.currentTimeMillis();
        sendCount++;

        synchronized (sendBucketCounts) {
            sendBucketIndex = increment(sendTime, sendBucketIndex, sendBucketCounts);
        }
    }

    /**
     * @return  return the message received count
     */
    public long getReceiveCount() {
        return receiveCount;
    }

    /**
     * @return  return the last time a message was received
     */
    public long getLastReceiveTime() {
        return receiveTime;
    }

    /**
     * Increment the message received count.
     */
    public synchronized void incrementReceive() {
        if (receiveBucketCounts == null) {
            throw new IllegalStateException("Socket statistics no setup to count receives.");
        }

        receiveTime = System.currentTimeMillis();
        receiveCount++;

        synchronized (receiveBucketCounts) {
            receiveBucketIndex = increment(receiveTime, receiveBucketIndex, receiveBucketCounts);
        }
    }

    /**
     * @return  return the bucket interval (milliseconds)
     */
    public int getBucketInternval() {
        return bucketInterval;
    }

    /**
     * Return the temporal ascending order of historic bucket counts were the first is the current bucket count, and the last is the
     * last bucket count, and the index is the starting millisecond start point, i.e. 0, 5000, 10000, etc...
     * for a bucket interval of 5000 milliseconds.
     * @param bucketIndex   the current bucket index being fill (first bucket in the map)
     * @param bucketCounts  the bucket counts
     * @return              return a temporal map of bucket counts, 1, 30000, 60000, etc...
     */
    private long[] getBucketCounts(final int bucketIndex, final long[] bucketCounts) {
        // NOTE: this routine is not thread safe.

        // Reset any buckets that have not been increment recently
        final long currentTime = System.currentTimeMillis();
        final int currentIndex = (int) ((currentTime / bucketInterval) % bucketCount);
        if (bucketIndex != currentIndex) {
            resetBuckets(currentIndex, bucketIndex, bucketCounts);
        }

        final long[] tempotalBucketCounts = new long[bucketCounts.length];

        for (int i = 0; i < bucketCount; i++) {
            final int index = (((bucketIndex - i) % bucketCount) + bucketCount) % bucketCount;
            tempotalBucketCounts[i] = bucketCounts[index];
        }

        return tempotalBucketCounts;
    }

    /**
     * @return  return a temporal ascending order of send buckets counts (starting from the current time bucket)
     */
    public long[] getSendBucketCounts() {
        if (sendBucketCounts == null) {
            return null;
        }

        synchronized (sendBucketCounts) {
            return getBucketCounts(sendBucketIndex, sendBucketCounts);
        }
    }

    /**
     * @return  return a temporal ascending order of receive buckets counts (starting from the current time bucket)
     */
    public long[] getReceiveBucketCounts() {
        if (receiveBucketCounts == null) {
            return null;
        }

        synchronized (receiveBucketCounts) {
            return getBucketCounts(receiveBucketIndex, receiveBucketCounts);
        }
    }
}
