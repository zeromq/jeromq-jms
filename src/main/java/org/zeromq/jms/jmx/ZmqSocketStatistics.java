package org.zeromq.jms.jmx;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.Serializable;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.zeromq.jms.protocol.ZmqSocketMetrics;

/**
 *  ZMQ socket statistics implementation of the MBean interface.
 */
public class ZmqSocketStatistics implements ZmqSocketStatisticsMBean, Serializable {

    private static final long serialVersionUID = -2774442255180410402L;

    private final String name;
    private final String addr;
    private final boolean isBound;
    private final String type;
    private final ZmqSocketMetrics socketMetrics;

    /**
     * Construct the statistic around the underlying protocol.
     * @param name           the socket name
     * @param addr           the socket address
     * @param type           the socket type
     * @param isBound        the socket bind indicator
     * @param socketMetrics  the ZMQ socket metrics
     */
    public ZmqSocketStatistics(final String name, final String addr, final String type, final boolean isBound, final ZmqSocketMetrics socketMetrics) {
        this.name = name;
        this.addr = addr;
        this.type = type;
        this.isBound = isBound;
        this.socketMetrics = socketMetrics;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isActive() {
        return socketMetrics.isActive();
    }

    @Override
    public String getAddr() {
        return addr;
    }

    @Override
    public boolean isBound() {
        return isBound;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public long getSendCount() {
        return socketMetrics.getSendCount();
    }

    @Override
    public Date getLastSendTime() {
        if (socketMetrics.getLastSendTime() == 0) {
            return null;
        }

        return new Date(socketMetrics.getLastSendTime());
    }

    @Override
    public long getReceiveCount() {
        return socketMetrics.getReceiveCount();
    }

    @Override
    public Date getLastReceiveTime() {
        if (socketMetrics.getLastReceiveTime() == 0) {
            return null;
        }

        return new Date(socketMetrics.getLastReceiveTime());
    }

    /**
     * Calculate the metrics for the send or receive based on the bucket counts.
     * @param bucketInterval  the bucket interval, i.e. 10000 milliseconds
     * @param bucketCounts    the bucket counts
     * @return                return the calculate metrics
     */
    protected Map<String, Double> calculateMetrics(final int bucketInterval, final long[] bucketCounts) {
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();

        // check that metrics have been calculated
        if (bucketCounts == null) {
            return metrics;
        }

        double countSum = 0;
        int intervalSum = 0;
        double countPerSecond = 0;
        int countPerSecondIndex = 0;

        for (int i = 0; i < bucketCounts.length; i++) {

            countSum = countSum + bucketCounts[i];
            intervalSum = intervalSum + bucketInterval;

            final double rate = countSum / intervalSum;

            // sample n+1 buckets to ensure the full 1 seconds.
            countPerSecond = countPerSecond + bucketCounts[i];
            final double ratePerSecond = countPerSecond * (1000 / ((i - countPerSecondIndex + 1.0) * bucketInterval));

            if ((i * bucketInterval) > 10000) {
                countPerSecond = countPerSecond - bucketCounts[countPerSecondIndex];
                countPerSecondIndex++;
            }

            if (!metrics.containsKey(RATE_PER_MINUTE) && intervalSum >= 60000) {
                metrics.put(RATE_PER_MINUTE, rate);
            } else if (!metrics.containsKey(RATE_PER_SECOND) && intervalSum > 10000) {
                metrics.put(RATE_PER_SECOND, ratePerSecond);
                metrics.put(MAX_RATE_PER_SECOND, ratePerSecond);
            } else if (metrics.containsKey(MAX_RATE_PER_SECOND)) {
                double maxRate = (double) metrics.get(MAX_RATE_PER_SECOND);

                if (ratePerSecond > maxRate) {
                    metrics.put(MAX_RATE_PER_SECOND, ratePerSecond);
                }
            }

            if (!metrics.containsKey(COUNT_30_SECONDS) && intervalSum >= 30000) {
                metrics.put(COUNT_30_SECONDS, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_60_SECONDS) && intervalSum >= 60000) {
                metrics.put(COUNT_60_SECONDS, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_90_SECONDS) && intervalSum >= 90000) {
                metrics.put(COUNT_90_SECONDS, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_2_MINUTES) && intervalSum >= 120000) {
                metrics.put(COUNT_2_MINUTES, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_5_MINUTES) && intervalSum >= 300000) {
                metrics.put(COUNT_5_MINUTES, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_10_MINUTES) && intervalSum >= 600000) {
                metrics.put(COUNT_10_MINUTES, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_30_MINUTES) && intervalSum >= 1800000) {
                metrics.put(COUNT_30_MINUTES, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_1_HOUR) && intervalSum >= 3600000) {
                metrics.put(COUNT_1_HOUR, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_6_HOURS) && intervalSum >= 21600000) {
                metrics.put(COUNT_6_HOURS, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_12_HOURS) && intervalSum >= 43200000) {
                metrics.put(COUNT_12_HOURS, countSum);
                continue;
            }
            if (!metrics.containsKey(COUNT_24_HOURS) && intervalSum >= 86400000) {
                metrics.put(COUNT_24_HOURS, countSum);
                continue;
            }
        }

        return metrics;
    }

    @Override
    public Map<String, Double> getSendMetrics() {
        final int bucketInterval = socketMetrics.getBucketInternval();
        final long[] bucketCounts = socketMetrics.getSendBucketCounts();

        return calculateMetrics(bucketInterval, bucketCounts);
    }

    @Override
    public Map<String, Double> getReceiveMetrics() {
        final int bucketInterval = socketMetrics.getBucketInternval();
        final long[] bucketCounts = socketMetrics.getReceiveBucketCounts();

        return calculateMetrics(bucketInterval, bucketCounts);
    }
}
