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
import java.util.List;
import java.util.Map;

import org.zeromq.jms.protocol.ZmqGateway;
import org.zeromq.jms.protocol.ZmqSocketMetrics;

/**
 *  ZMQ gateway implementation of the MBean interface.
 */
public class ZmqGatewayManager implements ZmqGatewayManagerMBean, Serializable {

    private static final long serialVersionUID = -3472494196728884010L;

    private final String name;
    private final ZmqGateway gateway;

    /**
     * Construct a gateway manager MBean.
     * @param name     the unique JMX name of the Mbean
     * @param gateway  the gateway to be managed
     */
    public ZmqGatewayManager(final String name, final ZmqGateway gateway) {
        this.name = name;
        this.gateway = gateway;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isActive() {
        return gateway.isActive();
    }

    @Override
    public Date getStartTime() {
        return gateway.getStartTime();
    }

    @Override
    public boolean isTransacted() {
        return gateway.isTransacted();
    }

    @Override
    public boolean isBound() {
        return gateway.isBound();
    }

    @Override
    public boolean isAcknowledged() {
        return gateway.isAcknowledged();
    }

    @Override
    public boolean isHeartbeat() {
        return gateway.isHeartbeat();
    }

    @Override
    public String getDirection() {
        return gateway.getDirection().toString();
    }

    /**
     * Calculate the metrics for the send or receive based on the bucket counts.
     * @param bucketInterval  the bucket interval, i.e. 10000 milliseconds
     * @param bucketCounts    the bucket counts
     * @return                return the calculate metrics
     */
    protected Map<String, Double> calculateMetrics(final int bucketInterval, final long[] bucketCounts) {
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();

        double countSum = 0;
        int intervalSum = 0;

        for (int i = 0; i < bucketCounts.length; i++) {

            countSum = countSum + bucketCounts[i];
            intervalSum = intervalSum + bucketInterval;

            final double rate = countSum / intervalSum;

            if (!metrics.containsKey(RATE_PER_SECOND) && intervalSum >= 1000) {
                metrics.put(RATE_PER_SECOND, rate);
                continue;
            }
            if (!metrics.containsKey(RATE_PER_SECOND) && intervalSum >= 60000) {
                metrics.put(RATE_PER_SECOND, rate);
                continue;
            }
            if (!metrics.containsKey(MAX_RATE_PER_SECOND)) {
                metrics.put(MAX_RATE_PER_SECOND, rate);
                continue;
            } else {
                double maxRate = (double) metrics.get(MAX_RATE_PER_SECOND);

                if (rate > maxRate) {
                    metrics.put(MAX_RATE_PER_SECOND, rate);
                    continue;
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
        List<ZmqSocketMetrics> metricsList = gateway.getMetrics();

        if (metricsList.size() > 0) {
            final int bucketInterval = metricsList.get(0).getBucketInternval();
            final long[] bucketCounts = metricsList.get(0).getSendBucketCounts();

            if (bucketCounts != null) {
                final long[] sumBucketCounts = new long[bucketCounts.length];

                for (ZmqSocketMetrics socketMetrics : metricsList) {
                    final long[] socketBucketCounts = socketMetrics.getSendBucketCounts();

                    for (int i = 0; i < sumBucketCounts.length; i++) {
                        sumBucketCounts[i] = sumBucketCounts[i] + socketBucketCounts[i];
                    }
                }

                return calculateMetrics(bucketInterval, sumBucketCounts);
            }
        }

        return new LinkedHashMap<String, Double>();
    }

    @Override
    public Map<String, Double> getReceiveMetrics() {
        List<ZmqSocketMetrics> metricsList = gateway.getMetrics();

        if (metricsList.size() > 0) {
            final int bucketInterval = metricsList.get(0).getBucketInternval();
            final long[] bucketCounts = metricsList.get(0).getReceiveBucketCounts();

            if (bucketCounts != null) {
                final long[] sumBucketCounts = new long[bucketCounts.length];

                for (ZmqSocketMetrics socketMetrics : metricsList) {
                    final long[] socketBucketCounts = socketMetrics.getReceiveBucketCounts();

                    for (int i = 0; i < sumBucketCounts.length; i++) {
                        sumBucketCounts[i] = sumBucketCounts[i] + socketBucketCounts[i];
                    }
                }

                return calculateMetrics(bucketInterval, sumBucketCounts);
            }
        }

        return new LinkedHashMap<String, Double>();
    }

    @Override
    public long getSendCount() {
        List<ZmqSocketMetrics> metricsList = gateway.getMetrics();

        long sendCount = 0;

        if (metricsList.size() > 0) {
            for (ZmqSocketMetrics socketMetrics : metricsList) {
                sendCount = sendCount + socketMetrics.getSendCount();
            }
        }

        return sendCount;
    }

    @Override
    public Date getLastSendTime() {
        List<ZmqSocketMetrics> metricsList = gateway.getMetrics();

        long lastSendTime = 0;

        if (metricsList.size() > 0) {
            for (ZmqSocketMetrics socketMetrics : metricsList) {
                if (lastSendTime < socketMetrics.getLastSendTime()) {
                    lastSendTime = socketMetrics.getLastSendTime();
                }
            }
        }

        if (lastSendTime == 0) {
            return null;
        }

        return new Date(lastSendTime);
    }

    @Override
    public long getReceiveCount() {
        List<ZmqSocketMetrics> metricsList = gateway.getMetrics();

        long receiveCount = 0;

        if (metricsList.size() > 0) {
            for (ZmqSocketMetrics socketMetrics : metricsList) {
                receiveCount = receiveCount + socketMetrics.getReceiveCount();
            }
        }

        return receiveCount;
    }

    @Override
    public Date getLastReceiveTime() {
        List<ZmqSocketMetrics> metricsList = gateway.getMetrics();

        long lastReceiveTime = 0;

        if (metricsList.size() > 0) {
            for (ZmqSocketMetrics socketMetrics : metricsList) {
                if (lastReceiveTime < socketMetrics.getLastReceiveTime()) {
                    lastReceiveTime = socketMetrics.getLastReceiveTime();
                }
            }
        }

        if (lastReceiveTime == 0) {
            return null;
        }

        return new Date(lastReceiveTime);
    }
}
