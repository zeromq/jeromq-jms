package org.zeromq.jms.jmx;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Map;

/**
 * ZMQ MBean interface that exposes metrics attributes within the JConsole.
 */
public interface ZmqMetricsMBean {

    String RATE_PER_SECOND = "Rate (s)";
    String RATE_PER_MINUTE = "Rate (min)";
    String MAX_RATE_PER_SECOND = "Max Rate (s)";
    String COUNT_30_SECONDS = "Count (30s)";
    String COUNT_60_SECONDS = "Count (60s)";
    String COUNT_90_SECONDS = "Count (90s)";
    String COUNT_2_MINUTES = "Count (2min";
    String COUNT_5_MINUTES = "Count (5min)";
    String COUNT_10_MINUTES = "Count (10min)";
    String COUNT_30_MINUTES = "Count (30min)";
    String COUNT_1_HOUR = "Count (1h)";
    String COUNT_6_HOURS = "Count (6h)";
    String COUNT_12_HOURS = "Count (12h)";
    String COUNT_24_HOURS = "Count (24h)";

    /**
     * @return  return true when there is activity
     */
    boolean isActive();

    /**
     * @return  return the send metrics
     */
    Map<String, Double> getSendMetrics();

    /**
     * @return  return the receive metrics
     */
    Map<String, Double> getReceiveMetrics();
}
