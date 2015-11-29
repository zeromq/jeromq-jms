package org.zeromq.jms.protocol.redelivery;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;
import org.zeromq.jms.protocol.ZmqSendEvent;

/**
 * Single n-ties and back-out re-delivery strategy.
 */
@ZmqComponent("retry")
@ZmqUriParameter("redelivery")
public class ZmqRetryRedeliveryPolicy implements ZmqRedeliveryPolicy {

    private static final Logger LOGGER = Logger.getLogger(ZmqRetryRedeliveryPolicy.class.getCanonicalName());

    private final Queue<ZmqSendEvent> redeliverQueue = new LinkedList<ZmqSendEvent>();
    private final Map<ZmqSendEvent, Integer> redeliveryMap = new HashMap<ZmqSendEvent, Integer>();

    private int retryCount;

    /**
     * Construct retry re-delivery policy.
     * @param retryCount  the retry count before failure
     */
    public ZmqRetryRedeliveryPolicy(final int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * @return  return the retry count before failure, i.e. 3...
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Setter for the retry count before message has failed to deliver.
     * @param retryCount  the retry count
     */
    @ZmqUriParameter("redelivery.retry")
    public void setRetryCount(final int retryCount) {
        this.retryCount = retryCount;
    }

    @Override
    public void redeliver(final Collection<ZmqSendEvent> events) {
        final List<ZmqSendEvent> failedRedeliveryMessages = new ArrayList<ZmqSendEvent>();

        synchronized (redeliveryMap) {
            for (ZmqSendEvent event : events) {
                if (redeliveryMap.containsKey(event)) {
                    int messageRetryCount = redeliveryMap.get(event) - 1;

                    if (messageRetryCount > 0) {
                        redeliveryMap.put(event, messageRetryCount);
                        redeliverQueue.add(event);
                    } else {
                        redeliveryMap.remove(event);
                        failedRedeliveryMessages.add(event);
                    }
                } else {
                    redeliveryMap.put(event, retryCount);
                    redeliverQueue.add(event);
                }
            }
        }
    }

    @Override
    public void delivered(final Collection<ZmqSendEvent> events) {
        synchronized (redeliveryMap) {
            for (ZmqSendEvent event : events) {
                redeliveryMap.remove(event);
            }
        }
    }

    @Override
    public ZmqSendEvent getNextRedeliver() {
        synchronized (redeliveryMap) {
            if (redeliverQueue.isEmpty()) {
                return null;
            }

            return redeliverQueue.poll();
        }
    }

    @Override
    public void onBackout(final ZmqSendEvent event) {
        LOGGER.warning("Event backed-out: " + event);
    }

}
