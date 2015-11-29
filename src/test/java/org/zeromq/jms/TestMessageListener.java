package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * Test JMS listener for the various unit test.
 */
public class TestMessageListener implements MessageListener {

    private static final Logger LOGGER = Logger.getLogger(TestMessageListener.class.getCanonicalName());

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void onMessage(final Message message) {
        counter.incrementAndGet();

        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text;
            try {
                text = textMessage.getText();
                LOGGER.info("Processed message: " + text);
            } catch (JMSException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * @return  return the number of messages received
     */
    public int getCounter() {
        return counter.get();
    }
}
