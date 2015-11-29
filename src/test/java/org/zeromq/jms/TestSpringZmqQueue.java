package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test Zero MQ Queues work with the spring frame work.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
@ContextConfiguration(locations = "/jms-zmq-context.xml")
public class TestSpringZmqQueue {

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private TestMessageListener jmsListener;

    /**
     * Send text to default destination.
     * @param text  the text to send
     */
    public void send(final String text) {

        this.jmsTemplate.send(new MessageCreator() {

            @Override
            public Message createMessage(final Session session) throws JMSException {
                Message message = session.createTextMessage(text);
                return message;
            }
        });
    }

    /**
     * Simplify the send by using convert and send.
     * @param text  the text to send.
     */
    public void sendText(final String text) {
        this.jmsTemplate.convertAndSend(text);
    }

    /**
     * Send text message to a specified destination.
     * @param  dest  the destination
     * @param  text  the test to send
     */
    public void send(final Destination dest, final String text) {

        this.jmsTemplate.send(dest, new MessageCreator() {

            @Override
            public Message createMessage(final Session session) throws JMSException {
                Message message = session.createTextMessage(text);
                return message;
            }
        });
    }

    /**
     * Send text messages and check they have been received by the message listener bean.
     * @throws InterruptedException  throws interrupt exception
     */
    @Test
    public void testSendMessage() throws InterruptedException {

        sendText("This is a ZMQ backed JMS text");
        sendText("This is a ZMQ backed JMS text");
        sendText("This is a ZMQ backed JMS text");

        Thread.sleep(1000);

        Assert.assertEquals(3, jmsListener.getCounter());
    }
}
