package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqTextMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;
import org.zeromq.jms.protocol.ZmqGateway.Direction;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.event.ZmqStompEventHandler;
import org.zeromq.jms.protocol.redelivery.ZmqRedeliveryPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRetryRedeliveryPolicy;

/**
 * Test Fire and Forget send/receive protocol retry functionality.
 */
public class TestZmqFireAndForgetGatewayWithRetry {

    private static final String SOCKET_ADDR = "tcp://*:9733";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";
    private static final String MESSAGE_4 = "this is the text message 4";

    /**
     * Test a send and receive protocol functionality transactions enabled.
     */
    @Test
    public void testSendAndReceiveMessageWithoutTransaction() {

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();
        final ZmqRedeliveryPolicy redelivery = new ZmqRetryRedeliveryPolicy(3);

        final ZmqGateway sender = new ZmqFireAndForgetGateway("protocol:sender", context, ZmqSocketType.PUSH, false, SOCKET_ADDR, flags, null,
                handler, null, null, null, null, false, Direction.OUTGOING);

        final ZmqGateway receiver = new ZmqFireAndForgetGateway("protocol:receiver", context, ZmqSocketType.PULL, true, SOCKET_ADDR, flags, null,
                handler, null, null, null, redelivery, true, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();
            final ZmqTextMessage outMessage3 = ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage();
            final ZmqTextMessage outMessage4 = ZmqTextMessageBuilder.create().appendText(MESSAGE_4).toMessage();

            sender.open();
            receiver.open();

            try {
                sender.send(outMessage1);
                sender.send(outMessage2);
                sender.send(outMessage3);

                final ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                receiver.commit();

                ZmqTextMessage inMessage2 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());
                receiver.rollback();

                inMessage2 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());

                final ZmqTextMessage inMessage3 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_3, inMessage3.getText());
                receiver.commit();

                ZmqTextMessage inMessage4 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertNull(inMessage4);

                sender.send(outMessage4);

                inMessage4 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_4, inMessage4.getText());
                receiver.rollback();
                inMessage4 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_4, inMessage4.getText());
                receiver.rollback();
                inMessage4 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_4, inMessage4.getText());
                receiver.rollback();
                inMessage4 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertEquals(MESSAGE_4, inMessage4.getText());
                receiver.rollback();

                final ZmqTextMessage inMessage5 = (ZmqTextMessage) receiver.receive(1000);
                Assert.assertNull(inMessage5);

            } catch (ZmqException ex) {
                ex.printStackTrace();

                Assert.fail(ex.getMessage());
            } finally {
                sender.close();
                receiver.close();

                context.close();
            }
        } catch (JMSException ex) {
            ex.printStackTrace();

            Assert.fail(ex.getMessage());
        }
    }
}
