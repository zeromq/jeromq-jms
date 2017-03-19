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
import org.zeromq.jms.ZmqTextMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;
import org.zeromq.jms.protocol.ZmqGateway.Direction;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.event.ZmqStompEventHandler;

/**
 * Test PAR gateway functionality send/receive protocol functionality.
 */
public class TestZmqParGateway {

    private static final String SOCKET_ADDR = "tcp://*:9733";
    // private static final String SOCKET_ADDR = "inproc://queue1";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    /**
     * Test a send and receive protocol functionality without transactions enabled.
     */
    @Test
    public void testSendAndReceiveMessageWithoutTransaction() {

        final ZMQ.Context context = ZMQ.context(1);
        final int flags = 0;
        final ZmqEventHandler handler = new ZmqStompEventHandler();

        final ZmqGateway sender =
            new ZmqParGateway("protocol:sender", context, ZmqSocketType.DEALER, true, SOCKET_ADDR, flags, null, handler,
                null, null, null, null, false, Direction.OUTGOING);

        // final ZMQ.Context context2 = ZMQ.context(1);

        final ZmqGateway receiver =
        new ZmqParGateway("protocol:receiver", context, ZmqSocketType.ROUTER, false, SOCKET_ADDR, flags, null, handler,
                null, null, null, null, false, Direction.INCOMING);

        try {
            final ZmqTextMessage outMessage1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
            final ZmqTextMessage outMessage2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();
            final ZmqTextMessage outMessage3 = ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage();

            sender.open();
            receiver.open();

            try {
                sender.send(outMessage1);
                final ZmqTextMessage inMessage1 = (ZmqTextMessage) receiver.receive(3000);

                Assert.assertNotNull(inMessage1);
                Assert.assertEquals(MESSAGE_1, inMessage1.getText());

                sender.send(outMessage2);
                sender.send(outMessage3);

                final ZmqTextMessage inMessage2 = (ZmqTextMessage) receiver.receive(3000);
                final ZmqTextMessage inMessage3 = (ZmqTextMessage) receiver.receive(3000);

                Assert.assertNotNull(inMessage2);
                Assert.assertEquals(MESSAGE_2, inMessage2.getText());

                Assert.assertNotNull(inMessage3);
                Assert.assertEquals(MESSAGE_3, inMessage3.getText());
            } catch (Exception ex) {
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
