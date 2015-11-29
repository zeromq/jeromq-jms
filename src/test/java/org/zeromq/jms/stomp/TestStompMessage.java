package org.zeromq.jms.stomp;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.junit.Assert;
import org.junit.Test;

/**
 * Test STOMP message construction.
 */
public class TestStompMessage {

    /**
     * Test construction of a connect message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testConnectMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineConnectMessage("1.2", "tcp://*:5557");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }

    /**
     * Test construction of a connected message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testConnectedMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineConnectedMessage("1.2");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }

    /**
     * Test construction of a disconnected message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testDisconnectedMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineDisconnectMessage("receipt-1");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }

    /**
     * Test construction of a error message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testErrorMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineErrorMessage("receipt-1", "error heading", "error description", "id-1");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }

    /**
     * Test construction of a send message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testSendMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineSendMessage("message-1", "topic/name1", "hello there!");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }

    /**
     * Test construction of a subscribe message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testSubscribeMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineSubscribeMessage("message-1", "topic/name1", "ack???");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }

    /**
     * Test construction of a unsubscribe message.
     * @throws StompException  throws STOMP exception
     */
    @Test
    public void testUnsubscribeMessage() throws StompException {
        StompMessage stompMessage = StompMessage.defineUnsubscribeMessage("message-1");

        String rawMessage = stompMessage.encode();

        StompMessage stompMessage2 = StompMessage.decode(rawMessage);

        Assert.assertEquals(stompMessage, stompMessage2);
    }
}
