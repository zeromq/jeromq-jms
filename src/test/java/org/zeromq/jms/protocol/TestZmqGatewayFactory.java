package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqQueue;
import org.zeromq.jms.ZmqURI;

/**
 * Test the gateway factory.
 */
public class TestZmqGatewayFactory {

    private String[] extensionPackageNames;
    private Map<String, ZmqURI> destinationSchema;
    private ZmqGatewayFactory factory;

    /**
     * Setup the Schema required for the factory tests.
     */
    @Before
    public void setup() {
        extensionPackageNames = null;
        destinationSchema = new HashMap<String, ZmqURI>();

        final ZmqURI uri =
            ZmqURI.create("jms:queue:queue?socket.addr=tcp://*:9596&socket.type=DEALER&socket.bind=false&redelivery=retry&redelivery.retry=0");
        final ZmqURI oldUri =
            ZmqURI.create("jms:queue:queue_old?gateway.addr=tcp://*:9596&gateway.bind=false&gateway.type=DEALER&redelivery=retry&redelivery.retry=0");
        final ZmqURI badUri = ZmqURI.create("jms:queue:bad_uri?gateway.addr=tcp://*:9597&redelivery=retry&redelivery.retry=0&event=wrong");
        final ZmqURI socketUri =
                ZmqURI.create(
                     "jms:queue:socketTest?socket.addr=tcp://*:9999&socket.type=DEALER&socket.bind=true&redelivery=retry&redelivery.retry=0"
                   + "&socket.bindRetryWaitTime=1000&socket.recieveMsgFlag=10"
                   + "&socket.linger=10000&socket.reconnectIVL=10001&socket.backlog=10002&socket.reconnectIVLMax=10003"
                   + "&socket.maxMsgSize=10004&socket.sndHWM=10005&socket.rcvHWM=10006&socket.affinity=10007"
                   + "&socket.identity=identify"
                   + "&socket.rate=10010&socket.recoveryInterval=10011"
                   + "&socket.reqCorrelate=true&socket.reqRelaxed=true"
                   + "&socket.multicastHops=10010&socket.receiveTimeOut=10011&socket.sendTimeOut=10012"
                   + "&socket.tcpKeepAlive=10020&socket.tcpKeepAliveCount=10021&socket.tcpKeepAliveInterval=10022&socket.tcpKeepAliveIdle=10023"
                   + "&socket.sendBufferSize=10030&socket.receiveBufferSize=10031&socket.routerMandatory=true"
                   + "&socket.xpubVerbose=true&socket.ipv4Only=true&socket.delayAttachOnConnect=true"
                   + "&proxy.proxyAddr=tcp://*:9998&proxy.proxyType=ROUTER");

        destinationSchema.put(uri.getDestinationName(), uri);
        destinationSchema.put(oldUri.getDestinationName(), oldUri);
        destinationSchema.put(badUri.getDestinationName(), badUri);
        destinationSchema.put(socketUri.getDestinationName(), socketUri);

        factory = new ZmqGatewayFactory(extensionPackageNames, destinationSchema);

    }

    /**
     * Test gateway construction from a URI.
     * @throws ZmqException  throws Zero MQ JMS exception
     */
    @Test
    public void testFactoryUrl() throws ZmqException {
        final ZmqURI uri = destinationSchema.get("queue_old");
        final ZmqQueue queue = new ZmqQueue(uri);
        final ZmqGateway protocol = factory.newConsumerGateway("test", queue, ZmqSocketType.PULL, true, null, false);

        Assert.assertNotNull(protocol);
    }

    /**
     * Test gateway construction failure from a BAD URI.
     * @throws ZmqException  throws Zero MQ JMS exception
     */
    @Test(expected = ZmqException.class)
    public void testFactoryBadUrl() throws ZmqException {
        final ZmqURI uri = destinationSchema.get("bad_uri");
        final ZmqQueue queue = new ZmqQueue(uri);

        factory.newConsumerGateway("test@", queue, ZmqSocketType.PULL, true, null, false);

        Assert.fail("No exceptino was thrown");
    }

    /**
     * Check if the URL parameters are coming through correctly.
     * @throws ZmqException  throws Zero MQ JMS exception
     */
    @Test
    public void testSetupOfContext() throws ZmqException {
        final ZmqURI uri = destinationSchema.get("socketTest");

        final ZmqQueue queue = new ZmqQueue(uri);

        final ZmqGateway protocol = factory.newConsumerGateway("test", queue, ZmqSocketType.PULL, true, null, false);
        final ZmqSocketContext socketContext = protocol.getSocketContext();

        Assert.assertEquals("tcp://*:9999", socketContext.getAddr());
        Assert.assertEquals(ZmqSocketType.DEALER, socketContext.getType());
        Assert.assertTrue(socketContext.isBindFlag());

        Assert.assertEquals(new Long(1000), socketContext.getBindRetryWaitTime());
        Assert.assertEquals(new Integer(10), socketContext.getRecieveMsgFlag());

        Assert.assertEquals(new Long(10000), socketContext.getLinger());
        Assert.assertEquals(new Long(10001), socketContext.getReconnectIVL());
        Assert.assertEquals(new Long(10002), socketContext.getBacklog());
        Assert.assertEquals(new Long(10003), socketContext.getReconnectIVLMax());

        Assert.assertEquals(new Long(10004), socketContext.getMaxMsgSize());
        Assert.assertEquals(new Long(10005), socketContext.getSndHWM());
        Assert.assertEquals(new Long(10006), socketContext.getRcvHWM());
        Assert.assertEquals(new Long(10007), socketContext.getAffinity());

        Assert.assertEquals("identify", new String(socketContext.getIdentity()));

        Assert.assertEquals(new Long(10010), socketContext.getRate());
        Assert.assertEquals(new Long(10011), socketContext.getRecoveryInterval());

        Assert.assertEquals(Boolean.TRUE, socketContext.getReqCorrelate());
        Assert.assertEquals(Boolean.TRUE, socketContext.getReqRelaxed());

        Assert.assertEquals(new Long(10010), socketContext.getMulticastHops());
        Assert.assertEquals(new Integer(10011), socketContext.getReceiveTimeOut());
        Assert.assertEquals(new Integer(10012), socketContext.getSendTimeOut());

        Assert.assertEquals(new Long(10020), socketContext.getTcpKeepAlive());
        Assert.assertEquals(new Long(10021), socketContext.getTcpKeepAliveCount());
        Assert.assertEquals(new Long(10022), socketContext.getTcpKeepAliveInterval());
        Assert.assertEquals(new Long(10023), socketContext.getTcpKeepAliveIdle());

        Assert.assertEquals(new Long(10030), socketContext.getSendBufferSize());
        Assert.assertEquals(new Long(10031), socketContext.getReceiveBufferSize());
        Assert.assertEquals(Boolean.TRUE, socketContext.getRouterMandatory());

        Assert.assertEquals(Boolean.TRUE, socketContext.getXpubVerbose());
        Assert.assertEquals(Boolean.TRUE, socketContext.getIpv4Only());
        Assert.assertEquals(Boolean.TRUE, socketContext.getDelayAttachOnConnect());

        Assert.assertEquals("tcp://*:9998", socketContext.getProxyAddr());
        Assert.assertEquals(ZmqSocketType.ROUTER, socketContext.getProxyType());
    }
}
