package org.zeromq.jms;
/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.junit.Assert;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import org.junit.Test;

/**
 * Test Zero MQ URI.
 */
public class TestZmqUri {
    /**
     * Test the parsing of the URI with old "gateway" attributes.
     */
    @Test
    public void parseMultiValueUri() {
        final ZmqURI uri = ZmqURI.create("jms:queue:queue?socket.addr=tcp://*:9586,tcp://*:9587");

        final String[] values = uri.getOptionValues("socket.addr");

        Assert.assertNotNull(values);
        Assert.assertEquals(2, values.length);
        Assert.assertEquals("tcp://*:9586", values[0]);
        Assert.assertEquals("tcp://*:9587", values[1]);
    }
        
    /**
     * Test the parsing of the URI with old "gateway" attributes.
     */
    @Test
    public void parseOldUri() {
        final ZmqURI uri = ZmqURI.create("jms:queue:queue_2?gateway=fireAndForget&gateway.addr=tcp://*:9586&redelivery.retry=3");

        Assert.assertEquals("Scheme not found :" + uri, "jms", uri.getScheme());
        Assert.assertEquals("Destination name not found :" + uri, "queue", uri.getDestinationType());
        Assert.assertEquals("Destination type not found :" + uri, "queue_2", uri.getDestinationName());
        Assert.assertEquals("Missing gateway :" + uri, "fireAndForget", uri.getOptionValue("gateway"));
        Assert.assertEquals("Missing default value [redelivery]:" + uri, "retry", uri.getOptionValue("redelivery", "retry"));
        Assert.assertEquals("Missing value [gateway.addr]:" + uri, "tcp://*:9586", uri.getOptionValues("gateway.addr")[0]);
        Assert.assertEquals("Missing value [redelivery.retry]:" + uri, "3", uri.getOptionValues("redelivery.retry")[0]);
        Assert.assertEquals("Missing default value [redelivery.backout]:" + uri, "DLQ",
                uri.getOptionValues("redelivery.backout", new String[] { "DLQ" })[0]);
    }

    /**
     * Test the parsing of the URI with old "socket" attributes, that replaces "gateway".
     */
    @Test
    public void parseNewUri() {
        // URI uri = URI.create("jms:zmp:/queue?addr=tcp://*:9586&destination=queue_2&retry=0");
        final ZmqURI uri = ZmqURI.create("jms:queue:queue_2?gateway=fireAndForget&socket.addr=tcp://*:9586&redelivery.retry=3");

        Assert.assertEquals("Scheme not found :" + uri, "jms", uri.getScheme());
        Assert.assertEquals("Destination name not found :" + uri, "queue", uri.getDestinationType());
        Assert.assertEquals("Destination type not found :" + uri, "queue_2", uri.getDestinationName());
        Assert.assertEquals("Missing gateway :" + uri, "fireAndForget", uri.getOptionValue("gateway"));
        Assert.assertEquals("Missing default value [redelivery]:" + uri, "retry", uri.getOptionValue("redelivery", "retry"));
        Assert.assertEquals("Missing value [socket.addr]:" + uri, "tcp://*:9586", uri.getOptionValues("socket.addr")[0]);
        Assert.assertEquals("Missing value [redelivery.retry]:" + uri, "3", uri.getOptionValues("redelivery.retry")[0]);
        Assert.assertEquals("Missing default value [redelivery.backout]:" + uri, "DLQ",
                uri.getOptionValues("redelivery.backout", new String[] { "DLQ" })[0]);
    }

    /**
     * Negative testing of the parsing of a URI.
     */
    @Test
    public void parseUri2() {

        final ZmqURI uri = ZmqURI.create("jms:queue:latTestQueue?gateway=par&socket.addr=inproc://lat_test&redelivery.retry=3");

        Assert.assertEquals("Scheme not found :" + uri, "jms", uri.getScheme());
        Assert.assertEquals("Missing gateway :" + uri, "par", uri.getOptionValue("gateway"));
        Assert.assertEquals("Missing value [socket.addr]:" + uri, "inproc://lat_test", uri.getOptionValues("socket.addr")[0]);
    }

    /**
     * Negative testing of the parsing of a URI.
     */
    @Test
    public void parseComplexUri() {
        final ZmqURI socketUri =
            ZmqURI.create(
                "jms:queue:socketTest?socket.addr=tcp://*:9999&socket.type=DEALER&socket.bind=false&redelivery=retry&redelivery.retry=0"
                   + "&socket.bindRetryWaitTime=1000&socket.recieveMsgFlag=10"
                   + "&socket.linger=10000&socket.reconnectIVL=10002&socket.backlog=10003&socket.reconnectIVLMax=10004"
                   + "&socket.maxMsgSize=10004&socket.sndHWM=10005&socket.rcvHWM=10006&socket.affinity=10007"
                   + "&socket.identity=identify"
                   + "&socket.rate=10010&socket.recoveryInterval=10011"
                   + "&socket.reqCorrelate=true&socket.reqRelaxed=true"
                   + "&socket.multicastHops=10010&socket.receiveTimeOut=10011&socket.sendTimeOut=10012"
                   + "&socket.tcpKeepAlive=10020&socket.tcpKeepAliveCount=10021&socket.tcpKeepAliveInterval=10022&socket.tcpKeepAliveIdle=10023"
                   + "&socket.sendBufferSize=10030&socket.receiveBufferSize=10031&socket.routerMandatory=true"
                   + "&socket.xpubVerbose=true&socket.ipv4Only=true&socket.delayAttachOnConnect=true");

        Assert.assertNotNull(socketUri);
    }
}
