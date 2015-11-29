package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.jms.protocol.ZmqGateway;
import org.zeromq.jms.protocol.ZmqGatewayFactory;
import org.zeromq.jms.protocol.ZmqSocketType;

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

        final ZmqURI uri = ZmqURI.create("jms:queue:queue_1?gateway.addr=tcp://*:9586&redelivery=retry&redelivery.retry=0");
        final ZmqURI badUri = ZmqURI.create("jms:queue:bad_uri?gateway.addr=tcp://*:9586&redelivery=retry&redelivery.retry=0&event=wrong");

        destinationSchema.put(uri.getDestinationName(), uri);
        destinationSchema.put(badUri.getDestinationName(), badUri);

        factory = new ZmqGatewayFactory(extensionPackageNames, destinationSchema);

    }

    /**
     * Test gateway construction from a URI.
     * @throws ZmqException  throws Zero MQ JMS exception
     */
    @Test
    public void testFactoryUrl() throws ZmqException {
        final ZmqURI uri = destinationSchema.get("queue_1");
        final ZmqQueue queue = new ZmqQueue(uri);

        final ZMQ.Context context = ZMQ.context(1);
        final ZmqGateway protocol = factory.newConsumerGateway("test@", queue, context, ZmqSocketType.PULL, true, null, false);

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

        final ZMQ.Context context = ZMQ.context(1);

        factory.newConsumerGateway("test@", queue, context, ZmqSocketType.PULL, true, null, false);
    }
}
