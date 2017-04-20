package org.zeromq.jms;

/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test the URI extended functionality.
 */
public class TestZmqExtendedURI {

    /**
     * Test a simple inheritance of one abstract URI.
     */
    //@Test
    public void parseUriWithSimpleExtend() {
        final ZmqURI uri1 = ZmqURI.create("jms:queue:abstract_queue?gateway=fireAndForget&socket.addr=tcp://*:9586&redelivery.retry=3");
        final ZmqURI uri2 = ZmqURI.create("jms:queue:final_queue?extends=abstract_queue");

        final Map<String, ZmqURI> schema = new HashMap<String, ZmqURI>();
        schema.put(uri1.getDestinationName(),  uri1);
        schema.put(uri2.getDestinationName(),  uri2);

        final ZmqURI uri = new ZmqExtendedURI(uri2, schema);

        Assert.assertEquals("Scheme not found :" + uri, "jms", uri.getScheme());
        Assert.assertEquals("Destination name not found :" + uri, "queue", uri.getDestinationType());
        Assert.assertEquals("Destination type not found :" + uri, "final_queue", uri.getDestinationName());
        Assert.assertEquals("Missing gateway :" + uri, "fireAndForget", uri.getOptionValue("gateway"));
        Assert.assertEquals("Missing default value [redelivery]:" + uri, "retry", uri.getOptionValue("redelivery", "retry"));
        Assert.assertEquals("Missing value [socket.addr]:" + uri, "tcp://*:9586", uri.getOptionValues("socket.addr")[0]);
        Assert.assertEquals("Missing value [redelivery.retry]:" + uri, "3", uri.getOptionValues("redelivery.retry")[0]);
        Assert.assertEquals("Missing default value [redelivery.backout]:" + uri, "DLQ",
                uri.getOptionValues("redelivery.backout", new String[] { "DLQ" })[0]);
    }

    /**
     * Test a simple inheritance of one abstract URI.
     */
    @Test
    public void parseUriWithMultiExtends() {
        final ZmqURI uri1 = ZmqURI.create("jms:queue:abstract_queue?gateway=fireAndForget&socket.addr=tcp://*:9586&redelivery.retry=3");
        final ZmqURI uri2 = ZmqURI.create("jms:queue:queue1?extends=abstract_queue&redelivery.retry=0");
        final ZmqURI uri3 = ZmqURI.create("jms:queue:queue2?extends=abstract_queue&gateway=par&redelivery.retry=2");
        final ZmqURI uri4 = ZmqURI.create("jms:queue:final_queue?extends=queue1,queue2&socket.addr=tcp://*:9587");

        final Map<String, ZmqURI> schema = new HashMap<String, ZmqURI>();
        schema.put(uri1.getDestinationName(),  uri1);
        schema.put(uri2.getDestinationName(),  uri2);
        schema.put(uri3.getDestinationName(),  uri3);
        schema.put(uri4.getDestinationName(),  uri4);

        final ZmqURI uri = new ZmqExtendedURI(uri4, schema);

        Assert.assertEquals("Scheme not found :" + uri, "jms", uri.getScheme());
        Assert.assertEquals("Destination name not found :" + uri, "queue", uri.getDestinationType());
        Assert.assertEquals("Destination type not found :" + uri, "final_queue", uri.getDestinationName());
        Assert.assertEquals("Missing gateway :" + uri, "par", uri.getOptionValue("gateway"));
        Assert.assertEquals("Missing default value [redelivery]:" + uri, "retry", uri.getOptionValue("redelivery", "retry"));
        Assert.assertEquals("Missing value [socket.addr]:" + uri, "tcp://*:9587", uri.getOptionValues("socket.addr")[0]);
        Assert.assertEquals("Missing value [redelivery.retry]:" + uri, "0", uri.getOptionValues("redelivery.retry")[0]);
        Assert.assertEquals("Missing default value [redelivery.backout]:" + uri, "DLQ",
                uri.getOptionValues("redelivery.backout", new String[] { "DLQ" })[0]);
    }
}
