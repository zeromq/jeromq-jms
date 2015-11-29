package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import junit.framework.Assert;

import org.junit.Test;

/**
 * Test Zero MQ URI.
 */
public class TestZmqUri {

    /**
     * Test the parsing of the URI.
     */
    @Test
    public void parseUri() {
        // URI uri = URI.create("jms:zmp:/queue?addr=tcp://*:9586&destination=queue_2&retry=0");
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
     * Negative testing of the parsing of a URI.
     */
    @Test
    public void parseUri2() {

        final ZmqURI uri = ZmqURI.create("jms:queue:latTestQueue?gateway=par&gateway.addr=inproc://lat_test&redelivery.retry=3");

        Assert.assertEquals("Scheme not found :" + uri, "jms", uri.getScheme());
        Assert.assertEquals("Missing gateway :" + uri, "par", uri.getOptionValue("gateway"));
        Assert.assertEquals("Missing value [gateway.addr]:" + uri, "inproc://lat_test", uri.getOptionValues("gateway.addr")[0]);
    }
}
