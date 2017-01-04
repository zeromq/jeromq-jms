package org.zeromq.jms;

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

/**
 * Test the Zero MQ message classes.
 */
public class TestZmqMessage {

    /**
     * Test the Zero MQ JMS Message instance.
     * @throws JMSException  throws JMS exceptions
     */
    @Test
    public void testMessage() throws JMSException {
        final ZmqMessage message = new ZmqMessage();

        // public boolean propertyExists(final String name) throws JMSException {
        message.setBooleanProperty("boolean", true);
        message.setByteProperty("byte", Byte.decode("0x01"));
        message.setDoubleProperty("double", 10.0);
        message.setFloatProperty("float", 10.0F);
        message.setIntProperty("int", 1);
        message.setLongProperty("long", 1L);
        message.setStringProperty("String", "String");
        message.setShortProperty("short", Short.valueOf("1"));
        message.setObjectProperty("Object", new Integer(1));

        Assert.assertEquals(true, message.getBooleanProperty("boolean"));
        Assert.assertEquals(Byte.decode("0x01"), new Byte(message.getByteProperty("byte")));
        Assert.assertEquals(new Double(10.0), new Double(message.getDoubleProperty("double")));
        Assert.assertEquals(new Float(10.0), new Float(message.getFloatProperty("float")));
        Assert.assertEquals(1, message.getIntProperty("int"));
        Assert.assertEquals(1L, message.getLongProperty("long"));
        Assert.assertEquals("String", message.getStringProperty("String"));
        Assert.assertEquals(Short.valueOf("1"), new Short(message.getShortProperty("short")));
        Assert.assertEquals(new Integer(1), message.getObjectProperty("Object"));

        message.setJMSCorrelationID("correlationID");
        Assert.assertEquals("correlationID", message.getJMSCorrelationID());

        message.setJMSCorrelationIDAsBytes(new byte[] { Byte.decode("0x01"), Byte.decode("0x02") });
        message.setJMSDeliveryMode(1);
        message.setJMSDestination(new ZmqQueue("queue"));
        message.setJMSExpiration(100);
        message.setJMSMessageID("messageID");
        message.setJMSPriority(1);
        message.setJMSRedelivered(true);
        message.setJMSReplyTo(new ZmqQueue("replyQueue"));
        message.setJMSTimestamp(200);
        message.setJMSType("type");

        Assert.assertArrayEquals(new byte[] { Byte.decode("0x01"), Byte.decode("0x02") }, message.getJMSCorrelationIDAsBytes());
        Assert.assertEquals(1, message.getJMSDeliveryMode());
        Assert.assertEquals(new ZmqQueue("queue"), message.getJMSDestination());
        Assert.assertEquals(100, message.getJMSExpiration());
        Assert.assertEquals("messageID", message.getJMSMessageID());
        Assert.assertEquals(1, message.getJMSPriority());
        Assert.assertEquals(true, message.getJMSRedelivered());
        Assert.assertEquals(new ZmqQueue("replyQueue"), message.getJMSReplyTo());
        Assert.assertEquals(200, message.getJMSTimestamp());
        Assert.assertEquals("type", message.getJMSType());
    }

    /**
     * Test the Zero MQ JMS Text Message instance.
     * @throws JMSException  throws JMS exceptions
     */
    @Test
    public void testTextMessage() throws JMSException {
        final ZmqTextMessage message = new ZmqTextMessage();

        final String objectIn = "this is an object";

        message.setText(objectIn);

        final String objectOut = message.getText();

        Assert.assertEquals(objectIn, objectOut);
    }

    /**
     * Test the Zero MQ JMS Map Message instance.
     * @throws JMSException  throws JMS exceptions
     */
    @Test
    public void testMapMessage() throws JMSException {
        final ZmpMapMessage message = new ZmpMapMessage();

        message.setBoolean("boolean", true);
        message.setByte("byte", Byte.decode("0x01"));
        message.setBytes("bytes", new byte[] { Byte.decode("0x01") });
        message.setBytes("bytes[1,1]", new byte[] { Byte.decode("0x01"), Byte.decode("0x02") }, 1, 1);
        message.setChar("char", 'Z');
        message.setDouble("double", 10.0);
        message.setFloat("float", 10.0F);
        message.setInt("int", 1);
        message.setLong("long", 1L);
        message.setString("String", "String");
        message.setShort("short", Short.valueOf("1"));

        Assert.assertEquals(true, message.getBoolean("boolean"));
        Assert.assertEquals(Byte.decode("0x01"), new Byte(message.getByte("byte")));
        Assert.assertArrayEquals(new byte[] { Byte.decode("0x01") }, message.getBytes("bytes"));
        Assert.assertArrayEquals(new byte[] { Byte.decode("0x02") }, message.getBytes("bytes[1,1]"));
        Assert.assertEquals('Z', message.getChar("char"));
        Assert.assertEquals(new Double(10.0), new Double(message.getDouble("double")));
        Assert.assertEquals(new Float(10.0), new Float(message.getFloat("float")));
        Assert.assertEquals(1, message.getInt("int"));
        Assert.assertEquals(1L, message.getLong("long"));
        Assert.assertEquals("String", message.getString("String"));
        Assert.assertEquals(Short.valueOf("1"), new Short(message.getShort("short")));
    }

    /**
     * Test the Zero MQ JMS Object Message instance.
     * @throws JMSException  throws JMS exceptions
     */
    @Test
    public void testObjectMessage() throws JMSException {
        final ZmqObjectMessage message = new ZmqObjectMessage();

        final String objectIn = "this is an object";

        message.setObject(objectIn);

        final String objectOut = (String) message.getObject();

        Assert.assertEquals(objectIn, objectOut);
    }
}
