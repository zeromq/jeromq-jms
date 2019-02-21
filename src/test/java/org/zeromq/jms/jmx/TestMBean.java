package org.zeromq.jms.jmx;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zeromq.jms.ZmqConnectionFactory;
import org.zeromq.jms.ZmqQueue;
import org.zeromq.jms.ZmqTextMessageBuilder;

/**
 * Test the JMX MBean functionality.
 */
public class TestMBean {
    private static final String QUEUE_NAME = "queue_1";
    private static final String QUEUE_ADDR = "tcp://*:9828";
    private static final String QUEUE_URI = "jms:queue:" + QUEUE_NAME + "?socket.addr=" + QUEUE_ADDR
            + "&redelivery.retry=0&event=ZmqStompEventHandler";

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 2";
    private static final String MESSAGE_3 = "this is the text message 3";

    private static InitialContext context;

    /**
     * Have ZERO MQ MBeans been instantiated correctly.
     * @throws NamingException               throw naming exception
     * @throws JMSException                  throws JMS exception
     * @throws MalformedObjectNameException  throws malformed object name exception
     * @throws IOException                   throws I/O exception
     * @throws InterruptedException          throws interrupt exception
     */
    @Test
    public void testMBeans() throws NamingException, JMSException, MalformedObjectNameException, IOException, InterruptedException {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        final Set<ObjectInstance> instances = ZmqMBeanUtils.getMbeans(mbs);

        if (instances.size() > 0) {
            for (ObjectInstance instance : instances) {
                System.err.println("Unknown object instance : " + instance);
            }
        }
        Assert.assertEquals(0, instances.size());

        final QueueConnectionFactory factory = (QueueConnectionFactory) context.lookup("java:/comp/env/jms/queueConnectionFactory_jmx");
        final QueueConnection connection = factory.createQueueConnection();
        final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = (Queue) context.lookup("java:/comp/env/jms/queueTest_jmx");

        final QueueSender sender = session.createSender(queue);
        final QueueReceiver receiver = session.createReceiver(queue);

        sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage());
        sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage());
        sender.send(ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage());

        final TextMessage message1 = (TextMessage) receiver.receive(1000);
        final TextMessage message2 = (TextMessage) receiver.receive(1000);
        final TextMessage message3 = (TextMessage) receiver.receive(1000);

        Assert.assertNotNull(message1.getText());
        Assert.assertNotNull(message2.getText());
        Assert.assertNotNull(message3.getText());

        final List<ZmqGatewayManagerMBean> managerMBeans = ZmqMBeanUtils.getGatewayManagerMBeans(mbs, "sender-.*");
        final ZmqGatewayManagerMBean managerMBean = managerMBeans.get(0);
        final List<ZmqSocketStatisticsMBean> statisticMBeans = ZmqMBeanUtils.getSocketStatisticsMBeans(mbs, managerMBean.getName());

        Assert.assertEquals(1, managerMBeans.size());
        Assert.assertEquals(1, statisticMBeans.size());

        final ZmqSocketStatisticsMBean statisticMBean = statisticMBeans.get(0);

        Assert.assertNotNull(statisticMBean.getLastSendTime());
        Assert.assertEquals(3, statisticMBean.getSendCount());

        Map<String, Double> metrics = statisticMBean.getSendMetrics();
        final Double sendCount30msec = metrics.get(ZmqSocketStatisticsMBean.COUNT_30_SECONDS);

        Assert.assertNotNull(sendCount30msec);

        session.close();
    }

    /**
     * Set-up JNDI context for JMS Zero MQ unit tests.
     * @throws NamingException  throws exception
     */
    @BeforeClass
    public static void setup() throws NamingException {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        context = new InitialContext();

        context.createSubcontext("java:");
        context.createSubcontext("java:/comp");
        context.createSubcontext("java:/comp/env");
        context.createSubcontext("java:/comp/env/jms");

        context.bind("java:/comp/env/jms/queueConnectionFactory_jmx", new ZmqConnectionFactory(new String[] { QUEUE_URI }));
        context.bind("java:/comp/env/jms/queueTest_jmx", new ZmqQueue(QUEUE_NAME));
    }

    /**
     * Tear-down JNDI context for JMS Zero MQ unit tests.
     * @throws NamingException  throws exception
     */
    @AfterClass
    public static void pulldown() throws NamingException {

        context = new InitialContext();

        context.destroySubcontext("java:");
        context.close();
    }
}
