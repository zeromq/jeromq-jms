package org.zeromq.jms.perf;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.management.MBeanServer;

import org.zeromq.jms.ZmqConnectionFactory;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;
import org.zeromq.jms.jmx.ZmqGatewayManagerMBean;
import org.zeromq.jms.jmx.ZmqMBeanUtils;
import org.zeromq.jms.jmx.ZmqSocketStatisticsMBean;

import zmq.ZMQ;

/**
 * Perform the ZMQ interprocess performance test.
 */
public class TestZmqInprocLat {

    /**
     * Private constructor to ensure utility class is always instantiated by the "main" method.
     */
    private TestZmqInprocLat() {
        // Throw an exception if this ever *is* called
        throw new AssertionError("Instantiating utility class.");
    }

    /**
     * Main procedure for starting the performance test.
     * @param argv        the JVM arguments of the test, i.e. 1 100000
     * @throws Exception  throws an exception of failure
     */
    public static void main(final String[] argv) throws Exception {
        if (argv.length < 2 || argv.length > 3) {
            printf("usage: inproc_lat <message-size> <roundtrip-count> {<message-delay>}\n");
            return;
        }

        final int messageSize = atoi(argv[0]);
        final int roundtripCount = atoi(argv[1]);

        int messageDelay = 0;

        if (argv.length > 2) {
            messageDelay = atoi(argv[2]);
        }
        // final String queueURI = "jms:queue:latTestQueue?gateway=par&gateway.addr=inproc://lat_test2&event=stomp";
        // final String queueOutURI =
        // "jms:queue:latTestOutQueue?gateway=par&gateway.addr=tcp://*:9945&redelivery.retry=3";
        // final String queueInURI =
        // "jms:queue:latTestInQueue?gateway=par&gateway.addr=tcp://*:9945&redelivery.retry=3";
        final String queueOutURI = "jms:queue:latTestOutQueue?gateway=par&gateway.type=DEALER&gateway.bind=true"
                + "&gateway.addr=inproc://lat_test2&redelivery.retry=3";
        final String queueInURI = "jms:queue:latTestInQueue?gateway=par&gateway.type=ROUTER&gateway.bind=false"
                + "&gateway.addr=inproc://lat_test2&redelivery.retry=3";
        final QueueConnectionFactory factory = new ZmqConnectionFactory(new String[] { queueOutURI, queueInURI });
        final QueueConnection connection = factory.createQueueConnection();
        final QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue inQueue = session.createQueue("latTestInQueue");
        final Queue outQueue = session.createQueue("latTestOutQueue");

        QueueSender sender = null;
        QueueReceiver receiver = null;

        try {
            sender = session.createSender(outQueue);
            receiver = session.createReceiver(inQueue);

            final CountDownLatch countDownLatch = new CountDownLatch(roundtripCount);

            receiver.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(final Message message) {

                    countDownLatch.countDown();
                }
            });

            try {
                // Msg smsg = ZMQ.zmq_msg_init_size ( message_size);

                printf("message size: %d [B]\n", (int) messageSize);
                printf("roundtrip count: %d\n", (int) roundtripCount);

                final String text = String.format("%1$-" + messageSize + "s", "*");
                final ZmqMessage message = ZmqTextMessageBuilder.create().appendText(text).toMessage();

                long watch = ZMQ.startStopwatch();

                for (int i = 0; i < roundtripCount; i++) {
                    if (messageDelay > 0) {
                        Thread.sleep(messageDelay);
                    }

                    sender.send(message);
                }

                final long elapsed = ZMQ.stopStopwatch(watch);
                final double latency = (double) elapsed / (roundtripCount * 2);

                try {
                    countDownLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    throw ex;
                }

                printf("count down latch = " + countDownLatch.getCount());
                // [us] is microseconds, millionth of a second
                printf("average latency: %.3f [us]\n", (double) latency);
            } catch (Exception ex) {
                throw ex;
            }

            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final List<ZmqGatewayManagerMBean> managerMBeans = ZmqMBeanUtils.getGatewayManagerMBeans(mbs);

            for (ZmqGatewayManagerMBean managerMBean : managerMBeans) {
                List<ZmqSocketStatisticsMBean> statisticMBeans = ZmqMBeanUtils.getSocketStatisticsMBeans(mbs, managerMBean.getName());

                for (ZmqSocketStatisticsMBean statisticMBean : statisticMBeans) {
                    System.out.println("name = " + statisticMBean.getName() + ", sendCount = " + statisticMBean.getSendCount() + ", lastSendTime = "
                            + statisticMBean.getLastSendTime() + ", receiveCount = " + statisticMBean.getReceiveCount() + "  lastRecieveTime = "
                            + statisticMBean.getLastReceiveTime());
                }
            }
        } finally {
            session.close();
        }
    }

    /**
     * Convert string integer to an integer value.
     * @param string  the integer as a string
     * @return        return the integer value
     */
    private static int atoi(final String string) {
        return Integer.parseInt(string);
    }

    /**
     * Print message.
     * @param string the message
     */
    private static void printf(final String string) {
        System.out.println(string);
    }

    /**
     * Print message with arguments.
     * @param string  the message
     * @param args    the argurments to be included in the message
     */
    private static void printf(final String string, final Object... args) {
        System.out.println(String.format(string, args));
    }

}
