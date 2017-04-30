package org.zeromq.jms.spring;
/*
 * Copyright (c) 2016 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.ConnectionFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.zeromq.jms.TestZmqQueueWithMultiSenders;
import org.zeromq.jms.ZmqConnectionFactory;

/**
 * Test Zero MQ Queues work with the spring annotation only frame work.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class)
public class TestSpringAnnoationZmqQueue {

    private static final Logger LOGGER = Logger.getLogger(TestZmqQueueWithMultiSenders.class.getCanonicalName());

    private static final String QUEUE_ADDR = "tcp://*:9712";
    private static final String QUEUE_CLIENT_NAME = "send1";
    private static final String QUEUE_CLIENT_URI = "jms:queue:" + QUEUE_CLIENT_NAME
        + "?socket.addr=" + QUEUE_ADDR + "&redelivery.retry=0&event=stomp";
    private static final String QUEUE_SERVER_NAME = "recv1";
    private static final String QUEUE_SERVER_URI = "jms:queue:" + QUEUE_SERVER_NAME
        + "?socket.addr=" + QUEUE_ADDR + "&socket.bind=true&redelivery.retry=0&event=stomp";

    /**
     * Spring configuration.
     */
    @Configuration
    @EnableJms
    static class AppConfig {

        /**
         * @return return the ZMQ connection factory
         */
        private ConnectionFactory connectionFactory() {
            final ConnectionFactory connectionFactory = new ZmqConnectionFactory(new String[] { QUEUE_CLIENT_URI, QUEUE_SERVER_URI });
            final CachingConnectionFactory cacheConnectionFactory = new CachingConnectionFactory(connectionFactory);

            return cacheConnectionFactory;
        }

        /**
         * Enable JMS listener annotated endpoints that are created under the cover by a JmsListenerContainerFactory.
         * @return  return the JMS listener
         */
        @Bean
        public DefaultJmsListenerContainerFactory myJmsListenerContainerFactory() {
          final DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();

          factory.setConnectionFactory(connectionFactory());
          //factory.setDestinationResolver(destinationResolver());
          factory.setConcurrency("5");

          return factory;
        }

        /**
         * @return return the JMS template to send a message
         */
        @Bean
        public JmsTemplate jmsTemplate() {
            final JmsTemplate template = new JmsTemplate();

            template.setConnectionFactory(connectionFactory());
            template.setDefaultDestinationName(QUEUE_CLIENT_NAME);

            return template;
        }
        /**
         * @return  return the service
         */
        @Bean
        public MyService myService() {
            return new MyService();
        }
    }

    /**
     * Simple service driven off a JMS message listener.
     */
    static class MyService {
        private CountDownLatch countDownLatch = new CountDownLatch(3);

        /**
         * @return  return the count down latch
         */
        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        /**
         * JMS message contains a string body.
         * @param msg  the message
         */
        @JmsListener(containerFactory = "myJmsListenerContainerFactory", destination = "recv1")
        public void process(final String msg) {
            LOGGER.info("Recieve message: " + msg);
            countDownLatch.countDown();
        }
    }

    @Autowired
    private MyService myService;

    @Autowired
    private JmsTemplate jmsTemplate;

    /**
     * Set initialisation of ZMQ using spring.
     */
     @Test
    public void testAnnotationSpring() {
        Assert.assertNotNull(myService);
        Assert.assertNotNull(jmsTemplate);

        jmsTemplate.convertAndSend("Hello");
        jmsTemplate.convertAndSend("there");
        jmsTemplate.convertAndSend("bye");

        try {
            myService.countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "Countdown latach was iterrupted", ex);
        }

        Assert.assertEquals(0L, myService.getCountDownLatch().getCount());
     }
}
