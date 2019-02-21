package org.zeromq.jms.annotation;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.jms.protocol.ZmqParGateway;
import org.zeromq.jms.protocol.redelivery.ZmqRetryRedeliveryPolicy;
import org.zeromq.jms.util.ClassUtils;

/**
 * Test Zero MQ annotation functionality.
 */
@Resource(name = "foo", description = "bar")
public class TestAnnotation {

    /**
     * Test find all classed under a specific package.
     * @throws ClassNotFoundException  throws class not found exception
     * @throws IOException             throws I/O exception
     */
    @Test
    public void testFindClassesUnderPackage() throws ClassNotFoundException, IOException {
        final List<Class<?>> classes = ClassUtils.getClasses("org.zeromq.jms.protocol");

        Assert.assertEquals(52, classes.size());
    }

    /**
     * Test find class with a specific annotation under a specific package.
     * @throws ClassNotFoundException  throws class not found exception
     * @throws IOException             throws I/O exception
     */
    @Test
    public void testFindClassesWithAnnotiation() throws ClassNotFoundException, IOException {
        final List<Class<?>> classes = ClassUtils.getClasses("org.zeromq.jms.protocol", ZmqComponent.class);

        Assert.assertEquals(8, classes.size());
    }

    /**
     * Test find methods of a class with a specific annotation.
     * @throws ClassNotFoundException  throws class not found exception
     * @throws IOException             throws I/O exception
     */
    @Test
    public void testFindMethodsWithAnnotiation() throws ClassNotFoundException, IOException {
        final List<Method> methods = ClassUtils.getMethods(ZmqRetryRedeliveryPolicy.class, ZmqUriParameter.class);

        Assert.assertEquals(1, methods.size(), 1);

        final Method method = methods.iterator().next();

        ZmqUriParameter attribute = (ZmqUriParameter) method.getAnnotation(ZmqUriParameter.class);
        Assert.assertEquals(attribute.value(), "redelivery.retry");
    }

    /**
     * Test find methods of a class with a specific annotated value.
     * @throws ClassNotFoundException     throws class not found exception
     * @throws IOException                throws I/O exception
     * @throws IllegalAccessException     throws illegal access exception
     * @throws IllegalArgumentException   throws illegal argument exception
     * @throws InvocationTargetException  throws invocation target exception
     */
    @Test
    public void testFindClassbyQuery() throws ClassNotFoundException, IOException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {

        final List<Class<?>> possibleClasses = ClassUtils.getClasses("org.zeromq.jms.protocol");
        final Class<?> clazz = ClassUtils.getClass(possibleClasses, ZmqComponent.class, "value", "par");

        Assert.assertNotNull(clazz);
        Assert.assertEquals(ZmqParGateway.class, clazz);
    }

    /**
     * Test find a specific setter method of a class.
     * @throws ClassNotFoundException     throws class not found exception
     * @throws IOException                throws I/O exception
     * @throws IllegalAccessException     throws illegal access exception
     * @throws IllegalArgumentException   throws illegal argument exception
     * @throws InvocationTargetException  throws invocation target exception
     */
    @Test
    public void testFindSetterMethodByQuery() throws ClassNotFoundException, IOException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException {

        final Method setMethod = ClassUtils.getSetterMethod(ZmqRetryRedeliveryPolicy.class, ZmqUriParameter.class, "value", "redelivery.retry");

        Assert.assertNotNull(setMethod);
        Assert.assertEquals(setMethod.getName(), "setRetryCount");
    }
}
