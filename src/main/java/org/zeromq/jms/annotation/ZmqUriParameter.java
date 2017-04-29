package org.zeromq.jms.annotation;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  Method setter annotation to describe attribute name(s) within a URI.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.PARAMETER, ElementType.METHOD })
public @interface ZmqUriParameter {

    /**
     *  Return the attribute name attached against setter,  i.e. redelivery=retry, redelivery.retry=3, etc...
     */
    String value();
}
