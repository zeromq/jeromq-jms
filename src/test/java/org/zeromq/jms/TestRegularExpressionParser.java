package org.zeromq.jms;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.logging.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

/**
 * General unit testing for regular expression.
 */
public class TestRegularExpressionParser {

    private static final Logger LOGGER = Logger.getLogger(TestRegularExpressionParser.class.getCanonicalName());

    /**
     * Test bench for expression parsing.
     */
    @Test
    public void testExpression() {
        // final String patternString = "=|!=|<=|>=|\\|\\||\\&\\&|\\d+|\\w+|[a-z()+\\-*/<>]";
        // final String patternString = "=|!=|<=|>=|\\|\\||\\&\\&|\\d+|\\w+|[()+\\-*/<>]";
        final String patternString = "=|!=|<=|>=|\\|\\||\\&\\&|\\d+|'(.*?)'|IS NULL|\\w+|[()+\\-*/<>]";
        final String conditionExpr = "=(xx_XX is null ++'this and that'+35>5*y)&&(z>=3 | k!=x)";

        final Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
        final Matcher matcher = pattern.matcher(conditionExpr);

        int pos = 0;
        while (matcher.find(pos)) {
            LOGGER.info("Found " + matcher.group() + " at position " + matcher.start() + ":" + matcher.end());
            pos = matcher.end();
        }
    }

    /**
     * Test bench for URI parsing.
     */
    @Test
    public void testUri() {
        // final String patternString = "^(jms){1}^:^(queue|topic){1}^:(\\*.)\\?^(\\*.\\=*.)";
        // final String patternString = "\\w+://(\\*|w+):\\d+|(\\w|\\.)+|:|\\?|=|\\&";
        final String patternString = "(tcp://(\\*|((\\w+|\\.)+)):\\d+)|(inproc://\\w+)|(\\w|\\.)+|:|\\?|=|\\&";
        // final String patternString = "\\w+://(\\*|(\\w+|\\.)):(\\d+|\\w+)|(\\w|\\.)+|:|\\?|=|\\&";
        // final String uri = "jms:queue:queue_2?gateway.addr=tcp://*:9586&redelivery.retry=0";
        final String uri = "jms:queue:queue_2?gateway.addr=tcp://zmq.org:9586&redelivery.retry=0";
        // final String uri = "jms:queue:queue_2";
        // final String uri = "jms:queue:latTestQueue?gateway.addr=inproc://lat_test&redelivery.retry=3";

        final Pattern pattern = Pattern.compile(patternString);
        final Matcher matcher = pattern.matcher(uri);

        int count = 0;
        int pos = 0;
        while (matcher.find(pos)) {
            LOGGER.info("Found " + matcher.group() + " at position " + matcher.start() + ":" + matcher.end());
            pos = matcher.end();

            if (++count > 20) {
                break;
            }
        }
    }

    /**
     * Test bench for string matching.
     */
    @Test
    public void testStringMatch() {

        final String pattern = "fred.*";
        final String value = "fred fields";

        final boolean equal = value.matches(pattern);

        Assert.assertTrue(equal);
    }
}
