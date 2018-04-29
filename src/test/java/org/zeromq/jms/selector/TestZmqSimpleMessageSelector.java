package org.zeromq.jms.selector;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 *  Test the Zero MQ JMS selector functionality.
 */
public class TestZmqSimpleMessageSelector {

    /**
     * Test math functionality within the selector expression.
     */
    @Test
    public void testMathExpresion() {
        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("6 = 2.0 + 3.0 + 1");
            final Map<String, Object> variables = new HashMap<String, Object>();

            final boolean result = selector.evaluate(variables);

            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test variables functionality within the selector expression.
     */
    @Test
    public void testVariable() {
        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("3.0 = var1 + 2.0");
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("var1", 1.0);

            final boolean result = selector.evaluate(variables);
            //((ZmqSimpleMessageSelector) selector).dump();

            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test variables functionality within the selector expression.
     */
    @Test
    public void testVariableNumberTypes() {
        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("21 = var1 + var2 + var3 + var4 + var5 + var6");
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("var1", (byte) 1);
            variables.put("var2", (short) 2);
            variables.put("var3", 3);
            variables.put("var4", 4L);
            variables.put("var5", (float) 5.0);
            variables.put("var6", (float) 6.0);

            final boolean result = selector.evaluate(variables);
            //((ZmqSimpleMessageSelector) selector).dump();

            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
    
    /**
     * Test SQL IN functionality within the selector expression.
     */
    @Test
    public void testIn() {
        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("1 = 1 and var1 in ((0.5 + 0.5), 2.0, 3.0)");
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("var1", 1.0);

            final boolean result = selector.evaluate(variables);

            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test SQL LIKE functionality within the selector expression.
     */
    @Test
    public void testLike() {
        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("var1 like '%is a%'");
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("var1", "this is a test");

            final boolean result = selector.evaluate(variables);

            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }

        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("var1 like '%is a%'");
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("var2", "this is a test");

            final boolean result = selector.evaluate(variables);

            Assert.assertFalse(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }

        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("var1 like 't__t'");
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("var1", "test");

            final boolean result = selector.evaluate(variables);

            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test SQL BETWEEN functionality within the selector expression.
     */
    @Test
    public void testBetween() {
        try {
            final ZmqMessageSelector selector = ZmqSimpleMessageSelector.parse("var1 between 1.0 and 5.0");
            final Map<String, Object> variables = new HashMap<String, Object>();

            variables.put("var1", 3.0);
            boolean result = selector.evaluate(variables);
            Assert.assertTrue(result);

            variables.put("var1", 1.0);
            result = selector.evaluate(variables);
            Assert.assertTrue(result);

            variables.put("var1", -0.5);
            result = selector.evaluate(variables);
            Assert.assertFalse(result);

            variables.put("var1", 1);
            result = selector.evaluate(variables);
            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test SQL IS NULL functionality within the selector expression.
     */
    @Test
    public void testNull() {
        try {
            final ZmqMessageSelector selectorIsNull = ZmqSimpleMessageSelector.parse("var1 is null");
            final Map<String, Object> variables = new HashMap<String, Object>();

            variables.put("var1", null);
            boolean result = selectorIsNull.evaluate(variables);
            Assert.assertTrue(result);

            final ZmqMessageSelector selectorIsNotNull = ZmqSimpleMessageSelector.parse("var1 is not null");

            result = selectorIsNotNull.evaluate(variables);
            Assert.assertFalse(result);

            variables.put("var1", "some string");

            result = selectorIsNull.evaluate(variables);
            Assert.assertFalse(result);

            result = selectorIsNotNull.evaluate(variables);
            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test SQL string functionality within the selector expression.
     */
    @Test
    public void testStringFunctions() {
        try {
            final Map<String, Object> variables = new HashMap<String, Object>();
            final ZmqMessageSelector selectorCase = ZmqSimpleMessageSelector
                    .parse("('lowercase' = lcase('lowerCASE')) AND ('UPPERCASE' = ucase('UPpercase'))");

            boolean result = selectorCase.evaluate(variables);
            Assert.assertTrue(result);

            final ZmqMessageSelector selectorMid = ZmqSimpleMessageSelector
                    .parse("('and' = mid('This and that', 6, 3)) AND ('and that' = mid('This and that', 6))");

            result = selectorMid.evaluate(variables);
            Assert.assertTrue(result);

            final ZmqMessageSelector selectorLen = ZmqSimpleMessageSelector.parse("len('This and that') = 13");

            result = selectorLen.evaluate(variables);
            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test double quotes within the selector expression.
     */
    @Test
    public void testDoubleQuotes() {
        try {
            final Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("topic", "org.fedoraproject.dev.logger.TEST");
            final ZmqMessageSelector selectorCase = ZmqSimpleMessageSelector
                    .parse("topic = \"org.fedoraproject.dev.logger.TEST\"");

            boolean result = selectorCase.evaluate(variables);
            Assert.assertTrue(result);

        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test SQL format functionality within the selector expression.
     */
    @Test
    public void testFormatFunctions() {
        try {
            final Map<String, Object> variables = new HashMap<String, Object>();
            final ZmqMessageSelector selectorNumberFormat = ZmqSimpleMessageSelector.parse("format(123.4, '##0.00') = '123.40'");

            boolean result = selectorNumberFormat.evaluate(variables);
            Assert.assertTrue(result);

            final Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, 1965);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DATE, 21);

            Date date = calendar.getTime();

            final ZmqMessageSelector selectorDateFormat = ZmqSimpleMessageSelector.parse("format(var1, 'YYYY-MM-DD') = '1965-01-21'");
            variables.put("var1", date);

            result = selectorDateFormat.evaluate(variables);
            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test SQL math functionality within the selector expression.
     */
    @Test
    public void testMathFunctions() {
        try {
            final Map<String, Object> variables = new HashMap<String, Object>();
            final ZmqMessageSelector selectorRound = ZmqSimpleMessageSelector.parse("round(19.5555) = 20");

            boolean result = selectorRound.evaluate(variables);
            Assert.assertTrue(result);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
