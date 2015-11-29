package org.zeromq.jms.selector;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.text.ParseException;

import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;

/**
 * ZmqSelectory factory class used to construct ZmqMessageSelector. This class can be
 * sub-classed to implement specialized message selectors.
 */
@ZmqComponent("sql")
@ZmqUriParameter("selector")
public class ZmqMessageSelectorFactory {

    /**
     * Parse the expression return and newly construct and immutable .
     * @param expression       the string expression to parse
     * @return                 return the selector ready to perform evaluations
     * @throws ParseException  throws parse exception
     */
    public ZmqMessageSelector parse(final String expression) throws ParseException {

        return ZmqSimpleMessageSelector.parse(expression);
    }
}
