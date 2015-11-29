package org.zeromq.jms.selector;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Map;

/**
 * Message selector interface to encapsulate evaluation implementation.
 */
public interface ZmqMessageSelector {

    /**
     * Evaluate the parsed expression starting from the root of the tree of terms. The function will return the
     * result generated from the expression. This can be True/False, a Number, a String, anything. throws the
runtime runtime exception ArithmeticException on failure.
     *
     * @param  variables            the map of variable values
     * @return                      return the result.
     */
    boolean evaluate(Map<String, Object> variables);
}
