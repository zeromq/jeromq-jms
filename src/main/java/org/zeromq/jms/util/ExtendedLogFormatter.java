package org.zeromq.jms.util;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * SimpleFormatter does not have the thread, so need to create a new formatter to expose the thread.
 */
public class ExtendedLogFormatter extends Formatter {
    private static final MessageFormat MESSAGE_FORMAT = new MessageFormat("{0,date}, {0,time, HH:mm:ss.SSS} {1} {2}: {3} [T:{4}] {5}\n");

    @Override
    public String format(final LogRecord record) {

        Object[] arguments = new Object[6];
        arguments[0] = new Date(record.getMillis());
        arguments[1] = record.getSourceClassName();
        arguments[2] = record.getSourceMethodName();
        arguments[3] = record.getLevel();
        arguments[4] = Long.toString(Thread.currentThread().getId());
        arguments[5] = record.getMessage();

        return MESSAGE_FORMAT.format(arguments);
    }
}
