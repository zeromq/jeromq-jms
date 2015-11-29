package org.zeromq.jms.util;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.concurrent.TimeUnit;

/**
 * Utility class to measure elapse time.
 */
public class Stopwatch {

    private final long start;

    /**
     * Construct stop-watch and start timing.
     */
    public Stopwatch() {
        start = System.nanoTime();
    }

    /**
     * Returns the elapsed CPU time in the specified units since the stop-watch was last started.
     * @param   unit   the unit of time, i.e. nanoseconds, milliseconds, seconds, minutes, etc...
     * @return         return the elapsed time
     */
    public long elapsedTime(final TimeUnit unit) {
        long now = System.nanoTime();

        return unit.convert((now - start), TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the elapsed CPU time (in milliseconds) since the stop-watch was last started.
     * @return         return the elapsed time in milliseconds
     */
    public long elapsedTime() {
        return elapsedTime(TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "Stopwatch [elapsedTime=" + elapsedTime() + "]";
    }
}
