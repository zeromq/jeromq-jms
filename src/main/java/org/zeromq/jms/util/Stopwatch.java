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
     * Returns the lapsed CPU time in the specified units since the stop-watch was last started.
     * @param  unit   the unit of time, i.e. nanoseconds, milliseconds, seconds, minutes, etc...
     * @return        return the elapsed time
     */
    public long lapsedTime(final TimeUnit unit) {
        final long now = System.nanoTime();

        return unit.convert((now - start), TimeUnit.NANOSECONDS);
    }

    /**
     * Returns the lapsed CPU time (in milliseconds) since the stop-watch was last started.
     * @return         return the elapsed time in milliseconds
     */
    public long lapsedTime() {
        return lapsedTime(TimeUnit.MILLISECONDS);
    }

    /**
     * Sleep the current thread for a specified time.
     * @param  waitTime  the time to wait
     * @param  unit      the unit of time, i.e. nanoseconds, milliseconds, seconds, minutes, etc...
     * @return           return true when sleep was successful
     */
    public boolean sleep(final long waitTime, final TimeUnit unit) {
        final long waitNanoseconds = TimeUnit.NANOSECONDS.convert(waitTime, unit);
        final long waitMilliseconds = TimeUnit.MILLISECONDS.convert(waitTime, unit);
        final int waitNanoAjustementNano = (int) waitNanoseconds % 1000;

        try {
            Thread.sleep(waitMilliseconds, waitNanoAjustementNano);
        } catch (InterruptedException ex) {
            return false;
        }

        return true;
    }

    /**
     * Sleep current thread for the specified number of milliseconds.
     * @param millis   the time in milliseconds
     * @return          return true when sleep was successful
     */
    public boolean sleep(final long millis) {
        return sleep(millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Return true if the specified lapsed time is still in the future.
     * @param time       the time lapsed to surpass
     * @param  unit      the unit of time, i.e. nanoseconds, milliseconds, seconds, minutes, etc...
     * @return           return true when still in the future
     */
    public boolean before(final long time, final TimeUnit unit) {
        final long waitNanoseconds = TimeUnit.NANOSECONDS.convert(time, unit);
        final long lapsedTime = lapsedTime(TimeUnit.NANOSECONDS);

        return lapsedTime < waitNanoseconds;
    }

    /**
     * Return true if the specified lapsed milliseconds is still in the future.
     * @param millis     the milliseconds lapsed to surpass
     * @return           return true when still in the future
     */
    public boolean before(final long millis) {
        return before(millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "Stopwatch [elapsedTime=" + lapsedTime() + "]";
    }
}
