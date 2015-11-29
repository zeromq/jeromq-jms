package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * All Zero MQ MSG socket related events.
 */
public interface ZmqEvent {

    /**
     * @return  return the message unique identifier.
     */
    Object getMessageId();
}
