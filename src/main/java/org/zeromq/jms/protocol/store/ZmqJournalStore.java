package org.zeromq.jms.protocol.store;
/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;

/**
 * All Zero MQ MSG store.
 */
public interface ZmqJournalStore {

    /**
     * Create a new message journal entry within the store.
     * @param  messageId     the unique ZMQ message identifier
     * @param  message       the ZMQ message to store
     * @throws ZmqException  throws I/O based ZMQ exception
     */
    void create(Object messageId, ZmqMessage message) throws ZmqException;

    /**
     * Delete the specified journal entry from the store.
     * @param  messageId     the unique ZMQ message identifier
     * @throws ZmqException  throws I/O based ZMQ exception
     * @return               return true when found and deleted 
     */
    boolean delete(Object messageId) throws ZmqException;

    /**
     * Ready a journal entry from the store.
     * @return               the journal entry
     * @throws ZmqException  throws I/O based ZMQ exception
     */
    ZmqJournalEntry read() throws ZmqException;

    /**
     * Open the message store, create root directory location when missing.
     * @throws ZmqException  throws I/O based ZMQ exception when root directory is not valid
     */
    void open() throws ZmqException;

    /**
     * Close the file store.
     * @throws ZmqException  throws I/O based ZMQ exception
     */
    void close() throws ZmqException;

    /**
     * Reset the file store. Purging all journals, etc...
     * @throws ZmqException  throws I/O based ZMQ exception
     */
    void reset() throws ZmqException;
}
