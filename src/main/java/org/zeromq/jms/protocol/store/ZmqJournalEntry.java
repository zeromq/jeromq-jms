package org.zeromq.jms.protocol.store;
/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Date;

import org.zeromq.jms.ZmqMessage;

/**
 * Class defines the journal entry from the message store.
 */
public class ZmqJournalEntry {
	
	private final Object messageId;
	private final Date createDate;
	private final boolean deleteFlag;
	private final ZmqMessage message;

	/**
	 * Create journal entry.
	 * @param messageId   the unique message identifier
	 * @param createDate  the entry creation date
	 * @param deleteFlag  the entry has been deleted
	 * @param message     the message
	 */
	public ZmqJournalEntry(final Object messageId, final Date createDate, final boolean deleteFlag, final ZmqMessage message) {
		this.messageId = messageId;
		this.createDate = createDate;
		this.deleteFlag = deleteFlag;
		this.message = message;
	}

	/**
	 * @return  return the unique message identifier
	 */
	public Object getMessageId() {
		return messageId;
	}

	/**
	 * @return  return the creation date of the journal entry
	 */
	public Date getCreateDate() {
		return createDate;
	}

	/**
	 * @return  return the true when the entry has been flagged as deleted
	 * 
	 */
	public boolean isDeleted() {
		return deleteFlag;
	}

	/**
	 * @return  return the message
	 */
	public ZmqMessage getMessage() {
		return message;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ZmqJournalEntry other = (ZmqJournalEntry) obj;
		if (messageId == null) {
			if (other.messageId != null)
				return false;
		} else if (!messageId.equals(other.messageId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ZmqJournalEntry [messageId=" + messageId + ", createDate=" + createDate + ", deleteFlag=" + deleteFlag
				+ ", message=" + message + "]";
	}
}
