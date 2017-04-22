package org.zeromq.jms.protocol.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.jms.JMSException;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.ZmqTextMessageBuilder;

/**
 * Test the simple message tore.
 */
public class TestZmqFileMessangeStore {

    private static final String MESSAGE_1 = "this is the text message 1";
    private static final String MESSAGE_2 = "this is the text message 1 2";
    private static final String MESSAGE_3 = "this is the text message 1 2 3";

    /**
     * Test storing of message in the journal file.
     * @throws IOException           throws I/O exception on test failure
     * @throws JMSException          throw JMS exception on test failure
     * @throws InterruptedException  throw interrupt on test failure
     */
    @Test()
    public void testStoreMessage() throws IOException, JMSException, InterruptedException {
        final String tempDir = System.getProperty("java.io.tmpdir");
        final Path location = Paths.get(tempDir).resolve("test-queue");
        final String groupId = "grouping";
        final String uniqueId = "zmq";

        final ZmqFileJounralStore store = new ZmqFileJounralStore(location, groupId, uniqueId, "yyyyMMdd", "GMT");

        store.reset();
        store.open();

        final ZmqMessage message1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
        final ZmqMessage message2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();
        final ZmqMessage message3 = ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage();

        store.create("messageId-1", message1);
        store.create("messageId-2", message2);
        store.create("messageId-3", message3);

        ZmqJournalEntry entry1 = store.read();
        Assert.assertNull(entry1);

        store.delete("messageId-2");

        final Path journalFile = store.getCurrentJournalFle(uniqueId);
        store.sweepJournalFile(journalFile, 1000);

        ZmqJournalEntry entry2 = store.read();
        Assert.assertNotNull(entry2);
        Assert.assertEquals("messageId-1", entry2.getMessageId());

        ZmqJournalEntry entry3 = store.read();
        Assert.assertNotNull(entry3);
        Assert.assertEquals("messageId-3", entry3.getMessageId());

        ZmqJournalEntry entry4 = store.read();
        Assert.assertNull(entry4);

        store.delete("messageId-1");
        store.delete("messageId-3");

        // switch to another unique ID to enable Archiving of the store.
        store.setUniqueId(uniqueId + "2");
        Thread.sleep(1000);

        store.sweepJournalFile(journalFile, 1000);
        Assert.assertFalse(Files.exists(journalFile));

        store.close();
    }

    /**
     * Test storing of message in the "MULTIPLE" journal file.
     * @throws IOException           throws I/O exception on test failure
     * @throws JMSException          throw JMS exception on test failure
     * @throws InterruptedException  throw interrupt on test failure
     */
    @Test()
    public void testMultiStoreMessage() throws IOException, JMSException, InterruptedException {
        final String tempDir = System.getProperty("java.io.tmpdir");
        final Path location = Paths.get(tempDir).resolve("test-queue");

        final String groupId = "grouping";
        final String uniqueId1 = "zmq-1";
        final String uniqueId2 = "zmq-2";
        final ZmqFileJounralStore store1 = new ZmqFileJounralStore(location, groupId, uniqueId1, "yyyyMMdd", "GMT");
        final ZmqFileJounralStore store2 = new ZmqFileJounralStore(location, groupId, uniqueId2, "yyyyMMdd", "GMT");

        // disable auto-sweep fir this test
        store1.setSweepPeriod(-1);
        store2.setSweepPeriod(-1);

        store1.reset();
        store2.reset();

        store1.open();
        store2.open();

        final ZmqMessage message1 = ZmqTextMessageBuilder.create().appendText(MESSAGE_1).toMessage();
        final ZmqMessage message2 = ZmqTextMessageBuilder.create().appendText(MESSAGE_2).toMessage();
        final ZmqMessage message3 = ZmqTextMessageBuilder.create().appendText(MESSAGE_3).toMessage();

        store1.create("messageId-1-1", message1);
        store2.create("messageId-2-2", message2);
        store2.create("messageId-3-2", message3);

        store1.sweepFiles(2000);
        store2.sweepFiles(2000);

        ZmqJournalEntry entry1 = store1.read();
        Assert.assertNull(entry1);

        ZmqJournalEntry entry2 = store2.read();
        Assert.assertNull(entry2);

        store1.sweepFiles(2000);
        store2.sweepFiles(2000);

        entry1 = store1.read();
        Assert.assertNull(entry1);

        entry2 = store2.read();
        Assert.assertNull(entry2);

        Thread.sleep(3000);
        store1.sweepFiles(2000);
        entry1 = store1.read();
        Assert.assertNotNull(entry1);
        Assert.assertEquals(message2, entry1.getMessage());

        entry1 = store1.read();
        Assert.assertNotNull(entry1);
        Assert.assertEquals(message3, entry1.getMessage());

        entry2 = store2.read();
        Assert.assertNull(entry2);

        store1.close();
        store2.close();
   }
}
