package org.zeromq.jms.protocol.store;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.annotation.ZmqComponent;
import org.zeromq.jms.annotation.ZmqUriParameter;
import org.zeromq.jms.util.ByteBufferBackedInputStream;

/**
 * This class implements a file based journal store.
 */
@ZmqComponent("file")
@ZmqUriParameter("journal")
public class ZmqFileJounralStore implements ZmqJournalStore {

    private static final Logger LOGGER = Logger.getLogger(ZmqFileJounralStore.class.getCanonicalName());

    public static final long JOUNRAL_SWEEP_PERIOD_MILLISECONDS = 3000;
    public static final long JOUNRAL_PURGE_PERIOD_MILLISECONDS = 10000;
    public static final long JOUNRAL_MESSAGE_REPUBLISH_MILLSECONDS = 6000;  // 2 sweeps before republish

    private static final String JOURNAL_FILE_DATE_PATTERN = "yyyyMMddHH";
    private static final String JOURNAL_ENTRY_DATE_PATTERN = "yyMMddHHmmssS";
    private static final char EOLN = "\n".charAt(0);
    private static final byte[] OFFSET_SEGMENT = new byte[4];
    private static final byte[] OFFSET_MESSAGE = new byte[4];
    private static final int PEEK_SIZE = 8;
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");

    private static final String JOURNAL_FILE_PREFIX = "journal_";
    private static final String JOURNAL_FILE_SUFFIX = ".jnl";
    private static final String JOURNAL_ARCHIVE_DIR = "archive";

    private DateFormat entryDateFormat;
    private DateFormat fileDateFormat;
    private Path location;
    private String groupId;
    private String uniqueId;

    private ScheduledExecutorService sweepScheduler = null;
    private long republishAfterMsec = JOUNRAL_MESSAGE_REPUBLISH_MILLSECONDS;
    private long archiveAfterMsec = JOUNRAL_PURGE_PERIOD_MILLISECONDS;

    private long sweepPeriod = JOUNRAL_SWEEP_PERIOD_MILLISECONDS;

    private final Map<String, Path> pathCache = new HashMap<String, Path>();
    private final Map<Object, MessageLocation> messageLocationMap = new ConcurrentHashMap<Object, MessageLocation>();

    private final TransferQueue<ZmqJournalEntry> messageQueue = new LinkedTransferQueue<ZmqJournalEntry>();

    /**
     * Return the data formatter used within the file store.
     * @param pattern   the pattern
     * @param timeZone  the time zone
     * @return          return the date formatter
     */
    private static DateFormat getDateFormat(final String pattern, final TimeZone timeZone) {
        final DateFormat format = new SimpleDateFormat(pattern);
        format.setTimeZone(timeZone);

        return format;
    }

    /**
     * Class to represent the location of a message on the file system.
     */
    private class MessageLocation {
        private final Object messageId;
        private final Path journalFile;
        private final long position;

        /**
         * Construct a message location pointer.
         * @param messageId    the message identifier
         * @param journalFile  the journal file
         * @param position      the position in the journal file
         */
        private MessageLocation(final Object messageId, final Path journalFile, final long position) {
            this.messageId = messageId;
            this.journalFile = journalFile;
            this.position = position;
        }

        /**
         * @return the path
         */
        public Object getMessageId() {
            return messageId;
        }

        /**
         * @return the path
         */
        public Path getJournalFile() {
            return journalFile;
        }

        /**
         * @return the position
         */
        public long getPosition() {
            return position;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;

            result = prime * result + ((messageId == null) ? 0 : messageId.hashCode());

            return result;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null) {
                return false;
            } else if (getClass() != obj.getClass()) {
                return false;
            }

            final MessageLocation other = (MessageLocation) obj;

            if (messageId == null) {
                if (other.messageId != null) {
                    return false;
                }
            } else if (!messageId.equals(other.messageId)) {
                return false;
            }

            return true;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "MessageLocation [messageId=" + messageId + ", journalFile=" + journalFile + ", position=" + position
                    + "]";
        }
    }

    /**
     * Construct file journal with default values, that need to be modified by setter
     * parameters. Used for the URI queue definition.
     */
    public ZmqFileJounralStore() {
        this.entryDateFormat = getDateFormat(JOURNAL_ENTRY_DATE_PATTERN, GMT);
        this.fileDateFormat = getDateFormat(JOURNAL_FILE_DATE_PATTERN, GMT);
    }

    /**
     * Construct file journal with the two important parameters.
     * @param location                the location of the file store
     * @param uniqueId                the unique identifier of the journal files
     * @param groupId                 the grouping identifier to group same journals together
     */
    public ZmqFileJounralStore(final Path location, final String groupId, final String uniqueId) {
        this.location = location;
        this.groupId  = groupId;
        this.uniqueId = uniqueId;
        this.entryDateFormat = getDateFormat(JOURNAL_ENTRY_DATE_PATTERN, GMT);
        this.fileDateFormat = getDateFormat(JOURNAL_FILE_DATE_PATTERN, GMT);
    }

    /**
     * Construct file journal with the two important parameters.
     * @param location                the location of the file store
     * @param uniqueId                the unique identifier of the journal files
     * @param groupId                 the grouping identifier to group same journals together
     * @param journalFileDatePattern  the date/time pattern used within the journal filename
     * @param timeZoneID              the time zone of the journal
     */
    public ZmqFileJounralStore(final Path location, final String groupId, final String uniqueId,
        final String journalFileDatePattern, final String timeZoneID) {

        this.location = location;
        this.groupId  = groupId;
        this.uniqueId = uniqueId;

        final TimeZone timeZone =  TimeZone.getTimeZone(timeZoneID);

        this.entryDateFormat = getDateFormat(JOURNAL_ENTRY_DATE_PATTERN, timeZone);
        this.fileDateFormat = getDateFormat(journalFileDatePattern, timeZone);
    }

    /**
     * Setter for the location of the journal files (i.e. c:\temp, /var/user/temp, etc..)
     * @param location  the location of the root directory
     */
    @ZmqUriParameter("journal.location")
    public void setLocation(final String location) {
        this.location = Paths.get(URI.create(location));
    }

    /**
     * Set the unique identifier.
     * @param uniqueId  the identifier
     */
    @ZmqUriParameter("journal.uniqueId")
    public void setUniqueId(final String uniqueId) {
        this.uniqueId = uniqueId;
    }

    /**
     * Set the unique file grouping identifier.
     * @param groupId  the group identifier
     */
    @ZmqUriParameter("journal.groupId")
    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * Setter for the location of the journal files based on a URI (i.e. file:///path/temp).
     * @param location  the location URI
     */
    @ZmqUriParameter("journal.locationURI")
    public void setLocationURI(final String location) {
        this.location = Paths.get(URI.create(location));
    }

    /**
     * Setter for the period between sweeping of the journal.
     * @param period  the period in milliseconds between scheduled sweeps
     */
    @ZmqUriParameter("journal.sweepPeriod")
    public void setSweepPeriod(final long period) {
        sweepPeriod = period;
    }

    /**
     * Setter for the message re-publishing time limit.
     * @param time  the time in milliseconds to lapse before republishing
     */
    @ZmqUriParameter("journal.republishAfter")
    public void setPublishAfter(final long time) {
        republishAfterMsec = time;
    }

    /**
     * Setter for the purging of OLD journal files in the archive in milliseconds. By setting
     * the time to -1 will stop the archive process.
     * @param time  the time in milliseconds to lapse before purging
     */
    @ZmqUriParameter("journal.archiveAfter")
    public void setArchiveAfter(final long time) {
        archiveAfterMsec = time;
    }

    /**
     * Setter for the entry date format pattern used in the journal for an entry. This
     * enables human readability of the file.
     * @param pattern  the java date pattern
     */
    @ZmqUriParameter("journal.entryDatePattern")
    public void setEntryDateFormat(final String pattern) {
        this.entryDateFormat = getDateFormat(pattern, GMT);
    }

    /**
     * Setter for the file date format pattern used in the journal file name.
     * @param pattern  the java date pattern
     */
    @ZmqUriParameter("journal.fileDatePattern")
    public void setFileDateFormat(final String pattern) {
        this.fileDateFormat = getDateFormat(pattern, GMT);
    }

    /**
     * Setter for the time zone identifier, otherwise local time zone will be sued within
     * the dates. Useful in cluster environments in different locals
     * @param timeZoneID  the java time zone ID, i.e. "GMT"
     */
    @ZmqUriParameter("journal.timeZoneID")
    public void setTimeZone(final String timeZoneID) {
        final TimeZone timeZone =  TimeZone.getTimeZone(timeZoneID);

        this.entryDateFormat.setTimeZone(timeZone);
        this.fileDateFormat.setTimeZone(timeZone);
    }

    /**
     * Return the current active journal filename for the specified unique identifier.
     * @param  uniqueId  the unique identifier
     * @return           return the current active journal file name
     */
    public String getCurrentJournalFileName(final String uniqueId) {
        final String journalDateLabel = fileDateFormat.format(new Date());
        final String fileName = JOURNAL_FILE_PREFIX + uniqueId + "_" + journalDateLabel + JOURNAL_FILE_SUFFIX;

        return fileName;
    }

    /**
     * Return the unique identifier of the file.
     * @param journalFile  the journal file
     * @return             return the unique identifier
     */
    public String getUniqueId(final Path journalFile) {
        final String filename = journalFile.getFileName().toString();
        final String journalDateLabel = fileDateFormat.format(new Date());
        final int startPos = JOURNAL_FILE_PREFIX.length();
        final int endPos = ("_" + journalDateLabel + JOURNAL_FILE_SUFFIX).length();

        return filename.substring(startPos, endPos);
    }

    /**
    * Return the current active journal file for the specified unique identifier.
     * @param  uniqueId  the unique identifier
      * @return           return the current path and cache it when it does not exist
     */
    public Path getCurrentJournalFle(final String uniqueId) {
        final String fileName = getCurrentJournalFileName(uniqueId);

        if (!pathCache.containsKey(fileName)) {
            final Path journalDir = location.resolve(groupId);
            final Path file = journalDir.resolve(fileName);

            pathCache.put(fileName, file);
        }

        final Path file = pathCache.get(fileName);

        return file;
    }

    /**
     * @return  create human readable unique id
     */
    protected String generateUniqueId() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();

        return name.replaceAll("\\W+", "-");
    }

    /**
     * @return  return the journal directory
     */
    public Path getJournalDir() {
        final Path journalDir = location.resolve(groupId);

        return journalDir;
    }
    /**
     * @return  return the path to the archive directory
     */
    public Path getAchiveJournalDir() {
        final Path archiveDir = location.resolve(groupId).resolve(JOURNAL_ARCHIVE_DIR);

        return archiveDir;
    }

    @Override
    public void open() throws ZmqException {
        if (location == null) {
            final String tempDir = System.getProperty("java.io.tmpdir");
            LOGGER.warning("No location path specified, defaulting to temp: " + tempDir);
            location = Paths.get(tempDir);
        }

        if (groupId == null) {
            throw new ZmqException("Missing groupId to open store, i.e. 'queue_1-incoming'");
        }

        if (uniqueId == null) {
            uniqueId = generateUniqueId();
            LOGGER.info("Defaulting to process identifier: " + uniqueId);
        }

        final Path journalDir = getJournalDir();

        try {
            Files.createDirectories(journalDir);
        } catch (IOException ex) {
            throw new ZmqException("Unable to open store (dir=" + journalDir + "): " + this, ex);
        }

        if (sweepPeriod > 0) {
            sweepScheduler = Executors.newScheduledThreadPool(1);

            if (sweepScheduler != null) {
                Runnable sweepCommand = new Runnable() {

                    @Override
                    public void run() {
                        try {
                            // sweep the files and archieve
                            sweepFiles(republishAfterMsec);

                            // purge all old archives
                            if (archiveAfterMsec >= 0) {
                                purgeArchives(archiveAfterMsec);
                            }
                        } catch (ZmqException ex) {
                            LOGGER.log(Level.SEVERE, "Sweep process failure: " + this, ex);
                        }
                    }
                };

                sweepScheduler.scheduleAtFixedRate(sweepCommand, sweepPeriod, sweepPeriod, TimeUnit.MILLISECONDS);
            }
        }

        LOGGER.info("Sucessfully openned: " + this);
    }

    @Override
    public void close() throws ZmqException {
        if (sweepScheduler != null) {
            try {
                sweepScheduler.shutdown();
                final boolean success = sweepScheduler.awaitTermination(3, TimeUnit.SECONDS);

                if (!success) {
                    LOGGER.severe("Sweep scheduler failed to stop: " + this);
                } else {
                    LOGGER.info("Sucessfully closed: " + this);
                }
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Sweep scheduler failed to stop: " + this, ex);
            }
        }
    }

    @Override
    public void reset() throws ZmqException {
        final Path journalDir = getJournalDir();

        if (Files.exists(journalDir)) {
            try {
                Files.walkFileTree(journalDir, new SimpleFileVisitor<Path>() {

                    @Override
                    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);

                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                        Files.delete(dir);

                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException ex) {
                throw new ZmqException("Unable to reset store (dir=" + journalDir + "): " + this, ex);
            }

            LOGGER.info("Sucessfully reset: " + this);
        } else {
            LOGGER.info("Nothing to reset: " + this);
        }
    }

    @Override
    public boolean delete(final Object messageId) throws ZmqException {
        final MessageLocation location = messageLocationMap.get(messageId);

        if (location != null) {
            assert (messageId.equals(location.getMessageId()));

            try {
                final Path journalFile = location.getJournalFile();
                final long position = location.getPosition();
                final SeekableByteChannel channel =
                    Files.newByteChannel(journalFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

                channel.position(position);
                final ByteBuffer peekBuffer = ByteBuffer.allocateDirect(PEEK_SIZE);

                channel.read(peekBuffer);

                final long entryPosition = channel.position();

                peekBuffer.rewind();

                //final int segmentOffset = peekBuffer.getInt();
                final int messageOffset = peekBuffer.getInt();

                final ByteBuffer entryBuffer = ByteBuffer.allocateDirect(messageOffset - PEEK_SIZE);

                channel.read(entryBuffer);
                entryBuffer.rewind();

                try (ObjectInput inEntry = new ObjectInputStream(new ByteBufferBackedInputStream(entryBuffer))) {
                    final String entryDate = (String) inEntry.readObject();
                    final boolean deleteFlag = inEntry.readBoolean();
                    final Object actualMessageId = inEntry.readObject();

                    assert (messageId.equals(actualMessageId));

                    if (!deleteFlag) {
                        final ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
                        final ObjectOutput out = new ObjectOutputStream(byteArrayOutput);

                        out.writeObject(entryDate);
                        out.writeBoolean(true);
                        out.writeObject(messageId);

                        channel.position(entryPosition);
                        channel.write(ByteBuffer.wrap(byteArrayOutput.toByteArray()));

                        messageLocationMap.remove(messageId);

                        // set as deleted, so return
                        return true;
                    }
                } catch (ClassNotFoundException ex) {
                    LOGGER.log(Level.SEVERE, "Unable to read message (pos=" + position + ", file=" + journalFile + "): " + this, ex);
                }
            } catch (IOException ex) {
                throw new ZmqException("Cannot delete message (messageId=" + messageId + ", file=" + location.getJournalFile() + "): " + this, ex);
            }
        }

        LOGGER.warning("Unknown event marked for deletion with reference (messageId=" + messageId + "): " + this);
        
        return false;
    }

    @Override
    public void create(final Object messageId, final ZmqMessage message) throws ZmqException {
        final ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();

        int messageOffset = 0;

        try {
            byteArrayOutput.write(OFFSET_SEGMENT);
            byteArrayOutput.write(OFFSET_MESSAGE);

            final ObjectOutput out = new ObjectOutputStream(byteArrayOutput);
            final String entryDate = entryDateFormat.format(new Date());

            out.writeObject(entryDate);
            out.writeBoolean(false);
            out.writeObject(messageId);
            out.flush();

            messageOffset = byteArrayOutput.size();

            final ObjectOutput out2 = new ObjectOutputStream(byteArrayOutput);
            out2.writeObject(message);
            out2.flush();
        } catch (IOException ex) {
            throw new ZmqException("Cannot convert message to and array of bytes (message=" + message + "): " + this, ex);
        }

        byteArrayOutput.write(EOLN);

        final int segmentOffset = byteArrayOutput.size();
        final byte[] bytes = byteArrayOutput.toByteArray();

        assert (segmentOffset == bytes.length);

        //update the block size of the message
        System.arraycopy(ByteBuffer.allocate(4).putInt(segmentOffset).array(), 0, bytes, 0, 4);
        System.arraycopy(ByteBuffer.allocate(4).putInt(messageOffset).array(), 0, bytes, 4, 4);

        final Path currentJournalFile = getCurrentJournalFle(uniqueId);

        try {
            Files.write(currentJournalFile, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            final long size = Files.size(currentJournalFile);
            final long position = size - bytes.length;

            final MessageLocation location = new MessageLocation(messageId, currentJournalFile, position);

            messageLocationMap.put(messageId, location);

            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("Message stored and tracked: " + message);
            }
        } catch (IOException ex) {
            throw new ZmqException("Cannot create message (" + messageId + ") : " + this, ex);
        }
    }

    /**
     * Purge all files archived files after a specified period.
     * @param archiveAfterMsec  the time lapsed (milliseconds)
     * @throws ZmqException     throw I/O exception on deletion failure
     */
    public void purgeArchives(final long archiveAfterMsec) throws ZmqException {
        final Path archiveDir = getAchiveJournalDir();
        final Date currentTime = new Date();

        try {
            Files.createDirectories(archiveDir);
        } catch (IOException ex) {
            throw new ZmqException("Unable to purge archieve file(s): " + ex.getMessage(), ex);
        }

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(archiveDir, "*" + JOURNAL_FILE_SUFFIX)) {
             for (Path journalFile : directoryStream) {
                final FileTime journalFileLastModified = Files.getLastModifiedTime(journalFile, LinkOption.NOFOLLOW_LINKS);

                   if (journalFileLastModified.toMillis() < (currentTime.getTime() - republishAfterMsec)) {
                       Files.deleteIfExists(journalFile);
                   }
               }
        } catch (IOException ex) {
            throw new ZmqException("Unable to purge archieve file(s): " + ex.getMessage(), ex);
        }
    }

    /**
     * Run the house keeping sweep process to republish message that have not been deleted, and archve jounrals that
     * are no longer useful.
     * @param  republishAfterMsec  the milliseconds time when entry is considered expired
     * @throws ZmqException     throw I/O exception on sweep failure
     */
    public void sweepFiles(final long republishAfterMsec) throws ZmqException {
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.info("Start sweep (republishAfterMsec=" + republishAfterMsec + ") on store: " + this);
        }

        final Map<String, List<Path>> journalMap = new TreeMap<String, List<Path>>();
        final Map<String, FileTime> journalLastModifiedMap = new TreeMap<String, FileTime>();
        final FileTime currentTime = FileTime.fromMillis(System.currentTimeMillis());
        final Path journalDir = getJournalDir();

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(journalDir, "*" + JOURNAL_FILE_SUFFIX)) {
            for (Path journalFile : directoryStream) {
                final String jounalFileUniqueId = getUniqueId(journalFile);
                final FileTime journalFileLastModified = Files.getLastModifiedTime(journalFile, LinkOption.NOFOLLOW_LINKS);

                if (!journalMap.containsKey(jounalFileUniqueId)) {
                    journalMap.put(jounalFileUniqueId, new LinkedList<Path>());
                    journalLastModifiedMap.put(jounalFileUniqueId, journalFileLastModified);
                }

                final List<Path> journalFiles = journalMap.get(jounalFileUniqueId);
                journalFiles.add(journalFile);

                final FileTime prevLastModified = journalLastModifiedMap.get(jounalFileUniqueId);
                if (prevLastModified.compareTo(journalFileLastModified) < 0) {
                    journalLastModifiedMap.put(jounalFileUniqueId, journalFileLastModified);
                }

                if (uniqueId.equals(jounalFileUniqueId)) {
                    // Touch this stores journals to ensure other sweeps know you are still active.
                    Files.setLastModifiedTime(journalFile, currentTime);

                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.info("Touch my store jounral: " + journalFile);
                    }
                }
            }
        } catch (IOException ex) {
            throw new ZmqException("Unable to sweep for archieve file(s).", ex);
        }

        if (journalMap.size() == 0) {
            return;
        }

        final String[] journalFileUniqueIds = new String[journalMap.size()];
        int index = 0;
        int uniqueIdIndex = -1;

        for (String journalFileUniqueId : journalMap.keySet()) {
            journalFileUniqueIds[index] = journalFileUniqueId;

            if (uniqueId.equals(journalFileUniqueId)) {
                uniqueIdIndex = index;
            }
            index++;
        }

        assert (uniqueIdIndex >= 0);

        int candidateIndex = (uniqueIdIndex + 1) % journalFileUniqueIds.length;
        final FileTime candidateLastModified = journalLastModifiedMap.get(journalFileUniqueIds[candidateIndex]);

        if (candidateLastModified.toMillis() < (currentTime.toMillis() - republishAfterMsec)) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.info("Checkout orphaned store [orphanUniqueId=" + journalFileUniqueIds[candidateIndex] +"] : " + this);
            }
            
            // Sweep oldest candidate file
            final List<Path> journalFiles = journalMap.get(journalFileUniqueIds[candidateIndex]);
            sweepOldestJournalFile(journalFiles, republishAfterMsec);
        }

        final List<Path> defaultJournalFiles = journalMap.get(uniqueId);
        if (defaultJournalFiles.size() > 1) {
            sweepOldestJournalFile(defaultJournalFiles, republishAfterMsec);
        }

        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.info("Sweep finished on store: " + this);
        }
    }

    /**
     * Sweep the oldest journal file within the specified list.
     * @param journalFiles         the list of journal files
     * @param republishAfterMsec   the milliseconds time when entry is considered expired
     * @throws ZmqException        throws I/O based ZMQ exception when process cannot be completed
     */
    public void sweepOldestJournalFile(final List<Path> journalFiles, final long republishAfterMsec) throws ZmqException {
        Collections.sort(journalFiles);
        final Path candidateFile = journalFiles.get(0);

        sweepJournalFile(candidateFile, republishAfterMsec);
    }

    /**
     * Sweep the journal entries that are not deleted and greater than a specified period for re-publishing. When
     * no entries can be found the journal needs to be archived.
     * @param journalFile          the journal file to be processed
     * @param republishAfterMsec   the milliseconds time when entry is considered expired
     * @throws ZmqException        throws I/O based ZMQ exception when process cannot be completed
     */
    public void sweepJournalFile(final Path journalFile, final long republishAfterMsec) throws ZmqException {
        final int count = republishLostMessages(journalFile, republishAfterMsec);

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Republished [count=" + count + ", journalFile=" + journalFile + "] message: " + this);
        }

        // With ALL journal entries deleted the files can be archived.
        if (count == 0) {
            final Path currentJounralFile = getCurrentJournalFle(uniqueId);

            if (!journalFile.equals(currentJounralFile)) {
                final Path archiveDir = getAchiveJournalDir();
                final Path archiveFile = archiveDir.resolve(journalFile.getFileName());

                try {
                    Files.createDirectories(archiveDir);
                    Files.move(journalFile, archiveFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

                    pathCache.remove(journalFile.getFileName());
                } catch (IOException ex) {
                    throw new ZmqException("Cannot archieve journal (file=" + journalFile + "): " + this, ex);
                }
            }
        }
    }

    /**
     * Read a journal file looking for entries that have not been deleted, and are older than the specified
     * lapse time (milliseconds). The total count of "undeleted" entries. This includes entries that are still with-in
     * the lapse time (have not expired).
     * @param journalFile          the journal file to be processed
     * @param republishAfterMsec   the milliseconds time when entry is considered expired
     * @return                     return the number of records that are not deleted
     * @throws ZmqException        throws I/O based ZMQ exception when process cannot be completed
     */
    public int republishLostMessages(final Path journalFile, final long republishAfterMsec) throws ZmqException {
        if (republishAfterMsec < 0) {
            // No republishing required, so enable archiving
            return 0;
        }

        try (SeekableByteChannel channel = Files.newByteChannel(journalFile, StandardOpenOption.READ)) {
            final ByteBuffer peekBuffer = ByteBuffer.allocateDirect(PEEK_SIZE);

            final Date republishAfterDate = new Date(System.currentTimeMillis() + republishAfterMsec);

            long position = channel.position();
            int peekSize = channel.read(peekBuffer);
            int count = 0;

            final List<ZmqJournalEntry> lostMessages = new LinkedList<ZmqJournalEntry>();

            while (peekSize > 0) {
                peekBuffer.rewind();

                final int segmentOffset = peekBuffer.getInt();
                final int messageOffset = peekBuffer.getInt();

                final ByteBuffer entryBuffer = ByteBuffer.allocateDirect(messageOffset - PEEK_SIZE);
                final int entrySize = channel.read(entryBuffer);

                entryBuffer.rewind();

                try (ObjectInput inEntry = new ObjectInputStream(new ByteBufferBackedInputStream(entryBuffer))) {
                    final Date entryDate = entryDateFormat.parse((String) inEntry.readObject());
                    final boolean deleteFlag = inEntry.readBoolean();
                    final Object messageId = inEntry.readObject();

                    if (deleteFlag && messageLocationMap.containsKey(messageId)) {
                        // someone delete the entry, so remove from checking
                        messageLocationMap.remove(messageId);
                    }

                    if (!deleteFlag && republishAfterDate.after(entryDate)) {
                        // active record, so read the message
                        count++;
                        final ByteBuffer messageBuffer = ByteBuffer.allocateDirect(segmentOffset - messageOffset);
                        final int messageSize = channel.read(messageBuffer);

                        assert (segmentOffset == entrySize + messageSize + 8);

                        messageBuffer.position(0);

                        InputStream inputStream = new ByteBufferBackedInputStream(messageBuffer);

                        try (ObjectInput inMessage = new ObjectInputStream(inputStream)) {
                            final ZmqMessage message = (ZmqMessage) inMessage.readObject();

                            ZmqJournalEntry entry = new ZmqJournalEntry(messageId, entryDate, deleteFlag, message);
                            lostMessages.add(entry);

                            final MessageLocation location = new MessageLocation(messageId, journalFile, position);
                            messageLocationMap.put(messageId, location);
                            
                            if (LOGGER.isLoggable(Level.FINEST)) {
                                LOGGER.finest("Republished message[messageId=" + messageId + "]: " + this);
                            }
                            
                        }
                    }
                } catch (ClassNotFoundException ex) {
                    LOGGER.log(Level.SEVERE, "Unable to read message (pos=" + position + ", file=" + journalFile + "): " + this, ex);
                } catch (ParseException ex) {
                    LOGGER.log(Level.SEVERE, "Unable to parse date (pos=" + position + ", file=" + journalFile + "): " + this, ex);
                }

                // jump to the next record
                position = position + segmentOffset;
                channel.position(position);

                // reset the peek buffer to the start again
                peekBuffer.rewind();
                // read the next record
                peekSize = channel.read(peekBuffer);
            }

            messageQueue.addAll(lostMessages);
            return count;
        } catch (IOException ex) {
            throw new ZmqException("Store (" + this + ") unable to publish lost messages: " + journalFile, ex);
        }
    }

    @Override
    public ZmqJournalEntry read() throws ZmqException {
        return messageQueue.poll();
    }

    @Override
    public String toString() {
        return "ZmqFileMessageStore [location=" + location  + ", groupId=" + groupId + ", uniqueId=" + uniqueId + "]";
    }
}
