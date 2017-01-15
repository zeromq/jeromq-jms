package org.zeromq.jms.protocol;

/*
 * Copyright (c) 2015 Jeremy Miller
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.jms.ZmqException;
import org.zeromq.jms.ZmqMessage;
import org.zeromq.jms.protocol.event.ZmqEventHandler;
import org.zeromq.jms.protocol.filter.ZmqFilterPolicy;
import org.zeromq.jms.protocol.redelivery.ZmqRedeliveryPolicy;
import org.zeromq.jms.protocol.store.ZmqJournalEntry;
import org.zeromq.jms.protocol.store.ZmqJournalStore;
import org.zeromq.jms.selector.ZmqMessageSelector;
import org.zeromq.jms.util.Stopwatch;

/**
 * Abstract class that implements the gateway functionality around 1 or more ZERO MQ sockets.
 */
public abstract class AbstractZmqGateway implements ZmqGateway {
    private static final Logger LOGGER = Logger.getLogger(AbstractZmqGateway.class.getCanonicalName());

    private static final int HEARTBEAT_RATE_MILLI_SECOND = 1000;
    private static final int AUTO_PAUSE_IDLE_MILLI_SECOND = 3000;

    private AtomicBoolean active = new AtomicBoolean(false);

    private final String name;

    private final ZmqSocketType type;
    private final boolean bound;
    private final String addr;
    private final int flags;
    private final ZMQ.Context context;

    private final List<ZmqSocketMetrics> metrics;
    private final List<ZmqSocketSession> sessions;
    private final boolean transacted;
    private final boolean acknowledge;
    private final boolean heartbeat;
    private final Direction direction;

    private final Date startDateTime;

    private final ZmqRedeliveryPolicy redelivery;

    private final ZmqFilterPolicy filterPolicy;
    private final ZmqEventHandler eventHandler;
    private final ZmqMessageSelector messageSelector;
    private final ZmqJournalStore journalStore;

    private ZmqGatewayListener listener = null;

    private ExecutorService socketExecutor = null;
    private ExecutorService listenerExecutor = null;

    private static final int SOCKET_WAIT_MILLI_SECOND = 500;

    private static final int SOCKET_METRIC_BUCKET_COUNT = 360;
    private static final int SOCKET_METRIC_BUCKET_INTERVAL_MILLI_SECOND = 10000;

    private static final int LISTENER_THREAD_POOL = 1;
    private static final int LISTENER_WAIT_MILLI_SECOND = 500;

    private final TransferQueue<ZmqSendEvent> incomingQueue = new LinkedTransferQueue<ZmqSendEvent>();
    private final Queue<ZmqSendEvent> incomingSnapshot = new LinkedList<ZmqSendEvent>();

    private final TransferQueue<ZmqSendEvent> outgoingQueue = new LinkedTransferQueue<ZmqSendEvent>();
    private final Queue<ZmqSendEvent> outgoingSnapshot = new LinkedList<ZmqSendEvent>();

    /**
     * Message tacking class.
     */
    private static final class TrackEvent {
        private final ZmqEvent event;
        private final long eventSent;

        /**
         * Construct the event tracker instance.
         * @param event      the event
         * @param eventSent  the event sent time (nano seconds)
         */
        TrackEvent(final ZmqEvent event, final long eventSent) {
            this.event = event;
            this.eventSent = eventSent;
        }

        @Override
        public String toString() {
            return "TrackEvent [eventSent=" + eventSent + ", event=" + event + "]";
        }
    }

    private final ConcurrentMap<Object, TrackEvent> trackEventMap = new ConcurrentHashMap<Object, TrackEvent>();

    /**
     * Inner class for publishing external JMS messages.
     */
    private class ListenerThread implements Runnable {

        @Override
        public void run() {
            while (active.get() && listener != null) {
                try {
                    final ZmqMessage message = receive(LISTENER_WAIT_MILLI_SECOND);

                    if (message != null) {
                        listener.onMessage(message);
                    }
                } catch (ZmqException ex) {
                    listener.onException(ex);
                }
            }
        }

    }

    /**
     * Construct abstract gateway.
     * @param name          the name of display the gateway
     * @param context       the Zero MQ context
     * @param type          the Zero MQ socket type, i.e. Push, Pull, Router, Dealer, etc...
     * @param isBound       the Zero MQ socket bind/connection indicator
     * @param addr          the Zero MQ socket address(es) is comma separated format
     * @param flags         the Zero MQ socket send flags
     * @param filter        the message filter policy
     * @param handler       the event handler functionality
     * @param listener      the listener instance
     * @param store         the (optional) message store
     * @param selector      the (optional) message selection policy
     * @param redelivery    the (optional) message re-delivery policy
     * @param transacted    the transaction indicator
     * @param acknowledge   the always acknowledge indicator
     * @param heartbeat     the send heart-beat indicator
     * @param direction     the direction, i.e. Incoming, Outgoing, etc..
     */
    public AbstractZmqGateway(final String name, final ZMQ.Context context, final ZmqSocketType type, final boolean isBound, final String addr,
            final int flags, final ZmqFilterPolicy filter, final ZmqEventHandler handler, final ZmqGatewayListener listener,
            final ZmqJournalStore store, final ZmqMessageSelector selector, final ZmqRedeliveryPolicy redelivery,
            final boolean transacted, final boolean acknowledge,
            final boolean heartbeat, final Direction direction) {

        this.name = name;
        this.context = context;
        this.type = type;
        this.bound = isBound;
        this.addr = addr;
        this.flags = flags;
        this.filterPolicy = filter;
        this.eventHandler = handler;
        this.listener = listener;
        this.journalStore = store;
        this.messageSelector = selector;
        this.redelivery = redelivery;
        this.transacted = transacted;
        this.acknowledge = acknowledge;
        this.heartbeat = heartbeat;
        this.direction = direction;
        this.startDateTime = new Date();

        this.metrics = new LinkedList<ZmqSocketMetrics>();
        this.sessions = new LinkedList<ZmqSocketSession>();
    }

    /**
     * @return  return a list of socket addresses
     */
    protected String[] getSocketAddrs() {
        String[] addrs = addr.split(",");
        return addrs;
    }

    @Override
    public void open() {
        active.set(true);

        if (journalStore != null) {
        	try {
				journalStore.open();
			} catch (ZmqException ex) {
                LOGGER.log(Level.SEVERE, "Unable to journal store: " + journalStore, ex);
                return;
			}
        }
 
        listenerExecutor = Executors.newFixedThreadPool(LISTENER_THREAD_POOL);

        if (listener != null) {
            ListenerThread listenerThread = new ListenerThread();
            listenerExecutor.execute(listenerThread);
        }

        String[] socketAddrs = getSocketAddrs();
        socketExecutor = Executors.newFixedThreadPool(socketAddrs.length);

        final boolean socketOutgoing = (direction == Direction.OUTGOING || heartbeat || acknowledge);
        final boolean socketIncoming = (direction == Direction.INCOMING || heartbeat || acknowledge);

        for (String socketAddr : socketAddrs) {
            final ZMQ.Socket socket = getSocket(context, type.getType());

            // Set the filters when they exist
            if (type == ZmqSocketType.SUB && filterPolicy != null) {
                String[] filters = filterPolicy.getConsumerFilters();
                if (filters != null) {
                    for (String filter : filters) {
                        byte[] filterAsBytes = filter.getBytes();
                        socket.subscribe(filterAsBytes);
                    }
                }
            }

            ZmqSocketMetrics socketMetrics = new ZmqSocketMetrics(socketAddr, SOCKET_METRIC_BUCKET_COUNT,
                    SOCKET_METRIC_BUCKET_INTERVAL_MILLI_SECOND, socketOutgoing, socketIncoming);
            metrics.add(socketMetrics);

            final ZmqSocketListener socketListener = getSocketListener(socketAddr, socketIncoming, socketOutgoing);

            ZmqSocketSession socketSession = new ZmqSocketSession(active, socket, type, socketAddr, bound, socketIncoming, socketOutgoing, flags,
                    SOCKET_WAIT_MILLI_SECOND, heartbeat, acknowledge, socketListener, filterPolicy, eventHandler, socketMetrics);

            sessions.add(socketSession);
            socketExecutor.execute(socketSession);
        }

        LOGGER.info("Gateway openned: " + toString());
    }

    /**
     * Construct a ZMQ socket and initialize default settings.
     * @param context       the Zero MQ context
     * @param socketType    the Zero MQ socket type, i.e. Push, Pull, Router, Dealer, etc...
     * @return              return the constructed and initialized ZMQ socket
     */
    protected ZMQ.Socket getSocket(final ZMQ.Context context, final int socketType) {
        final ZMQ.Socket socket = context.socket(socketType);

        socket.setSendTimeOut(0);

        return socket;
    }

    /**
     * Return a socket listener for socket session. This is the main "event" based routine that will be extended
     * to add additional functionality.
     * @param socketAddr      the ZMQ address
     * @param socketIncoming  the incoming messages indicator
     * @param socketOutgoing  the outgoing messages indicator
     * @return                return the instance of the listener
     */
    protected ZmqSocketListener getSocketListener(final String socketAddr, final boolean socketIncoming, final boolean socketOutgoing) {

        final ZmqSocketListener socketListener = new ZmqSocketListener() {

            @Override
            public void open(final ZmqSocketSession session) {
            }

            @Override
            public ZmqEvent send(final ZmqSocketSession source) {
                ZmqEvent sendEvent = null;

                if (journalStore != null) {
                	try {
                		final ZmqJournalEntry journalEntry = journalStore.read();
                		if (journalEntry != null && (!trackEventMap.containsKey(journalEntry.getMessageId()))) {
                			sendEvent =
                                eventHandler.createSendEvent(journalEntry.getMessageId(), journalEntry.getMessage());
                		}
                	} catch (ZmqException ex) {
                		LOGGER.log(Level.WARNING, "Failed to read from the journal store", ex);
                	}
                }

                if (sendEvent == null) {
                	try {
                		sendEvent = outgoingQueue.poll(SOCKET_WAIT_MILLI_SECOND, TimeUnit.MILLISECONDS);
                	} catch (InterruptedException ex) {
                		LOGGER.log(Level.WARNING, "Polling of outgoing queue interrupted", ex);
                	}
                }

                // No message(s) so send a heart-beat when required
                if (heartbeat && socketOutgoing && sendEvent == null) {
                    // check whether a heart-beat need to be sent since the last message sent
                    final long lastReceiveTime = source.getLastReceiveTime();
                    final long lastSendTime = source.getLastSendTime();

                    final long currentTime = System.nanoTime();
                    final long lastReceiveLaspedTime = (currentTime - lastReceiveTime) / 1000000;
                    final long lastSendLaspedTime = (currentTime - lastSendTime) / 1000000;
                    final ZmqSocketStatus status = source.getStatus();

                    if (lastReceiveLaspedTime > HEARTBEAT_RATE_MILLI_SECOND && lastSendLaspedTime > HEARTBEAT_RATE_MILLI_SECOND) {
                        if (lastReceiveLaspedTime > AUTO_PAUSE_IDLE_MILLI_SECOND && status == ZmqSocketStatus.RUNNING) {
                            // connection has been dropped, so stop sending messages apart from heart-beats
                            source.pause();
                        } else {
                            sendEvent = eventHandler.createHeartbeatEvent();
                        }
                    }
                }

                if (sendEvent instanceof ZmqHeartbeatEvent) {
                    if (acknowledge) {
                        final long messageSent = System.currentTimeMillis();
                        final Object messageId = sendEvent.getMessageId();
                        final TrackEvent tackEvent = new TrackEvent(sendEvent, messageSent);

                        if (LOGGER.isLoggable(Level.FINEST)) {
                            LOGGER.log(Level.FINEST, "Socket " + socketAddr + " for gateway " + name + " tacking event: " + sendEvent);
                        }

                        trackEventMap.put(messageId, tackEvent);
                    }
                }

                if (LOGGER.isLoggable(Level.FINEST) && sendEvent != null) {
                    LOGGER.log(Level.FINEST, "Socket " + socketAddr + " for gateway " + name + " send event: " + sendEvent);
                }

                return sendEvent;
            }

            @Override
            public void error(final ZmqSocketSession source, final ZmqEvent event) {
                if (event instanceof ZmqSendEvent) {
                    final ZmqSendEvent sendEvent = (ZmqSendEvent) event;

                    try {
                        outgoingQueue.put(sendEvent);

                        if (LOGGER.isLoggable(Level.FINEST) && sendEvent != null) {
                            LOGGER.log(Level.FINEST, "Socket " + socketAddr + " for gateway " + name + " send event: " + sendEvent);
                        }
                    } catch (InterruptedException ex) {
                        LOGGER.log(Level.SEVERE, "Unable to re-send event: " + event, ex);
                    }
                }
            }

            @Override
            public ZmqEvent receive(final ZmqSocketSession session, final ZmqEvent event) {
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.log(Level.FINEST, "Socket " + socketAddr + " for gateway " + name + " consume event: " + event);
                }

                if (event instanceof ZmqSendEvent) {
                    try {
                        if (journalStore != null) {
                        	journalStore.create(event.getMessageId(),  ((ZmqSendEvent) event).getMessage());
                        }

                        incomingQueue.put((ZmqSendEvent) event);
                    } catch (InterruptedException ex) {
                        LOGGER.log(Level.SEVERE, "Socket " + socketAddr + " for gateway " + name
                            + " cannot consume message due to intenral error: " + event, ex);

                        return null;
                    } catch (ZmqException ex) {
                        LOGGER.log(Level.SEVERE, "Socket " + socketAddr + " for gateway " + name
                            + " cannot store messahe due to intenral error: " + event, ex);

                        return null;
                    }
                }

                ZmqEvent replyEvent = null;

                if (event instanceof ZmqHeartbeatEvent) {
                    // Heart beat is ALL SENDS
                    try {
                        if (session.isAcknowledge() && session.isIncoming()) {
                            replyEvent = eventHandler.createAckEvent(event);
                        }
                    } catch (ZmqException ex) {
                        LOGGER.log(Level.SEVERE, "Socket " + socketAddr + " for gateway " + name + " received corrupt event: " + event, ex);
                    }
                } else if (event instanceof ZmqAckEvent) {
                    ZmqAckEvent ackEvent = (ZmqAckEvent) event;
                    final Object messageId = ackEvent.getMessageId();
                    if (messageId == null) {
                        LOGGER.log(Level.SEVERE, "Socket " + socketAddr + " for gateway " + name + " received corrupt event: " + event);
                    } else {
                        final TrackEvent trackedEvent = trackEventMap.remove(messageId);
                        if (trackedEvent == null) {
                            LOGGER.log(Level.WARNING, "Socket " + socketAddr + " for gateway " + name + " received ACK for untracked event: "
                                    + event);
                        }
                    }
                }

                if (replyEvent != null && LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.log(Level.FINEST, "Socket " + socketAddr + " for gateway " + name + " reply event: " + event);
                }

                return replyEvent;
            }

            @Override
            public void close(final ZmqSocketSession session) {
            }
        };

        return socketListener;
    }

    @Override
    public boolean isActive() {
        return active.get();
    }

    @Override
    public void close() {
        if (acknowledge) {
            // wait for a period before warning about failed ACKS
            if (trackEventMap.size() > 0) {
                LOGGER.info("Gateway " + name + " waiting for acknowledgement message(s): " + trackEventMap.size());

                try {
                    Thread.sleep(SOCKET_WAIT_MILLI_SECOND);
                } catch (InterruptedException ex) {
                    LOGGER.throwing(AbstractZmqGateway.class.getCanonicalName(), "close()", ex);
                }
            }
        }
        
        if (journalStore != null) {
        	try {
				journalStore.close();
			} catch (ZmqException ex) {
				LOGGER.log(Level.SEVERE, "Unable to close the journal store: " + journalStore, ex);
			}
        }

        active.set(false);

        if (listenerExecutor != null) {
            try {
                listenerExecutor.shutdown();
                final boolean success = listenerExecutor.awaitTermination(3, TimeUnit.SECONDS);

                if (!success) {
                    LOGGER.severe("Listener threads failed to stop: " + toString());
                }
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Listener threads failed to stop: " + toString(), ex);
            }
        }

        if (socketExecutor != null) {
            try {
                socketExecutor.shutdown();
                final boolean success = socketExecutor.awaitTermination(3, TimeUnit.SECONDS);

                if (!success) {
                    LOGGER.severe("Socket threads failed to stop: " + toString());
                }
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Socket threads failed to stop: " + toString(), ex);
            }
        }

        // dump all tracked missing
        if (acknowledge) {
            for (TrackEvent tackMessage : trackEventMap.values()) {
                LOGGER.warning("Gateway " + name + " has un-acknowledged message (LOST): " + tackMessage);
            }

            trackEventMap.clear();
        }

        LOGGER.info("Gateway closed: " + toString());
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void commit() throws ZmqException {
        if (!active.get()) {
            throw new ZmqException("The gateway has been close: " + toString());
        }

        if (!transacted) {
            throw new ZmqException("No transacion started: " + toString());
        }

        synchronized (outgoingSnapshot) {
        	for (ZmqSendEvent event : outgoingSnapshot) {
                outgoingQueue.add(event);

                if (journalStore != null) {
                	journalStore.create(event.getMessageId(), event.getMessage());            	
                }
            }

            outgoingSnapshot.clear();
        }

        synchronized (incomingSnapshot) {
            if (redelivery != null) {
                redelivery.delivered(incomingSnapshot);

            }

            for (final ZmqSendEvent event : incomingSnapshot) {
        		journalStore.delete(event.getMessageId());            	
            }

            incomingSnapshot.clear();
        }

        LOGGER.fine("Transaction committed: " + toString());
    }

    @Override
    public void rollback() throws ZmqException {
        if (!active.get()) {
            throw new ZmqException("The gateway has been close: " + toString());
        }

        if (!transacted) {
            throw new ZmqException("No transacion started: " + toString());
        }

        synchronized (outgoingSnapshot) {
            outgoingSnapshot.clear();
        }

        synchronized (incomingSnapshot) {
            if (redelivery != null) {
                redelivery.redeliver(incomingSnapshot);
            }

            incomingSnapshot.clear();
        }

        LOGGER.fine("Transaction rolledback: " + toString());
    }

    @Override
    public void send(final ZmqMessage message) throws ZmqException {
        final ZmqSendEvent event = eventHandler.createSendEvent(message);

        if (transacted) {
            synchronized (outgoingSnapshot) {
                outgoingSnapshot.add(event);
            }
        } else {
            outgoingQueue.add(event);
            
            if (journalStore != null) {
            	journalStore.create(event.getMessageId(), event.getMessage());            	
            }
        }
    }
    
    /**
     * Return true when the message passes the JMS selector or non specified.
     * @param message  the message
     * @return         return true on selector pass or no selector specified.
     */
    protected boolean isValidMessage(final ZmqMessage message) {
        if (messageSelector != null) {
            final Map<String, Object> variables = message.getProperties();
            final boolean validMessage = messageSelector.evaluate(variables);

            return validMessage;
        }

        return true;
    }

    @Override
    public ZmqMessage receive() throws ZmqException {
        ZmqMessage message = receive(SOCKET_WAIT_MILLI_SECOND);

        while (message == null && active.get()) {
            message = receive(SOCKET_WAIT_MILLI_SECOND);
        }

        if (message == null) {
            throw new ZmqException("The gateway has been close: " + toString());
        }

        return message;
    }

    @Override
    public ZmqMessage receive(final int timeout) throws ZmqException {
        Stopwatch stopwatch = null;

        if (LOGGER.isLoggable(Level.FINER)) {
            stopwatch = new Stopwatch();

        }
        if (!active.get()) {
            throw new ZmqException("The gateway has been close: " + toString());
        }

        // check for re-delivers
        if (redelivery != null) {
            final ZmqSendEvent event = redelivery.getNextRedeliver();

            if (event != null) {
                final ZmqMessage message = event.getMessage();

                if (transacted) {
                    synchronized (incomingSnapshot) {
                        incomingSnapshot.add(event);
                    }
                }

                if (stopwatch != null) {
                    LOGGER.log(Level.FINER, "Receive re-delivery message: " + stopwatch.elapsedTime() + " (msec)");
                }

                return message;
            }
        }

        // check the journal store for any messages.
        if (journalStore != null) {
        	ZmqJournalEntry journalEntry = journalStore.read();
        	
        	if (journalEntry != null) {
                if (transacted) {
                    final ZmqSendEvent event =
                        eventHandler.createSendEvent(journalEntry.getMessageId(), journalEntry.getMessage());

                    synchronized (outgoingSnapshot) {
                        outgoingSnapshot.add(event);
                    }
                } else {
            		journalStore.delete(journalEntry.getMessageId());
                }
                
                return journalEntry.getMessage();
        	}
        }
        
        // check the message from the JeroMQ queue.
        final long startTime = System.currentTimeMillis();

        try {
            final ZmqSendEvent event = incomingQueue.poll(timeout, TimeUnit.MILLISECONDS);

            if (event != null) {
                ZmqMessage message = event.getMessage();
                if (isValidMessage(message)) {
                    // when transacted kept track of messages for roll-back
                    if (transacted) {
                        synchronized (incomingSnapshot) {
                            incomingSnapshot.add(event);
                        }
                    } else {
                    	if (journalStore != null) {
                    		journalStore.create(event.getMessageId(), message);
                    	}
                    }

                    if (stopwatch != null) {
                        LOGGER.log(Level.FINER, "Receive incoming message: " + stopwatch.elapsedTime() + " (msec)");
                    }

                    return message;
                }
            }
        } catch (InterruptedException ex) {
            throw new ZmqException("Unable to poll internal queue: " + toString(), ex);
        }

        final long endTime = System.currentTimeMillis();
        final long lapsedTime = endTime - startTime;

        if (lapsedTime < timeout) {
            final int remainingTimeout = (int) (timeout - lapsedTime);

            final ZmqMessage message = receive(remainingTimeout);

            if (stopwatch != null) {
                if (message == null) {
                    LOGGER.log(Level.FINER, "Receive incoming message (Wait): " + stopwatch.elapsedTime() + " (msec)");
                } else {
                    LOGGER.log(Level.FINER, "Receive no message (Timeout): " + stopwatch.elapsedTime() + " (msec)");
                }
            }

            return message;
        }

        return null;
    }

    @Override
    public void setListener(final ZmqGatewayListener listener) {
        if (listener != null && this.listener == null) {
            ListenerThread listenerThread = new ListenerThread();
            listenerExecutor.execute(listenerThread);
        }

        this.listener = listener;
    }

    /**
     * @return  return the metric of the protocol, or null when nothing is being measured.
     */
    public List<ZmqSocketMetrics> getMetrics() {
        return metrics;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getAddr() {
        return addr;
    }

    @Override
    public ZmqSocketType getType() {
        return type;
    }

    @Override
    public boolean isBound() {
        return bound;
    }

    @Override
    public boolean isTransacted() {
        return transacted;
    }

    @Override
    public boolean isAcknowledged() {
        return acknowledge;
    }

    @Override
    public boolean isHeartbeat() {
        return heartbeat;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public Date getStartTime() {
        return startDateTime;
    }

    /**
     * @return  return the ZMQ context
     */
    protected ZMQ.Context getContext() {
        return context;
    }

    /**
     * @return  return the JMS to ZMQ filter resolver
     */
    protected ZmqFilterPolicy getFilterPolicy() {
        return filterPolicy;
    }

    /**
     * @return  return the ZMQ, protocol, JMS message marshaler
     */
    protected ZmqEventHandler getEventHandler() {
        return eventHandler;
    }

    /**
     * @return  return the JMS message selector or null when no selection applied
     */
    protected ZmqMessageSelector getMessageSelector() {
        return messageSelector;
    }

    @Override
    public String toString() {
        return getClass().getCanonicalName() 
        	+" [active=" + active + ", name=" + name + ", type=" + type + ", isBound=" + bound + ", addr=" + addr
            + ", transacted=" + transacted + ", acknowleged=" + acknowledge + ", heartbeat=" + heartbeat + ", direction=" + direction 
            + ", eventHandler=" + eventHandler + ", journalStore=" + journalStore + "]";
    }
}
