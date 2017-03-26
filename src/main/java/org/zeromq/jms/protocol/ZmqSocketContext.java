package org.zeromq.jms.protocol;

/**
 * ZMQ socket configuration.
 */
public class ZmqSocketContext {

    private String addr;
    private ZmqSocketType type;
    private boolean bindFlag;
    private int recieveMsgFlag;

    private Long linger;
    private Long reconnectIVL;
    private Long backlog;
    private Long reconnectIVLMax;
    private Long maxMsgSize;
    private Long sndHWM;
    private Long rcvHWM;
    private Long affinity;

    private byte[] identity;

    private Long rate;
    private long recoveryInterval;

    private Boolean reqCorrelate;
    private Boolean reqRelaxed;

    private Long multicastHops;
    private Integer receiveTimeOut;
    private Integer sendTimeOut;

    private Long tcpKeepAlive;
    private Long tcpKeepAliveCount;
    private Long tcpKeepAliveInterval;
    private Long tcpKeepAliveIdle;

    private Long sendBufferSize;
    private Long receiveBufferSize;
    private Boolean routerMandatory;
    private Boolean xpubVerbose;
    private Boolean ipv4Only;
    private Boolean delayAttachOnConnect;

    /**
     * Construct empty socket context.
     */
    public ZmqSocketContext() {
    }

    /**
     * Construct socket context with mandatory fields.
     * @param addr            the address
     * @param type            the type of session, i.e. PUSH, PULL, DEALER, etc..
     * @param bindFlag        the flag to specify a bind or connect the socket to the address
     * @param recieveMsgFlag  the message receive flag, i.e. 0 = Do not wait.
     */
    public ZmqSocketContext(final String addr, final ZmqSocketType type, final boolean bindFlag, final int recieveMsgFlag) {

        this.addr = addr;
        this.type = type;
        this.bindFlag = bindFlag;
        this.recieveMsgFlag = recieveMsgFlag;
    }

    /**
     * Copy construct to make the instance immutable.
     * @param context  the context to copy
     */
    public ZmqSocketContext(final ZmqSocketContext context) {
        this.addr = context.addr;
        this.type = context.type;
        this.bindFlag = context.bindFlag;
        this.recieveMsgFlag = context.recieveMsgFlag;

        this.linger = context.affinity;
        this.reconnectIVL = context.reconnectIVL;
        this.backlog = context.backlog;
        this.reconnectIVLMax = context.reconnectIVLMax;
        this.maxMsgSize = context.maxMsgSize;
        this.sndHWM = context.sndHWM;
        this.rcvHWM = context.rcvHWM;
        this.affinity = context.affinity;

        this.identity = context.getIdentity();

        this.rate = context.rate;
        this.recoveryInterval = context.recoveryInterval;

        this.reqCorrelate = context.reqCorrelate;
        this.reqRelaxed = context.reqRelaxed;

        this.multicastHops = context.multicastHops;
        this.receiveTimeOut = context.receiveTimeOut;
        this.sendTimeOut = context.sendTimeOut;

        this.tcpKeepAlive = context.tcpKeepAlive;
        this.tcpKeepAliveCount = context.tcpKeepAliveCount;
        this.tcpKeepAliveInterval = context.tcpKeepAliveInterval;
        this.tcpKeepAliveIdle = context.tcpKeepAliveIdle;

        this.sendBufferSize = context.sendBufferSize;
        this.receiveBufferSize = context.receiveBufferSize;
        this.routerMandatory = context.routerMandatory;
        this.xpubVerbose = context.xpubVerbose;
        this.ipv4Only = context.ipv4Only;
        this.delayAttachOnConnect = context.delayAttachOnConnect;
    }

    /**
     * @return  return the address/addresses
     */
    public String getAddr() {
        return addr;
    }

    /**
     * Set the socket address/addresses.
     * @param addr  the address value
     */
    public void setAddr(final String addr) {
        this.addr = addr;
    }

    /**
     * @return  return the socket type, i.e. PULL, PUSH, etc...
     */
    public ZmqSocketType getType() {
        return type;
    }

    /**
     * Change the type of socket, i.e. PULL, PUSH, etc...
     * @param  type   the type of socket
     */
    public void setType(final ZmqSocketType type) {
        this.type = type;
    }

    /**
     * @return  return TRUE when to socket should bind to the address
     */
    public boolean isBindFlag() {
        return bindFlag;
    }

    /**
     * Set the bind/connect flag, "true" is to bind.
     * @param bindFlag  the bind flag value
     */
    public void setBindFlag(final boolean bindFlag) {
        this.bindFlag = bindFlag;
    }

    /**
     * @return  return the message receive flag
     */
    public int getRecieveMsgFlag() {
        return recieveMsgFlag;
    }

    /**
     * Set the ZMQ "revcieveMsgFlag", sued on the receive(...., flag) function.
     * @param recieveMsgFlag  the new value, or 0 for no wait.
     */
    public void setRecieveMsgFlag(final int recieveMsgFlag) {
        this.recieveMsgFlag = recieveMsgFlag;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getLinger() {
        return linger;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  linger  the value to set to
     */
    public void setLinger(final Long linger) {
        this.linger = linger;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getReconnectIVL() {
        return reconnectIVL;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reconnectIVL  the value to set to
     */
    public void setReconnectIVL(final Long reconnectIVL) {
        this.reconnectIVL = reconnectIVL;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getBacklog() {
        return backlog;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  backlog  the value to set to
     */
    public void setBacklog(final Long backlog) {
        this.backlog = backlog;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getReconnectIVLMax() {
        return reconnectIVLMax;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reconnectIVLMax  the value to set to
     */
    public void setReconnectIVLMax(final Long reconnectIVLMax) {
        this.reconnectIVLMax = reconnectIVLMax;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getMaxMsgSize() {
        return maxMsgSize;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  maxMsgSize  the value to set to
     */
    public void setMaxMsgSize(final Long maxMsgSize) {
        this.maxMsgSize = maxMsgSize;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getSndHWM() {
        return sndHWM;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  sndHWM  the value to set to
     */
    public void setSndHWM(final Long sndHWM) {
        this.sndHWM = sndHWM;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getRcvHWM() {
        return rcvHWM;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  rcvHWM  the value to set to
     */
    public void setRcvHWM(final Long rcvHWM) {
        this.rcvHWM = rcvHWM;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getAffinity() {
        return affinity;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  affinity  the value to set to
     */
    public void setAffinity(final Long affinity) {
        this.affinity = affinity;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public byte[] getIdentity() {
        return identity;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  identity  the value to set to
     */
    public void setIdentity(final byte[] identity) {
        this.identity = identity;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getRate() {
        return rate;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  rate  the value to set to
     */
    public void setRate(final Long rate) {
        this.rate = rate;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public long getRecoveryInterval() {
        return recoveryInterval;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  recoveryInterval  the value to set to
     */
    public void setRecoveryInterval(final long recoveryInterval) {
        this.recoveryInterval = recoveryInterval;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getReqCorrelate() {
        return reqCorrelate;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reqCorrelate  the value to set to
     */
    public void setReqCorrelate(final Boolean reqCorrelate) {
        this.reqCorrelate = reqCorrelate;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getReqRelaxed() {
        return reqRelaxed;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  reqRelaxed  the value to set to
     */
    public void setReqRelaxed(final Boolean reqRelaxed) {
        this.reqRelaxed = reqRelaxed;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getMulticastHops() {
        return multicastHops;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  multicastHops  the value to set to
     */
    public void setMulticastHops(final Long multicastHops) {
        this.multicastHops = multicastHops;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Integer getReceiveTimeOut() {
        return receiveTimeOut;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  receiveTimeOut  the value to set to
     */
    public void setReceiveTimeOut(final Integer receiveTimeOut) {
        this.receiveTimeOut = receiveTimeOut;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Integer getSendTimeOut() {
        return sendTimeOut;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  sendTimeOut  the value to set to
     */
    public void setSendTimeOut(final Integer sendTimeOut) {
        this.sendTimeOut = sendTimeOut;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAlive() {
        return tcpKeepAlive;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAlive  the value to set to
     */
    public void setTcpKeepAlive(final Long tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAliveCount() {
        return tcpKeepAliveCount;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAliveCount  the value to set to
     */
    public void setTcpKeepAliveCount(final Long tcpKeepAliveCount) {
        this.tcpKeepAliveCount = tcpKeepAliveCount;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAliveInterval() {
        return tcpKeepAliveInterval;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAliveInterval  the value to set to
     */
    public void setTcpKeepAliveInterval(final Long tcpKeepAliveInterval) {
        this.tcpKeepAliveInterval = tcpKeepAliveInterval;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getTcpKeepAliveIdle() {
        return tcpKeepAliveIdle;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  tcpKeepAliveIdle  the value to set to
     */
    public void setTcpKeepAliveIdle(final Long tcpKeepAliveIdle) {
        this.tcpKeepAliveIdle = tcpKeepAliveIdle;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getSendBufferSize() {
        return sendBufferSize;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  sendBufferSize  the value to set to
     */
    public void setSendBufferSize(final Long sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Long getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  receiveBufferSize  the value to set to
     */
    public void setReceiveBufferSize(final Long receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getRouterMandatory() {
        return routerMandatory;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  routerMandatory  the value to set to
     */
    public void setRouterMandatory(final Boolean routerMandatory) {
        this.routerMandatory = routerMandatory;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getXpubVerbose() {
        return xpubVerbose;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  xpubVerbose  the value to set to
     */
    public void setXpubVerbose(final Boolean xpubVerbose) {
        this.xpubVerbose = xpubVerbose;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getIpv4Only() {
        return ipv4Only;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  ipv4Only  the value to set to
     */
    public void setIpv4Only(final Boolean ipv4Only) {
        this.ipv4Only = ipv4Only;
    }

    /**
     * @return  see <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     */
    public Boolean getDelayAttachOnConnect() {
        return delayAttachOnConnect;
    }

    /**
     * See <a href="https://www.javadoc.io/doc/org.zeromq/jeromq/0.3.6">jeromq </a> java documentation for more detail.
     * @param  delayAttachOnConnect  the value to set to
     */
    public void setDelayAttachOnConnect(final Boolean delayAttachOnConnect) {
        this.delayAttachOnConnect = delayAttachOnConnect;
    }

    @Override
    public String toString() {
        return "ZmqSocketContext [addr=" + addr + ", type=" + type + ", bindFlag=" + bindFlag
            + ", recieveMsgFlag=" + recieveMsgFlag + "]";
    }
}
