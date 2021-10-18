package com._247.logging.binlog;

import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class BinLogEventReader  extends LogEvent {
    private List<Integer> attributeIndex = null;
    private boolean indexDone = false;
    private ByteBuffer buffer = null;
    private static List<BinLogEventReader.LogEventHeaderField> logEventHeaderFields = null;
    static Logger logger = Logger.getLogger(BinLogEventReader.class);



    public BinLogEventReader() {

        init(true);
    }


    private void init(boolean force)
    {
        if(this.buffer != null && force == false)
            return;

        int minimalBufferSize = ((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTNAME.ordinal())).getStartOffset();
        ++minimalBufferSize;
        minimalBufferSize += 4;
        this.buffer = ByteBuffer.allocate(minimalBufferSize);
        this.attributeIndex = new ArrayList();
        this.addDefaultHeaderAttributes();
    }

    public static BinLogEventReader makeLogEventFromBytes(byte[] eventBytes) {
        BinLogEventReader event = new BinLogEventReader();
        event.buffer = ByteBuffer.wrap(eventBytes);
        return event;
    }

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    public void addAttribute(LogAttribute attribute) throws LogException {
        logger.debug("adding attribute [" + attribute.getName() + "]");
        byte[] attributeData = attribute.getBytes();
        ByteBuffer newBuffer = ByteBuffer.allocate(this.buffer.capacity() + attributeData.length);
        newBuffer.position(0);
        byte[] tmpBytes = new byte[this.buffer.capacity() - 4];
        this.buffer.position(0);
        this.buffer.get(tmpBytes, 0, tmpBytes.length);
        newBuffer.put(tmpBytes);
        newBuffer.put(attributeData);
        BinlogReaderUtils.putUnsignedInt(newBuffer, (long)newBuffer.capacity());
        this.buffer = newBuffer;
        this.fixEventLength();

        indexDone = false;
    }

    public LogAttribute getAttribute(int index) throws LogException {
        return index < BinLogEventReader.LogHeaders.TM_LOGV4_NUM_HEADER_ATTRS.ordinal() ? this.getHeaderAttribute(index) : this.getBodyAttribute(index - BinLogEventReader.LogHeaders.TM_LOGV4_NUM_HEADER_ATTRS.ordinal());
    }

    public int getNumAttributes() throws LogException {
        this.indexAttributes();
        return BinLogEventReader.LogHeaders.TM_LOGV4_NUM_HEADER_ATTRS.ordinal() + this.attributeIndex.size();
    }

    public int getVersion() {
        return this.buffer.get(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_VERSION.ordinal())).getStartOffset());
    }

    public void setVersion(int version) {
        indexDone = false;
        init(false);
        this.buffer.put(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_VERSION.ordinal())).getStartOffset(), (byte)version);
    }

    public int getLogClass() {
        return this.buffer.getShort(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_CLASS.ordinal())).getStartOffset());
    }

    public void putLogClass(short logClassEnumVal) {
        init(false);
        this.buffer.putShort(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_CLASS.ordinal())).getStartOffset(), logClassEnumVal);
    }

    public int getLogName() {
        return this.buffer.getShort(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_NAME.ordinal())).getStartOffset());
    }

    public void putLogName(short logNameEnumVal) {
        indexDone = false;
        init(false);
        this.buffer.putShort(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_NAME.ordinal())).getStartOffset(), logNameEnumVal);
    }

    public BigInteger getCreateTime() {
        long time = this.buffer.getLong(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_CREATION.ordinal())).getStartOffset());
        return new BigInteger(ByteBuffer.allocate(8).putLong(time).array());
    }

    public void putCreateTime(BigInteger createTime) {
        init(false);

        indexDone = false;
        int position = this.buffer.position();
        this.buffer.position(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_CREATION.ordinal())).getStartOffset());
        byte[] createTimeBytes = createTime.toByteArray();

        for(int padBytes = 8 - createTimeBytes.length; padBytes > 0; --padBytes) {
            this.buffer.put((byte)0);
        }

        this.buffer.put(createTime.toByteArray(), 0, createTimeBytes.length);
        this.buffer.position(position);
    }

    public BigInteger getWrittenTime() {
        long time = this.buffer.getLong(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_WRITTEN.ordinal())).getStartOffset());
        return new BigInteger(ByteBuffer.allocate(8).putLong(time).array());
    }

    public void putWrittenTime(BigInteger writtenTime) {

        init(false);

        indexDone = false;
        int position = this.buffer.position();
        this.buffer.position(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_WRITTEN.ordinal())).getStartOffset());
        byte[] timeBytes = writtenTime.toByteArray();

        for(int padBytes = 8 - timeBytes.length; padBytes > 0; --padBytes) {
            this.buffer.put((byte)0);
        }

        this.buffer.put(writtenTime.toByteArray(), 0, timeBytes.length);
        this.buffer.position(position);
    }

    public long getEventId() {
        return BinlogReaderUtils.getUnsignedInt(this.buffer, ((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_EVENTID.ordinal())).getStartOffset());
    }

    public void putEventId(long eventId) {

        init(false);

        indexDone = false;
        BinlogReaderUtils.putUnsignedInt(this.buffer, eventId, ((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_EVENTID.ordinal())).getStartOffset());
    }

    public UUID getUUID() {
        return new UUID(this.buffer.getLong(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_UUID.ordinal())).getStartOffset()), this.buffer.getLong(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_UUID.ordinal())).getStartOffset() + 8));
    }

    public void putUUID(UUID uuid) {
        init(false);
        indexDone = false;
        this.buffer.putLong(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_UUID.ordinal())).getStartOffset(), uuid.getMostSignificantBits());
        this.buffer.putLong(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_UUID.ordinal())).getStartOffset() + 8, uuid.getLeastSignificantBits());
    }

    public int getHostSize() {
        return this.buffer.get(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTSIZE.ordinal())).getStartOffset());
    }

    public String getHostname() throws UnsupportedEncodingException {
        int hostSize = this.getHostSize();
        if (hostSize == 0) {
            return "";
        } else {
            --hostSize;
            byte[] hostBytes = new byte[hostSize];
            logger.debug("reading hostname starting at [" + ((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTNAME.ordinal())).getStartOffset() + "] " + "and reading [" + hostSize + "] bytes. Buffer size [" + this.buffer.capacity() + "]");
            int currPosition = this.buffer.position();
            this.buffer.position(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTNAME.ordinal())).getStartOffset());
            this.buffer.get(hostBytes, 0, hostSize);
            this.buffer.position(currPosition);
            return new String(hostBytes, Charset.forName("UTF-8"));
        }
    }

    public void putHostname(String hostname) {

        init(false);

        indexDone = false;
        if (hostname.charAt(hostname.length() - 1) != 0) {
            hostname = hostname + '\u0000';
        }

        logger.debug("updating hostname to [" + hostname + "][" + hostname.length() + "]");
        int currentHostSize = this.getHostSize();
        if (currentHostSize == 0) {
            currentHostSize = 1;
        }

        int sizeDiff = hostname.length() - currentHostSize;
        ByteBuffer newBuffer = ByteBuffer.allocate(this.buffer.capacity() + sizeDiff);
        logger.debug("new event size in putHostname is [" + newBuffer.capacity() + "] updated from [" + this.buffer.capacity() + "]");
        newBuffer.position(0);
        byte[] headers = new byte[((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTSIZE.ordinal())).getStartOffset()];
        this.buffer.position(0);
        logger.debug("getting " + headers.length + " bytes from old buffer");
        this.buffer.get(headers, 0, headers.length);
        newBuffer.put(headers);
        newBuffer.put((byte)hostname.length());
        newBuffer.put(hostname.getBytes());
        this.buffer.position(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTNAME.ordinal())).getStartOffset() + currentHostSize);
        byte[] attributes = new byte[newBuffer.capacity() - newBuffer.position()];
        logger.debug("getting " + attributes.length + " bytes of attributes starting at " + this.buffer.position());
        this.buffer.get(attributes, 0, attributes.length);
        logger.debug("writing " + attributes.length + " bytes of attributes starting at " + newBuffer.position());
        newBuffer.put(attributes);
        this.buffer = newBuffer;
        this.buffer.position(this.buffer.capacity());
        this.fixEventLength();
    }


    public Map<String,String[]> getBotyAttributeValuesByNames(Set<String> names) throws LogException {

        Set<String> lowerCaseNames = names.stream().map( s -> s.toLowerCase()).collect(Collectors.toSet());

        Map<String,List<String>> nametoValuesMap = new HashMap();

        if (names != null && !names.isEmpty()) {
            int attributeCount = this.getNumAttributes();
            LogAttribute attribute = null;

            for(int i = 0; i < attributeCount; ++i) {
                attribute = this.getAttribute(i);

                String attrName = attribute.getName().toLowerCase();

                if(lowerCaseNames.contains(attrName))
                {
                    List<String> attrs = null;
                    if(nametoValuesMap.containsKey(attrName))
                    {
                        attrs = nametoValuesMap.get(attrName);
                    }
                    else
                    {
                        attrs = new ArrayList();
                    }

                    attrs.add(attribute.getValueAsString());

                    nametoValuesMap.put(attrName,attrs);
                }
            }

            Map<String,String[]> nametoValuesArrayMap = new HashMap();

            nametoValuesMap.keySet().stream().forEach( r ->
            {
                String[] strArray = new String[0];
                String[] values = nametoValuesMap.get(r).toArray(strArray);
                nametoValuesArrayMap.put(r,values);
            });

            return nametoValuesArrayMap;
        } else {
            return null;
        }
    }

    public String[] getBodyAttributeValuesByName(String name) throws LogException {
        List<String> attrs = new ArrayList();
        if (name != null && !name.isEmpty()) {
            int attributeCount = this.getNumAttributes();
            LogAttribute attribute = null;

            for(int i = 0; i < attributeCount; ++i) {
                attribute = this.getAttribute(i);
                if (attribute.getName().equalsIgnoreCase(name)) {
                    attrs.add(attribute.getValueAsString());
                }
            }

            String[] stringArray = (String[])attrs.toArray(new String[0]);
            return stringArray;
        } else {
            return null;
        }
    }

    private void fixEventLength() {
        int position = this.buffer.position();
        this.buffer.position(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_SIZE.ordinal())).getStartOffset());
        BinlogReaderUtils.putUnsignedInt(this.buffer, (long)this.buffer.capacity());
        this.buffer.position(this.buffer.capacity() - 4);
        BinlogReaderUtils.putUnsignedInt(this.buffer, (long)this.buffer.capacity());
        this.buffer.position(position);
    }

    private void addDefaultHeaderAttributes() {
        init(false);
        indexDone = false;
        this.buffer.position(0);
        this.buffer.put((byte)4);
        BinlogReaderUtils.putUnsignedInt(this.buffer, (long)this.buffer.capacity());
        this.putLogClass((short)0);
        this.putLogName((short)0);
        BigInteger now = (new BigInteger((new Long(System.currentTimeMillis())).toString())).multiply(new BigInteger("1000"));
        this.putWrittenTime(now);
        this.putCreateTime(now);
        this.putEventId(0L);
        this.putUUID(UUID.randomUUID());
        this.buffer.put(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTSIZE.ordinal())).getStartOffset(), (byte)0);
        this.putHostname(Hostname);
        this.fixEventLength();
    }

    private LogAttribute getHeaderAttribute(int index) throws LogException {
        int headerType = 0;
        Object value = null;
        int id = 0;
        if (index == BinLogEventReader.LogHeaders.TM_LOGV4_VERSION.ordinal()) {
            headerType = 3;
            id = 1;
            value = new Integer(this.getVersion());
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_SIZE.ordinal()) {
            headerType = 4;
            id = 2;
            value = new Long((long)this.buffer.capacity());
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_CLASS.ordinal()) {
            headerType = 21;
            id = 3;
            value = new Integer(this.getLogClass());
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_NAME.ordinal()) {
            headerType = 21;
            id = 4;
            value = new Integer(this.getLogName());
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_WRITTEN.ordinal()) {
            headerType = 18;
            id = 5;
            value = this.getWrittenTime();
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_CREATION.ordinal()) {
            headerType = 18;
            id = 6;
            value = this.getCreateTime();
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_EVENTID.ordinal()) {
            headerType = 4;
            id = 7;
            value = new Long(this.getEventId());
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_UUID.ordinal()) {
            headerType = 15;
            id = 8;
            value = this.getUUID();
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_HOSTSIZE.ordinal()) {
            headerType = 3;
            id = 9;
            value = new Integer(this.getHostSize());
        } else if (index == BinLogEventReader.LogHeaders.TM_LOGV4_HOSTNAME.ordinal()) {
            headerType = 1;
            id = 10;

            try {
                value = this.getHostname();
            } catch (UnsupportedEncodingException var6) {
                throw new LogException(LogExceptionCodeEnum.BAD_FORMAT, "unable to encode hostname value " + var6.getMessage());
            }
        }

        return new LogAttribute(headerType, id, ((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(index)).getFieldName(), value);
    }

    private LogAttribute getBodyAttribute(int index) throws LogException {
        this.indexAttributes();
        if (index > this.attributeIndex.size()) {
            throw new LogException(LogExceptionCodeEnum.OUT_OF_BOUNDS, "Attribute index [" + index + "] is is passed known attribute list size [" + this.attributeIndex.size() + "]");
        } else {
            int attrIndex = (Integer)this.attributeIndex.get(index);
            this.buffer.position(attrIndex);
            return LogAttribute.makeAttributeFromBuffer(this.buffer);
        }
    }

    private void indexAttributes() throws LogException {
        if(indexDone == true)
            return;


        this.attributeIndex.clear();
        int headerSize = ((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTNAME.ordinal())).getStartOffset() + this.buffer.get(((BinLogEventReader.LogEventHeaderField)logEventHeaderFields.get(BinLogEventReader.LogHeaders.TM_LOGV4_HOSTSIZE.ordinal())).getStartOffset());
        logger.debug("computed headerSize [" + headerSize + "]");
        int footerIndex = this.buffer.capacity() - 4;
        this.buffer.position(headerSize);

        while(this.buffer.position() < footerIndex) {
            this.attributeIndex.add(this.buffer.position());
            LogAttribute.findNextAttribute(this.buffer);
        }

        if (this.buffer.position() != footerIndex) {
            logger.error("not at footer: [" + this.buffer.position() + "] != [" + footerIndex + "]");
            throw new LogException(LogExceptionCodeEnum.BAD_FORMAT, "finished going through attributes but not at footer");
        }

        indexDone = true;
    }

    static {
        ArrayList<BinLogEventReader.LogEventHeaderField> tmpLogEventHeaderFields = new ArrayList();
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.event.version", 0, 1));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.event.size", 1, 4));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.event.class", 5, 2));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.event.name", 7, 2));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.time.written", 9, 8));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.time.creation", 17, 8));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.event.id", 25, 4));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("uuid", 29, 16));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.hostname.size", 45, 1));
        tmpLogEventHeaderFields.add(new BinLogEventReader.LogEventHeaderField("log.hostname", 46, 0));
        logEventHeaderFields = Collections.unmodifiableList(tmpLogEventHeaderFields);

        try {
            Hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException var2) {
            logger.warn("Unable to obtain localhost hostname...");
        }

    }

    private static class LogEventHeaderField {
        private String fieldName;
        private int startOffset;
        private int length;

        LogEventHeaderField(String fieldName, int startOffset, int length) {
            this.fieldName = fieldName;
            this.startOffset = startOffset;
            this.length = length;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public int getStartOffset() {
            return this.startOffset;
        }

        public int getLength() {
            return this.length;
        }
    }

    private static enum LogHeaders {
        TM_LOGV4_VERSION,
        TM_LOGV4_SIZE,
        TM_LOGV4_CLASS,
        TM_LOGV4_NAME,
        TM_LOGV4_WRITTEN,
        TM_LOGV4_CREATION,
        TM_LOGV4_EVENTID,
        TM_LOGV4_UUID,
        TM_LOGV4_HOSTSIZE,
        TM_LOGV4_HOSTNAME,
        TM_LOGV4_NUM_HEADER_ATTRS;

        private LogHeaders() {
        }
    }
}