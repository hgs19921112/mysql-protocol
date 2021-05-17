package com.luxin.mysql.protocol;

/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Buffer contains code to read and write packets from/to the MySQL server.
 */
public class Buffer {
    public final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    static final String CharSet = MysqlProtocolConstants.CHARACTER_SET;
    static final int MAX_BYTES_TO_DUMP = 512;

    static final int NO_LENGTH_LIMIT = -1;

    static final long NULL_LENGTH = -1;

    private int bufLength = 0;

    private byte[] byteBuffer;

    private int position = 0;

    protected boolean wasMultiPacket = false;

    /* Type ids of response packets. */
    public static final short TYPE_ID_ERROR = 0xFF;
    public static final short TYPE_ID_EOF = 0xFE;
    /** It has the same signature as EOF, but may be issued by server only during handshake phase **/
    public static final short TYPE_ID_AUTH_SWITCH = 0xFE;
    public static final short TYPE_ID_LOCAL_INFILE = 0xFB;
    public static final short TYPE_ID_OK = 0;

    public static final int HEADER_LENGTH=4;
    public Buffer(byte[] buf) {
        this.byteBuffer = buf;
        setBufLength(buf.length);
    }

    public Buffer(int size) {
        this.byteBuffer = new byte[size];
        setBufLength(this.byteBuffer.length);
        this.position = HEADER_LENGTH;
    }

    public final void clear() {
        this.position = HEADER_LENGTH;
    }

    public final void dump() {
        dump(getBufLength());
    }

    public final String dump(int numBytes) {
        return StringUtils.dumpAsHex(getBytes(0, numBytes > getBufLength() ? getBufLength() : numBytes), numBytes > getBufLength() ? getBufLength() : numBytes);
    }

    public final String dumpClampedBytes(int numBytes) {
        int numBytesToDump = numBytes < MAX_BYTES_TO_DUMP ? numBytes : MAX_BYTES_TO_DUMP;

        String dumped = StringUtils.dumpAsHex(getBytes(0, numBytesToDump > getBufLength() ? getBufLength() : numBytesToDump),
                numBytesToDump > getBufLength() ? getBufLength() : numBytesToDump);

        if (numBytesToDump < numBytes) {
            return dumped + " ....(packet exceeds max. dump length)";
        }

        return dumped;
    }

    public  void dumpHeader() {
        for (int i = 0; i < HEADER_LENGTH; i++) {
            String hexVal = Integer.toHexString(readByte(i) & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal;
            }

            System.out.print(hexVal + " ");
        }
    }

    public  void dumpNBytes(int start, int nBytes) {
        StringBuilder asciiBuf = new StringBuilder();

        for (int i = start; (i < (start + nBytes)) && (i < getBufLength()); i++) {
            String hexVal = Integer.toHexString(readByte(i) & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal;
            }

            System.out.print(hexVal + " ");

            if ((readByte(i) > 32) && (readByte(i) < 127)) {
                asciiBuf.append((char) readByte(i));
            } else {
                asciiBuf.append(".");
            }

            asciiBuf.append(" ");
        }

        System.out.println("    " + asciiBuf.toString());
    }

    public  void ensureCapacity(int additionalData)  {
        if ((this.position + additionalData) > getBufLength()) {
            if ((this.position + additionalData) < this.byteBuffer.length) {
                // byteBuffer.length is != getBufLength() all of the time due to re-using of packets (we don't shrink them)
                //
                // If we can, don't re-alloc, just set buffer length to size of current buffer
                setBufLength(this.byteBuffer.length);
            } else {
                //
                // Otherwise, re-size, and pad so we can avoid allocing again in the near future
                //
                int newLength = (int) (this.byteBuffer.length * 1.25);

                if (newLength < (this.byteBuffer.length + additionalData)) {
                    newLength = this.byteBuffer.length + (int) (additionalData * 1.25);
                }

                if (newLength < this.byteBuffer.length) {
                    newLength = this.byteBuffer.length + additionalData;
                }

                byte[] newBytes = new byte[newLength];

                System.arraycopy(this.byteBuffer, 0, newBytes, 0, this.byteBuffer.length);
                this.byteBuffer = newBytes;
                setBufLength(this.byteBuffer.length);
            }
        }
    }

    /**
     * Skip over a length-encoded string
     *
     * @return The position past the end of the string
     */
    public int fastSkipLenString() {
        long len = this.readFieldLength();

        this.position += len;

        return (int) len;
    }

    public void fastSkipLenByteArray() {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH || len == 0) {
            return;
        }

        this.position += len;
    }

    public  final byte[] getBufferSource() {
        return this.byteBuffer;
    }

    public int getBufLength() {
        return this.bufLength;
    }

    /**
     * Returns the array of bytes this Buffer is using to read from.
     *
     * @return byte array being read from
     */
    public byte[] getByteBuffer() {
        return this.byteBuffer;
    }

    public final byte[] getBytes(int len) {
        byte[] b = new byte[len];
        System.arraycopy(this.byteBuffer, this.position, b, 0, len);
        this.position += len; // update cursor

        return b;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.mysql.jdbc.Buffer#getBytes(int, int)
     */
    public  byte[] getBytes(int offset, int len) {
        byte[] dest = new byte[len];
        System.arraycopy(this.byteBuffer, offset, dest, 0, len);

        return dest;
    }

    public int getCapacity() {
        return this.byteBuffer.length;
    }

    public ByteBuffer getNioBuffer() {
        throw new IllegalArgumentException("Get NioBuffer error.");
    }

    /**
     * Returns the current position to write to/ read from
     *
     * @return the current position to write to/ read from
     */
    public int getPosition() {
        return this.position;
    }

    public final boolean isEOFPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_EOF && (getBufLength() <= 5);
    }

    final boolean isAuthMethodSwitchRequestPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_AUTH_SWITCH;
    }

    public final boolean isOKPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_OK;
    }

    public final boolean isResultSetOKPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_EOF && (getBufLength() < 16777215);
    }

    public final boolean isRawPacket() {
        return ((this.byteBuffer[0] & 0xff) == 1);
    }

    public final long newReadLength() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 251:
                return 0;

            case 252:
                return readInt();

            case 253:
                return readLongInt();

            case 254: // changed for 64 bit lengths
                return readLongLong();

            default:
                return sw;
        }
    }

    public final byte readByte() {
        return this.byteBuffer[this.position++];
    }

    public final byte readByte(int readAt) {
        return this.byteBuffer[readAt];
    }

    public final long readFieldLength() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 251:
                return NULL_LENGTH;

            case 252:
                return readInt();

            case 253:
                return readLongInt();

            case 254:
                return readLongLong();

            default:
                return sw;
        }
    }

    public final int readInt() {
        byte[] b = this.byteBuffer; // a little bit optimization

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);
    }

    public final int readIntAsLong() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16) | ((b[this.position++] & 0xff) << 24);
    }

    public final byte[] readLenByteArray(int offset) {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH) {
            return null;
        }

        if (len == 0) {
            return EMPTY_BYTE_ARRAY;
        }

        this.position += offset;

        return getBytes((int) len);
    }

    public final long readLength() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 251:
                return 0;

            case 252:
                return readInt();

            case 253:
                return readLongInt();

            case 254:
                return readLong();

            default:
                return sw;
        }
    }

    public final long readLong() {
        byte[] b = this.byteBuffer;

        return ((long) b[this.position++] & 0xff) | (((long) b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                | ((long) (b[this.position++] & 0xff) << 24);
    }

    public final int readLongInt() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16);
    }

    public final long readLongLong() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                | ((long) (b[this.position++] & 0xff) << 24) | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40)
                | ((long) (b[this.position++] & 0xff) << 48) | ((long) (b[this.position++] & 0xff) << 56);
    }

    public  final int readnBytes() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 1:
                return this.byteBuffer[this.position++] & 0xff;

            case 2:
                return this.readInt();

            case 3:
                return this.readLongInt();

            case 4:
                return (int) this.readLong();

            default:
                return 255;
        }
    }

    //
    // Read a null-terminated string
    //
    // To avoid alloc'ing a new byte array, we do this by hand, rather than calling getNullTerminatedBytes()
    //
    public final String readString() {
        int i = this.position;
        int len = 0;
        int maxLen = getBufLength();

        while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
            len++;
            i++;
        }

        String s = StringUtils.toString(this.byteBuffer, this.position, len);
        this.position += (len + 1); // update cursor

        return s;
    }



    public void setBufLength(int bufLengthToSet) {
        this.bufLength = bufLengthToSet;
    }

    /**
     * Sets the array of bytes to use as a buffer to read from.
     *
     * @param byteBufferToSet
     *            the array of bytes to use as a buffer
     */
    public void setByteBuffer(byte[] byteBufferToSet) {
        this.byteBuffer = byteBufferToSet;
    }

    /**
     * Set the current position to write to/ read from
     *
     * @param positionToSet
     *            the position (0-based index)
     */
    public void setPosition(int positionToSet) {
        this.position = positionToSet;
    }

    /**
     * Sets whether this packet was part of a multipacket
     *
     * @param flag
     *            was this packet part of a multipacket?
     */
    public void setWasMultiPacket(boolean flag) {
        this.wasMultiPacket = flag;
    }

    @Override
    public String toString() {
        return dumpClampedBytes(getPosition());
    }

    public String toSuperString() {
        return super.toString();
    }

    /**
     * Was this packet part of a multipacket?
     *
     * @return was this packet part of a multipacket?
     */
    public boolean wasMultiPacket() {
        return this.wasMultiPacket;
    }

    public final void writeByte(byte b) {
        ensureCapacity(1);

        this.byteBuffer[this.position++] = b;
    }

    // Write a byte array
    public final void writeBytesNoNull(byte[] bytes) {
        int len = bytes.length;
        ensureCapacity(len);
        System.arraycopy(bytes, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    // Write a byte array with the given offset and length
    public final void writeBytesNoNull(byte[] bytes, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(bytes, offset, this.byteBuffer, this.position, length);
        this.position += length;
    }

    public final void writeDouble(double d) throws SQLException {
        long l = Double.doubleToLongBits(d);
        writeLongLong(l);
    }

    /**
     * 写入 LengthEncodedInteger
     * @param length
     * @throws SQLException
     */
    public final void writeFieldLength(long length) {
        if (length < 251) {
            writeByte((byte) length);
        } else if (length < 65536L) {
            ensureCapacity(3);
            writeByte((byte) 252);
            writeInt((int) length);
        } else if (length < 16777216L) {
            ensureCapacity(4);
            writeByte((byte) 253);
            writeLongInt((int) length);
        } else {
            ensureCapacity(9);
            writeByte((byte) 254);
            writeLongLong(length);
        }
    }

    public final void writeFloat(float f)  {
        ensureCapacity(4);

        int i = Float.floatToIntBits(f);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    public final void writeInt(int i)  {
        ensureCapacity(2);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
    }

    // Write a String using the specified character encoding
    public final void writeLenBytes(byte[] b)  {
        int len = b.length;
        ensureCapacity(len + 9);
        writeFieldLength(len);
        System.arraycopy(b, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }


    public final void writeLong(long i)  {
        ensureCapacity(4);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    public final void writeLongInt(int i)  {
        ensureCapacity(3);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
    }

    public final void writeLongLong(long i) {
        ensureCapacity(8);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
        b[this.position++] = (byte) (i >>> 32);
        b[this.position++] = (byte) (i >>> 40);
        b[this.position++] = (byte) (i >>> 48);
        b[this.position++] = (byte) (i >>> 56);
    }

    // Write null-terminated string
    public final void writeString(String s) throws SQLException {
        ensureCapacity((s.length() * 3) + 1);
        writeStringNoNull(s);
        this.byteBuffer[this.position++] = 0;
    }


    // Write string, with no termination
    public final void writeStringNoNull(String s) throws SQLException {
        int len = s.length();
        ensureCapacity(len * 3);
        System.arraycopy(StringUtils.getBytes(s), 0, this.byteBuffer, this.position, len);
        this.position += len;

        // for (int i = 0; i < len; i++)
        // {
        // this.byteBuffer[this.position++] = (byte)s.charAt(i);
        // }
    }

    // Write a String using the specified character encoding
    public final void writeStringNoNull(String s, String encoding) throws UnsupportedEncodingException {
        byte[] b = StringUtils.getBytes(s, encoding);
        int len = b.length;
        ensureCapacity(len);
        System.arraycopy(b, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    public void writeHeader(byte[] bodyLen,byte seq) throws SQLException {
        if(bodyLen.length!=3){
            throw new IllegalArgumentException("body length must be 3!");
        }
        ensureCapacity(4);
        byteBuffer[0]=bodyLen[0];
        byteBuffer[2]=bodyLen[2];
        byteBuffer[3]=bodyLen[3];
        byteBuffer[4]=seq;
    }

    public void writeHeaderLength(int len) throws SQLException {
        ensureCapacity(3);
        byteBuffer[0] = (byte)len;
        byteBuffer[1] = (byte)(len >> 8);
        byteBuffer[2] = (byte)(len >> 16);
    }

    public void writeSeq(byte seq) throws SQLException {
        ensureCapacity(1);
        byteBuffer[3] = seq;
    }
    public void writeByte(int b) throws SQLException {
        ensureCapacity(1);
        byteBuffer[this.position++] = (byte)b;
    }

    public String readRestString() throws UnsupportedEncodingException {
        return readRestString(CharSet);
    }

    public String readRestString(String encoding) throws UnsupportedEncodingException {
        int bufLength = this.getBufLength();
        byte[] bs = Arrays.copyOfRange(this.byteBuffer, position,this.getBufLength() );
        try{
            return new String(bs, encoding);
        }catch(UnsupportedEncodingException e){
            return new String(bs);
        }
    }

    //private static final String CODE_PAGE_1252 = "Cp1252";
    static void int2Store(byte[] dest,short originVal){
        dest[0] = (byte)(originVal);
        dest[1] = (byte)(originVal >> 8);
    }
    static void int3Store(byte[] dest,int originVal){
        dest[0] = (byte)(originVal);
        dest[1] = (byte)(originVal >> 8 );
        dest[2] = (byte)(originVal >> 16);
    }

    static void int4Store(byte[] dest,int originVal){
        dest[0] = (byte)(originVal);
        dest[1] = (byte)(originVal >> 8 );
        dest[2] = (byte)(originVal >> 16);
        dest[3] = (byte)(originVal >> 24);
    }

    static int getInt3(byte[] dest){
        return (dest[0]&0xff)|((dest[1]&0xff)<<8)|((dest[2]&0xff)<<16);
    }

    static int getInt2(byte[] dest){
        return (dest[0]&0xff)|((dest[1]&0xff)<<8);
    }

    static int getInt4(byte[] dest){
        return (dest[0]&0xff)|((dest[1]&0xff)<<8)|((dest[2]&0xff)<<16)|((dest[3]&0xff)<<24);
    }

    static void int6Store(byte[] dest,long originVal){
        dest[0] = (byte)(originVal);
        dest[1] = (byte)(originVal >> 8 );
        dest[2] = (byte)(originVal >> 16);
        dest[3] = (byte)(originVal >> 24);
        dest[4] = (byte)(originVal >> 32);
        dest[5] = (byte)(originVal >> 40);
    }

    static void int8Store(byte[] dest,long originVal){
        dest[0] = (byte)(originVal);
        dest[1] = (byte)(originVal >> 8 );
        dest[2] = (byte)(originVal >> 16);
        dest[3] = (byte)(originVal >> 24);
        dest[4] = (byte)(originVal >> 32);
        dest[5] = (byte)(originVal >> 40);
        dest[6] = (byte)(originVal >> 48);
        dest[7] = (byte)(originVal >> 56);
    }

    // Right from Monty's code
    public static String newCrypt(String password, String seed, String encoding) {
        byte b;
        double d;

        if ((password == null) || (password.length() == 0)) {
            return password;
        }

        long[] pw = newHash(seed.getBytes());
        long[] msg = hashPre41Password(password, encoding);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];

        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }

        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);

        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }

        return new String(chars);
    }

    public static long[] hashPre41Password(String password, String encoding) {
        // remove white spaces and convert to bytes
        try {
            return newHash(password.replaceAll("\\s", "").getBytes(encoding));
        } catch (UnsupportedEncodingException e) {
            return new long[0];
        }
    }

    public static long[] hashPre41Password(String password) {
        return hashPre41Password(password, Charset.defaultCharset().name());
    }

    static long[] newHash(byte[] password) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;

        for (byte b : password) {
            tmp = 0xff & b;
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr2 += ((nr2 << 8) ^ nr);
            add += tmp;
        }

        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;

        return result;
    }




}
