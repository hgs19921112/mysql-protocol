package com.luxin.mysql.protocol;


import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

import java.util.concurrent.ConcurrentHashMap;
public class StringUtils {
    private static final String platformEncoding = System.getProperty("file.encoding");
    private static final ConcurrentHashMap<String, Charset> charsetsByAlias = new ConcurrentHashMap<String, Charset>();

    public static String toString(byte[] value, int offset, int length) {
        try {
            Charset cs = findCharset(platformEncoding);

            return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    static Charset findCharset(String alias) throws UnsupportedEncodingException {
        try {
            Charset cs = charsetsByAlias.get(alias);

            if (cs == null) {
                cs = Charset.forName(alias);
                Charset oldCs = charsetsByAlias.putIfAbsent(alias, cs);
                if (oldCs != null) {
                    // if the previous value was recently set by another thread we return it instead of value we found here
                    cs = oldCs;
                }
            }

            return cs;

            // We re-throw these runtimes for compatibility with java.io
        } catch (UnsupportedCharsetException uce) {
            throw new UnsupportedEncodingException(alias);
        } catch (IllegalCharsetNameException icne) {
            throw new UnsupportedEncodingException(alias);
        } catch (IllegalArgumentException iae) {
            throw new UnsupportedEncodingException(alias);
        }
    }

    /**
     * Dumps the given bytes to STDOUT as a hex dump (up to length bytes).
     *
     * @param byteBuffer
     *            the data to print as hex
     * @param length
     *            the number of bytes to print
     *
     * @return ...
     */
    public static String dumpAsHex(byte[] byteBuffer, int length) {
        StringBuilder outputBuilder = new StringBuilder(length * 4);

        int p = 0;
        int rows = length / 8;

        for (int i = 0; (i < rows) && (p < length); i++) {
            int ptemp = p;

            for (int j = 0; j < 8; j++) {
                String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xff);

                if (hexVal.length() == 1) {
                    hexVal = "0" + hexVal;
                }

                outputBuilder.append(hexVal + " ");
                ptemp++;
            }

            outputBuilder.append("    ");

            for (int j = 0; j < 8; j++) {
                int b = 0xff & byteBuffer[p];

                if (b > 32 && b < 127) {
                    outputBuilder.append((char) b + " ");
                } else {
                    outputBuilder.append(". ");
                }

                p++;
            }

            outputBuilder.append("\n");
        }

        int n = 0;

        for (int i = p; i < length; i++) {
            String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal;
            }

            outputBuilder.append(hexVal + " ");
            n++;
        }

        for (int i = n; i < 8; i++) {
            outputBuilder.append("   ");
        }

        outputBuilder.append("    ");

        for (int i = p; i < length; i++) {
            int b = 0xff & byteBuffer[i];

            if (b > 32 && b < 127) {
                outputBuilder.append((char) b + " ");
            } else {
                outputBuilder.append(". ");
            }
        }

        outputBuilder.append("\n");

        return outputBuilder.toString();
    }


    public static byte[] getBytes(String value) {
        try {
            return getBytes(value, 0, value.length(), platformEncoding);
        } catch (UnsupportedEncodingException e) {
            // can't happen, emulating new String(byte[])
        }

        return null;
    }

    public static byte[] getBytes(String value, int offset, int length, String encoding) throws UnsupportedEncodingException {

        Charset cs = findCharset(encoding);

        ByteBuffer buf = cs.encode(CharBuffer.wrap(value.toCharArray(), offset, length));

        // can't simply .array() this to get the bytes especially with variable-length charsets the buffer is sometimes larger than the actual encoded data
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen];
        buf.get(asBytes, 0, encodedLen);

        return asBytes;
    }

    public static byte[] getBytes(String value, String encoding) throws UnsupportedEncodingException {
        return getBytes(value, 0, value.length(), encoding);
    }
}
