package org.iq80.twoLayerLog.util;

public class Strings {

	final static int ASCII_SPACE = 0x20; // ' '
	final static int ASCII_TILDE = 0x7E; // '~'

	public static String escapeString(byte[] data, int offset, int size) {
		StringBuilder sb = new StringBuilder();
		for (int i = offset; i < size + offset; i++) {
			int c = (data[i] & 0xff);
			if (c >= ASCII_SPACE && c <= ASCII_TILDE) {
				sb.append((char) ('\0' + c));
			} else {
				sb.append(String.format("\\x%02x", c));
			}
		}
		return sb.toString();
	}

	public static String escapeString(ByteBuf buf) {
		return escapeString(buf.data(), buf.offset(), buf.size());
	}

	public static String escapeString(BFSlice slice) {
		return escapeString(slice.data(), slice.offset(), slice.size());
	}

}
