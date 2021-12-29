package org.iq80.twoLayerLog.util;

public interface BFSlice {
	byte[] data();
	
	int offset();
	
	int limit();
	
	int size();
	
	int incrOffset(int inc);
	
	void setOffset(int offset);
	
	void init(BFSlice s);
	
	void init(ByteBuf b);
	
	void init(byte[] data, int offset, int size);
	
	byte getByte(int idx);
	
	boolean empty();
	
	void clear();
	
	String encodeToString();
	
	String escapeString();
	
	int compare(BFSlice s);
	
	int compare(ByteBuf b);
	
	void removePrefix(int n);
	
	long hashCode0();
	
	BFSlice clone();
	
	/**
	 * read 32bit fixed natural number.
	 * @return
	 */
	int readFixedNat32();
	
	/**
	 * read 64bit fixed natural number.
	 * @return
	 */
	long readFixedNat64();
	
	/**
	 * read 32bit var natural number.
	 * @return
	 */
	int readVarNat32();
	
	/**
	 * read 64bit var natural number.
	 * @return
	 */
	long readVarNat64();
	
	/**
	 * read slice.
	 * @param value
	 */
	BFSlice readLengthPrefixedSlice();
}
