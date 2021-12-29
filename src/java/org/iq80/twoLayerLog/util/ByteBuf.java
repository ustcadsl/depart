package org.iq80.twoLayerLog.util;

public interface ByteBuf {
	
	byte[] data();
	
	int offset();
	
	int endOffset();
	
	/**
	 * @return
	 */
	int size();
	
	int capacity();
	
	/**
	 * call resize(bytes, (byte)0x00);
	 * @param bytes
	 */
	void resize(int bytes);
	
	void resize(int bytes, byte initialValue);
	
	void require(int bytes);
	
	void swap(ByteBuf buf);
	
	/**
	 * return data[offset+idx]
	 * @param idx
	 * @return
	 */
	byte getByte(int idx);
	
	/**
	 * data[offset+idx] = b;
	 * @param idx
	 * @param b
	 */
	void setByte(int idx, byte b);
	
	ByteBuf clone();
	
	void clear();
	
	boolean empty();

	String encodeToString();
	
	String escapeString();
	
	long hashCode0();
	
	int compare(BFSlice slice);
	
	int compare(ByteBuf b);
	
	void assign(byte[] data, int size);
	
	void assign(byte[] data, int offset, int size);
	
	void assign(String s);
	
	void assign(ByteBuf buf);
	
	void assign(BFSlice slice);

	
	void append(byte[] buf, int size);
	
	void append(byte[] buf, int offset, int size);
	
	void append(String s);
	
	void append(ByteBuf buf);
	
	void append(BFSlice slice);
	
	void addByte(byte b);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void addFixedNat32(int value);
	
	/**
	 * append 32bit fixed natural number.
	 * @param value
	 */
	public void addFixedNat32Long(long value);
	
	/**
	 * append 64bit fixed natural number.
	 * @param value
	 */
	public void addFixedNat64(long value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 * @throws Exception
	 */
	public void addVarNat32(int value);
	
	/**
	 * append 32bit var natural number.
	 * @param value
	 */
	public void addVarNat64(long value);
	
	/**
	 * append slice.
	 * @param value
	 */
	public void addLengthPrefixedSlice(BFSlice value);
}
