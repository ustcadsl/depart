package org.iq80.twoLayerLog.util;

import java.nio.BufferOverflowException;

public class UnpooledByteBuf implements ByteBuf {

	byte[] data;
	int offset;
	int size;
	int capacity;

	public UnpooledByteBuf() {
		data = null;
		offset = 0;
		size = 0;
		capacity = 0;
	}
	
	@Override
	final public byte[] data() {
		return data;
	}
	
	@Override
	final public int offset() {
		return offset;
	}
	
	@Override
	final public int endOffset() {
		return offset + size;
	}
	
	@Override
	final public int size() {
		return size;
	}
	
	@Override
	final public int capacity() {
		return capacity;
	}
	
	@Override
	final public byte getByte(int idx) {
		if (idx < 0 || idx >= size)
			throw new BufferOverflowException();
		return data[offset + idx];
	}
	
	@Override
	final public void setByte(int idx, byte b) {
		if (idx < 0 || idx >= size)
			throw new BufferOverflowException();
		data[offset + idx] = b;
	}

	@Override
	public ByteBuf clone() {
		ByteBuf ret = new UnpooledByteBuf();
		ret.assign(data, offset, size());
		return ret;
	}
	
	@Override
	final public String encodeToString() {
		if (data == null || size() == 0)
			return "";
		
		return new String(data, offset, size());
	}
	
	@Override
	public String escapeString() {
		return Strings.escapeString(this);
	}

	@Override
	final public void clear() {
		size = 0;
	}
	
	@Override
	final public boolean empty() {
		return size == 0;
	}
	
	@Override
	public void swap(ByteBuf buf0) {
		UnpooledByteBuf b = (UnpooledByteBuf)buf0;
		
		byte[] data0 = data;
		int size0 = size;
		int offset0 = offset;
		int capacity0 = capacity;
		
		data = b.data;
		size = b.size;
		offset = b.offset;
		capacity = b.capacity;
		
		b.data = data0;
		b.size = size0;
		b.offset = offset0;
		b.capacity = capacity0;
	}
	
	@Override
	public void resize(int n) {
		resize(n, (byte)0);
	}
	
	@Override
	public void resize(int n, byte value) {
		if (n < 0)
			return;
		
		if (n < size()) {
			size = n;
			return;
		}
		
		int oldSize = size();
		int newcapacity = calculateCapacity(n);
		if (newcapacity > capacity()) {
			byte[] newData = new byte[newcapacity];
			if (size() > 0)
				System.arraycopy(data, offset, newData, 0, size());
			data = newData;
			offset = 0;
			capacity = newcapacity;
		}
		
		for (int i = oldSize; i < n; i++)
			data[i] = value;
		size = n;
	}
	
	final int calculateCapacity(int size) {
		int newcapacity = 1;
		while (newcapacity < size)
			newcapacity = newcapacity << 1;
		if (newcapacity < 16)
			newcapacity = 16;
		return newcapacity;
	}

	@Override
	final public void require(int bytes) {
		if (bytes <= 0)
			return;
		
		if (capacity - size < bytes) {
			int total  = size + bytes;
			int newcapacity = calculateCapacity(total);
			byte[] newData = new byte[newcapacity];
			if (size() > 0)
				System.arraycopy(data, offset, newData, 0, size());
			data = newData;
			offset = 0;
			capacity = newcapacity;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		UnpooledByteBuf b= (UnpooledByteBuf)o;
		
		if (size() == 0 && size() == b.size())
			return true;
		
		if (data != null && b.data != null && size() == b.size()) {
			int size = size();
			byte[] bdata = b.data;
			for (int i = 0; i < size; i++) {
				if (data[offset+i] != bdata[b.offset+i])
					return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public void append(byte[] buf, int n) {
		append(buf, 0 ,n);
	}
	
	@Override
	public void append(byte[] buf, int off0, int n) {
		if (n <= 0)
			return;
		
		require(n);
		System.arraycopy(buf, off0, data, endOffset(), n);
		size += n;
	}
	
	@Override
	public void append(String s) {
		byte[] b = s.getBytes();
		append(b, 0, b.length);
	}
	
	@Override
	public void append(ByteBuf buf) {
		append(buf.data(), buf.offset(), buf.size());
	}
	
	@Override
	public void append(BFSlice slice) {
		append(slice.data(), slice.offset(), slice.size());
	}
	
	@Override
	final public void addByte(byte b) {
		require(1);
		data[size++] = b;
	}
	
	public void assign(byte[] data, int n) {
		clear();
		append(data, n);
	}
	
	public void assign(byte[] data, int off, int n) {
		clear();
		append(data, off, n);
	}
	
	@Override
	public void assign(String s) {
		clear();
		append(s);
	}
	
	@Override
	public void assign(ByteBuf buf) {
		clear();
		append(buf);
	}
	
	@Override
	public void assign(BFSlice slice) {
		clear();
		append(slice);
	}

	@Override
	final public void addFixedNat32(int value) {
		require(4);
		int limit = offset + size;
		Coding.encodeFixedNat32(data, limit, value);
		size += 4;
	}
	
	@Override
	final public void addFixedNat32Long(long value) {
		require(4);
		int limit = offset + size;
		Coding.encodeFixedNat32Long(data, limit, limit+4, value);
		size += 4;
	}

	@Override
	final public void addFixedNat64(long value) {
		require(8);
		int limit = offset + size;
		Coding.encodeFixedNat64(data, limit, limit+8, value);
		size += 8;
	}

	@Override
	final public void addVarNat32(int value) {
		int vlen = Coding.varNatLength(value);
		require(vlen);
		int limit = offset + size;
		Coding.encodeVarNat32(data, limit, limit+vlen, value);
		size += vlen;
	}

	@Override
	final public void addVarNat64(long value) {
		int vlen = Coding.varNatLength(value);
		require(vlen);
		int limit = offset + size;
		Coding.encodeVarNat64(data, limit, limit+vlen, value);
		size += vlen;
	}

	@Override
	final public void addLengthPrefixedSlice(BFSlice value) {
		addVarNat32(value.size());
		append(value.data(), value.offset(), value.size());
	}
	

	@Override
	public long hashCode0() {
		return Hash.hash0(data, offset, size(), 301);
	}
	
	@Override
	public int compare(BFSlice b) {
		return ByteUtils.bytewiseCompare(data, offset, size(), b.data(), b.offset(), b.size());
	}
	
	@Override
	public int compare(ByteBuf b) {
		return ByteUtils.bytewiseCompare(data, offset, size(), b.data(), b.offset(), b.size());
	}
}
