package org.iq80.twoLayerLog.util;

public class BFSliceFactory {
	
	public static BFSlice newUnpooled() {
		return new UnpooledSlice();
	}
	
	public static BFSlice newUnpooled(byte[] data, int offset, int size) {
		UnpooledSlice s = new UnpooledSlice();
		s.init(data, offset, size);
		return s;
	}
	
	public static BFSlice newUnpooled(String s) {
		return new UnpooledSlice(s);
	}
	
	public static BFSlice newUnpooled(BFSlice s) {
		UnpooledSlice ret = new UnpooledSlice();
		ret.init(s.data(), s.offset(), s.size());
		return ret;
	}
	
	public static BFSlice newUnpooled(ByteBuf b) {
		return newUnpooled(b.data(), b.offset(), b.size());
	}
}
