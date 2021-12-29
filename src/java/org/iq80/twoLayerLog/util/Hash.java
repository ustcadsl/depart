package org.iq80.twoLayerLog.util;

public class Hash {
	final static long kUint32Mask = 0xFFFFFFFFL;

	/**
	 * Similar to murmur hash
	 * @param data
	 * @param offset
	 * @param n
	 * @param seed uint32
	 * @return
	 */
	public static long hash0(byte[] data, int offset, int n, long seed) {
		long m = 0xc6a4a793L;
		long r = 24L;
		int limit = offset + n;
		long h = ((seed ^ (n * m)) & kUint32Mask);
		
		// Pick up four bytes at a time
		while (offset + 4 <= limit) {
		    long w = (Coding.decodeFixedNat32Long(data, offset) & kUint32Mask);
		    offset += 4;
		    h += w; h &= kUint32Mask;
		    h *= m; h &= kUint32Mask;
		    h ^= (h >> 16); h &= kUint32Mask;
		}
		
		// Pick up remaining bytes
		switch (limit - offset) {
		case 3:
			h += ((data[offset+2] & 0x0ffL) << 16); h &= kUint32Mask;
		case 2:
			h += ((data[offset+1] & 0x0ffL) << 8); h &= kUint32Mask;
		case 1:
			h += (data[offset] & 0x0ffL);
			h *= m; h &= kUint32Mask;
			h ^= (h >> r); h &= kUint32Mask;
			break;
		}
		
		return h & kUint32Mask;
	}
}
