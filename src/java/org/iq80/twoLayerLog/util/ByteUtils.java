package org.iq80.twoLayerLog.util;

public class ByteUtils {
	final public static int memcmp(byte[] a, int aoff, byte[] b, int boff, int size) {
		for (int i = 0; i < size; i++) {
			if (a[aoff+i] < b[boff+i])
				return -1;
			else if (a[aoff+i] > b[boff+i])
				return 1;
		}
		return 0;
	}
	
	final public static int  bytewiseCompare(byte[] a, int aoff, int asize, byte[] b, int boff, int bsize) {
		int minLen = Integer.min(asize, bsize);
		int r = memcmp(a, aoff, b, boff, minLen);
		if (r == 0) {
		    if (asize < bsize) r = -1;
		    else if (asize > bsize) r = +1;
		}
		return r;
	}
}
