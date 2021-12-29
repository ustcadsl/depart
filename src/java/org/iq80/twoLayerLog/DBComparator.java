package org.iq80.twoLayerLog;

import java.util.Comparator;

public interface DBComparator
        extends Comparator<byte[]>
{
    String name();

    /**
     * If {@code start < limit}, returns a short key in [start,limit).
     * Simple comparator implementations should return start unchanged,
     */
    byte[] findShortestSeparator(byte[] start, byte[] limit);

    /**
     * returns a 'short key' where the 'short key' is greater than or equal to key.
     * Simple comparator implementations should return key unchanged,
     */
    byte[] findShortSuccessor(byte[] key);
}
