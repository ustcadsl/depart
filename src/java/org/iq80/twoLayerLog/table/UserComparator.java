package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.Slice;

import java.util.Comparator;

// todo this interface needs more thought
public interface UserComparator
        extends Comparator<Slice>
{
    String name();

    int compare(Slice sliceA, Slice sliceB);

    Slice findShortestSeparator(Slice start, Slice limit);

    Slice findShortSuccessor(Slice key);
}
