package org.iq80.twoLayerLog.util;

import org.iq80.twoLayerLog.impl.InternalKey;
import org.iq80.twoLayerLog.impl.SeekingIterator;

public interface InternalIterator
        extends SeekingIterator<InternalKey, Slice>
{
}
