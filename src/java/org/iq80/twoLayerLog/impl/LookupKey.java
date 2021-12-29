package org.iq80.twoLayerLog.impl;

import org.iq80.twoLayerLog.util.Slice;

public class LookupKey
{
    private final InternalKey key;

    public LookupKey(Slice userKey, long sequenceNumber)
    {
        key = new InternalKey(userKey, sequenceNumber, ValueType.VALUE);
    }

    public InternalKey getInternalKey()
    {
        return key;
    }

    public Slice getUserKey()
    {
        return key.getUserKey();
    }

    @Override
    public String toString()
    {
        return key.toString();
    }
}
