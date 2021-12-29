package org.iq80.twoLayerLog.util;

import com.google.common.collect.Maps;
import org.iq80.twoLayerLog.impl.InternalKey;

import java.util.Map.Entry;

public class InternalTableIterator
        extends AbstractSeekingIterator<InternalKey, Slice>
        implements InternalIterator
{
    private final TableIterator tableIterator;

    public InternalTableIterator(TableIterator tableIterator)
    {
        this.tableIterator = tableIterator;
    }

    @Override
    public void seekToFirstInternal()
    {
        tableIterator.seekToFirst();
    }

    @Override
    public void seekInternal(InternalKey targetKey)
    {
        tableIterator.seek(targetKey.encode());
    }

    @Override
    public Entry<InternalKey, Slice> getNextElement()
    {
        if (tableIterator.hasNext()) {
            Entry<Slice, Slice> next = tableIterator.next();
            return Maps.immutableEntry(new InternalKey(next.getKey()), next.getValue());
        }
        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InternalTableIterator");
        sb.append("{fromIterator=").append(tableIterator);
        sb.append('}');
        return sb.toString();
    }
}
