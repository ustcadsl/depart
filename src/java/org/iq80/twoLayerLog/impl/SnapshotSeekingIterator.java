package org.iq80.twoLayerLog.impl;

import com.google.common.collect.Maps;
import org.iq80.twoLayerLog.util.AbstractSeekingIterator;
import org.iq80.twoLayerLog.util.DbIterator;
import org.iq80.twoLayerLog.util.Slice;

import java.util.Comparator;
import java.util.Map.Entry;

public final class SnapshotSeekingIterator
        extends AbstractSeekingIterator<Slice, Slice>
{
    private final DbIterator iterator;
    private final SnapshotImpl snapshot;
    private final Comparator<Slice> userComparator;

    public SnapshotSeekingIterator(DbIterator iterator, SnapshotImpl snapshot, Comparator<Slice> userComparator)
    {
        this.iterator = iterator;
        this.snapshot = snapshot;
        this.userComparator = userComparator;
        this.snapshot.getVersion().retain();
    }

    public void close()
    {
        this.snapshot.getVersion().release();
    }

    @Override
    protected void seekToFirstInternal()
    {
        iterator.seekToFirst();
        findNextUserEntry(null);
    }

    @Override
    protected void seekInternal(Slice targetKey)
    {
        iterator.seek(new InternalKey(targetKey, snapshot.getLastSequence(), ValueType.VALUE));
        findNextUserEntry(null);
    }

    @Override
    protected Entry<Slice, Slice> getNextElement()
    {
        if (!iterator.hasNext()) {
            return null;
        }

        Entry<InternalKey, Slice> next = iterator.next();

        // find the next user entry after the key we are about to return
        findNextUserEntry(next.getKey().getUserKey());

        return Maps.immutableEntry(next.getKey().getUserKey(), next.getValue());
    }

    private void findNextUserEntry(Slice deletedKey)
    {
        // if there are no more entries, we are done
        if (!iterator.hasNext()) {
            return;
        }

        do {
            // Peek the next entry and parse the key
            InternalKey internalKey = iterator.peek().getKey();

            // skip entries created after our snapshot
            if (internalKey.getSequenceNumber() > snapshot.getLastSequence()) {
                iterator.next();
                continue;
            }

            // if the next entry is a deletion, skip all subsequent entries for that key
            if (internalKey.getValueType() == ValueType.DELETION) {
                deletedKey = internalKey.getUserKey();
            }
            else if (internalKey.getValueType() == ValueType.VALUE) {
                // is this value masked by a prior deletion record?
                if (deletedKey == null || userComparator.compare(internalKey.getUserKey(), deletedKey) > 0) {
                    return;
                }
            }
            iterator.next();
        } while (iterator.hasNext());
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("SnapshotSeekingIterator");
        sb.append("{snapshot=").append(snapshot);
        sb.append(", iterator=").append(iterator);
        sb.append('}');
        return sb.toString();
    }
}
