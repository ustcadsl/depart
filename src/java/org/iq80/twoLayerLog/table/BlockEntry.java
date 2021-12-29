package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.Slice;

import java.util.Map.Entry;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class BlockEntry
        implements Entry<Slice, Slice>
{
    private final Slice key;
    private final Slice value;

    public BlockEntry(Slice key, Slice value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        this.key = key;
        this.value = value;
    }

    @Override
    public Slice getKey()
    {
        return key;
    }

    @Override
    public Slice getValue()
    {
        return value;
    }

    /**
     * @throws UnsupportedOperationException always
     */
    @Override
    public final Slice setValue(Slice value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockEntry entry = (BlockEntry) o;

        if (!key.equals(entry.key)) {
            return false;
        }
        if (!value.equals(entry.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockEntry");
        sb.append("{key=").append(key.toString(UTF_8));      // todo don't print the real value
        sb.append(", value=").append(value.toString(UTF_8));
        sb.append('}');
        return sb.toString();
    }
}
