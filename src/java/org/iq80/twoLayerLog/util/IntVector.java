package org.iq80.twoLayerLog.util;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class IntVector
{
    private int size;
    private int[] values;

    public IntVector(int initialCapacity)
    {
        this.values = new int[initialCapacity];
    }

    public int size()
    {
        return size;
    }

    public void clear()
    {
        size = 0;
    }

    public void add(int value)
    {
        checkArgument(size + 1 >= 0, "Invalid minLength: %s", size + 1);

        ensureCapacity(size + 1);

        values[size++] = value;
    }

    private void ensureCapacity(int minCapacity)
    {
        if (values.length >= minCapacity) {
            return;
        }

        int newLength = values.length;
        if (newLength == 0) {
            newLength = 1;
        }
        else {
            newLength <<= 1;

        }
        values = Arrays.copyOf(values, newLength);
    }

    public int[] values()
    {
        return Arrays.copyOf(values, size);
    }

    public void write(SliceOutput sliceOutput)
    {
        for (int index = 0; index < size; index++) {
            sliceOutput.writeInt(values[index]);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("IntVector");
        sb.append("{size=").append(size);
        sb.append(", values=").append(Arrays.toString(values));
        sb.append('}');
        return sb.toString();
    }
}
