package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.SliceInput;
import org.iq80.twoLayerLog.util.SliceOutput;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.VariableLengthQuantity;

public class BlockHandle
{
    public static final int MAX_ENCODED_LENGTH = 10 + 10;

    private final long offset;
    private final int dataSize;

    BlockHandle(long offset, int dataSize)
    {
        this.offset = offset;
        this.dataSize = dataSize;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getDataSize()
    {
        return dataSize;
    }

    public int getFullBlockSize()
    {
        return dataSize + BlockTrailer.ENCODED_LENGTH;
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

        BlockHandle that = (BlockHandle) o;

        if (dataSize != that.dataSize) {
            return false;
        }
        if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + dataSize;
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockHandle");
        sb.append("{offset=").append(offset);
        sb.append(", dataSize=").append(dataSize);
        sb.append('}');
        return sb.toString();
    }

    public static BlockHandle readBlockHandle(SliceInput sliceInput)
    {
        long offset = VariableLengthQuantity.readVariableLengthLong(sliceInput);
        long size = VariableLengthQuantity.readVariableLengthLong(sliceInput);

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Blocks can not be larger than Integer.MAX_VALUE");
        }

        return new BlockHandle(offset, (int) size);
    }

    public static Slice writeBlockHandle(BlockHandle blockHandle)
    {
        Slice slice = Slices.allocate(MAX_ENCODED_LENGTH);
        SliceOutput sliceOutput = slice.output();
        writeBlockHandleTo(blockHandle, sliceOutput);
        return slice.slice();
    }

    public static void writeBlockHandleTo(BlockHandle blockHandle, SliceOutput sliceOutput)
    {
        VariableLengthQuantity.writeVariableLengthLong(blockHandle.offset, sliceOutput);
        VariableLengthQuantity.writeVariableLengthLong(blockHandle.dataSize, sliceOutput);
    }
}
