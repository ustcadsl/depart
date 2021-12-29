package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.SliceInput;
import org.iq80.twoLayerLog.util.SliceOutput;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.CompressionType;

import static java.util.Objects.requireNonNull;

public class BlockTrailer
{
    public static final int ENCODED_LENGTH = 5;

    private final CompressionType compressionType;
    private final int crc32c;

    public BlockTrailer(CompressionType compressionType, int crc32c)
    {
        requireNonNull(compressionType, "compressionType is null");

        this.compressionType = compressionType;
        this.crc32c = crc32c;
    }

    public CompressionType getCompressionType()
    {
        return compressionType;
    }

    public int getCrc32c()
    {
        return crc32c;
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

        BlockTrailer that = (BlockTrailer) o;

        if (crc32c != that.crc32c) {
            return false;
        }
        if (compressionType != that.compressionType) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = compressionType.hashCode();
        result = 31 * result + crc32c;
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockTrailer");
        sb.append("{compressionType=").append(compressionType);
        sb.append(", crc32c=0x").append(Integer.toHexString(crc32c));
        sb.append('}');
        return sb.toString();
    }

    public static BlockTrailer readBlockTrailer(Slice slice)
    {
        SliceInput sliceInput = slice.input();
        CompressionType compressionType = CompressionType.getCompressionTypeByPersistentId(sliceInput.readUnsignedByte());
        int crc32c = sliceInput.readInt();
        return new BlockTrailer(compressionType, crc32c);
    }

    public static Slice writeBlockTrailer(BlockTrailer blockTrailer)
    {
        Slice slice = Slices.allocate(ENCODED_LENGTH);
        writeBlockTrailer(blockTrailer, slice.output());
        return slice;
    }

    public static void writeBlockTrailer(BlockTrailer blockTrailer, SliceOutput sliceOutput)
    {
        sliceOutput.writeByte(blockTrailer.getCompressionType().persistentId());
        sliceOutput.writeInt(blockTrailer.getCrc32c());
    }
}
