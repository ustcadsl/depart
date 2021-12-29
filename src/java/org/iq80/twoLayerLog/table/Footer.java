package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.SliceInput;
import org.iq80.twoLayerLog.util.SliceOutput;
import org.iq80.twoLayerLog.util.Slices;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.iq80.twoLayerLog.table.BlockHandle.readBlockHandle;
import static org.iq80.twoLayerLog.table.BlockHandle.writeBlockHandleTo;
import static org.iq80.twoLayerLog.util.SizeOf.SIZE_OF_LONG;

public class Footer
{
    public static final int ENCODED_LENGTH = (BlockHandle.MAX_ENCODED_LENGTH * 2) + SIZE_OF_LONG;

    private final BlockHandle metaindexBlockHandle;
    private final BlockHandle indexBlockHandle;

    Footer(BlockHandle metaindexBlockHandle, BlockHandle indexBlockHandle)
    {
        this.metaindexBlockHandle = metaindexBlockHandle;
        this.indexBlockHandle = indexBlockHandle;
    }

    public BlockHandle getMetaindexBlockHandle()
    {
        return metaindexBlockHandle;
    }

    public BlockHandle getIndexBlockHandle()
    {
        return indexBlockHandle;
    }

    public static Footer readFooter(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        checkArgument(slice.length() == ENCODED_LENGTH, "Expected slice.size to be %s but was %s", ENCODED_LENGTH, slice.length());

        SliceInput sliceInput = slice.input();

        // read metaindex and index handles
        BlockHandle metaindexBlockHandle = readBlockHandle(sliceInput);
        BlockHandle indexBlockHandle = readBlockHandle(sliceInput);

        // skip padding
        sliceInput.setPosition(ENCODED_LENGTH - SIZE_OF_LONG);

        // verify magic number
        long magicNumber = sliceInput.readUnsignedInt() | (sliceInput.readUnsignedInt() << 32);
        checkArgument(magicNumber == TableBuilder.TABLE_MAGIC_NUMBER, "File is not a table (bad magic number)");

        return new Footer(metaindexBlockHandle, indexBlockHandle);
    }

    public static Slice writeFooter(Footer footer)
    {
        Slice slice = Slices.allocate(ENCODED_LENGTH);
        writeFooter(footer, slice.output());
        return slice;
    }

    public static void writeFooter(Footer footer, SliceOutput sliceOutput)
    {
        // remember the starting write index so we can calculate the padding
        int startingWriteIndex = sliceOutput.size();

        // write metaindex and index handles
        writeBlockHandleTo(footer.getMetaindexBlockHandle(), sliceOutput);
        writeBlockHandleTo(footer.getIndexBlockHandle(), sliceOutput);

        // write padding
        sliceOutput.writeZero(ENCODED_LENGTH - SIZE_OF_LONG - (sliceOutput.size() - startingWriteIndex));

        // write magic number as two (little endian) integers
        sliceOutput.writeInt((int) TableBuilder.TABLE_MAGIC_NUMBER);
        sliceOutput.writeInt((int) (TableBuilder.TABLE_MAGIC_NUMBER >>> 32));
    }
}
