package org.iq80.twoLayerLog.table;

import com.google.common.base.Throwables;
import org.iq80.twoLayerLog.impl.SeekingIterable;
import org.iq80.twoLayerLog.util.Closeables;
import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.TableIterator;
import org.iq80.twoLayerLog.util.VariableLengthQuantity;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.concurrent.Callable;
import org.apache.cassandra.service.StorageService;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class Table
        implements SeekingIterable<Slice, Slice>
{
    protected final String name;
    protected final FileChannel fileChannel;
    protected final Comparator<Slice> comparator;
    protected final boolean verifyChecksums;
    protected final Block indexBlock;
    protected final BlockHandle metaindexBlockHandle;
    public int flagR;

    public Table(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums, Slice indexBlockSlice, Slice footerSlice, int flagR)
            throws IOException
    {
        requireNonNull(name, "name is null");
        requireNonNull(fileChannel, "fileChannel is null");
        long size = fileChannel.size();
        checkArgument(size >= Footer.ENCODED_LENGTH, "File is corrupt: size must be at least %s bytes", Footer.ENCODED_LENGTH);
        requireNonNull(comparator, "comparator is null");

        this.name = name;
        this.fileChannel = fileChannel;
        this.verifyChecksums = verifyChecksums;
        this.comparator = comparator;
        
        this.flagR = flagR;
        Footer footer = init(); //////
        //indexBlock = readBlock(footer.getIndexBlockHandle()); //////
        //metaindexBlockHandle = footer.getMetaindexBlockHandle();

        //Footer footer = Footer.readFooter(footerSlice);
        //indexBlock = new Block(indexBlockSlice, comparator);
        indexBlock = readBlock(footer.getIndexBlockHandle(), 1); 
        metaindexBlockHandle = footer.getMetaindexBlockHandle();
    }

    protected abstract Footer init()
            throws IOException;

    @Override
    public TableIterator iterator()
    {
        return new TableIterator(this, indexBlock.iterator());
    }

    public Block openBlock(Slice blockEntry)
    {
        BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry.input());
        Block dataBlock;
        try {
            dataBlock = readBlock(blockHandle, 0);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return dataBlock;
    }

    protected static ByteBuffer uncompressedScratch = ByteBuffer.allocateDirect(4 * 1024 * 1024);

    protected abstract Block readBlock(BlockHandle blockHandle, int indexFlag)
            throws IOException;

    protected int uncompressedLength(ByteBuffer data)
            throws IOException
    {
        int length = VariableLengthQuantity.readVariableLengthInt(data.duplicate());
        return length;
    }

    /**
     * Given a key, return an approximate byte offset in the file where
     * the data for that key begins (or would begin if the key were
     * present in the file).  The returned value is in terms of file
     * bytes, and so includes effects like compression of the underlying data.
     * For example, the approximate offset of the last key in the table will
     * be close to the file length.
     */
    public long getApproximateOffsetOf(Slice key)
    {
        BlockIterator iterator = indexBlock.iterator();
        iterator.seek(key);
        if (iterator.hasNext()) {
            BlockHandle blockHandle = BlockHandle.readBlockHandle(iterator.next().getValue().input());
            return blockHandle.getOffset();
        }

        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        return metaindexBlockHandle.getOffset();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Table");
        sb.append("{name='").append(name).append('\'');
        sb.append(", comparator=").append(comparator);
        sb.append(", verifyChecksums=").append(verifyChecksums);
        sb.append('}');
        return sb.toString();
    }

    public Callable<?> closer()
    {
        return new Closer(fileChannel);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final Closeable closeable;

        public Closer(Closeable closeable)
        {
            this.closeable = closeable;
        }

        @Override
        public Void call()
        {
            Closeables.closeQuietly(closeable);
            return null;
        }
    }
}
