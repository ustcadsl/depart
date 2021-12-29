package org.iq80.twoLayerLog.table;

import com.google.common.base.Throwables;
import org.iq80.twoLayerLog.CompressionType;
import org.iq80.twoLayerLog.Options;
import org.iq80.twoLayerLog.util.PureJavaCrc32C;
import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.iq80.twoLayerLog.impl.DbImpl.TARGET_FILE_SIZE;
import org.apache.cassandra.service.StorageService;
import java.util.Map;

public class TableBuilder
{
    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;

    private final int blockRestartInterval;
    private final int blockSize;
    private final CompressionType compressionType;

    private final FileChannel fileChannel;
    private final BlockBuilder dataBlockBuilder;
    private final BlockBuilder indexBlockBuilder;
    private Slice lastKey;
    private final UserComparator userComparator;

    private long entryCount;

    // Either Finish() or Abandon() has been called.
    private boolean closed;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    private boolean pendingIndexEntry;
    private BlockHandle pendingHandle;  // Handle to add to index block

    private Slice compressedOutput;

    private long position;

    public TableBuilder(Options options, FileChannel fileChannel, UserComparator userComparator)
    {
        requireNonNull(options, "options is null");
        requireNonNull(fileChannel, "fileChannel is null");
        try {
            checkState(position == fileChannel.position(), "Expected position %s to equal fileChannel.position %s", position, fileChannel.position());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        this.fileChannel = fileChannel;
        this.userComparator = userComparator;

        blockRestartInterval = options.blockRestartInterval();
        blockSize = options.blockSize();
        compressionType = options.compressionType();

        dataBlockBuilder = new BlockBuilder((int) Math.min(blockSize * 1.1, TARGET_FILE_SIZE), blockRestartInterval, userComparator);

        // with expected 50% compression
        int expectedNumberOfBlocks = 1024;
        //indexBlockBuilder = new BlockBuilder(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks, 1, userComparator);//20
        indexBlockBuilder = new BlockBuilder(8 * expectedNumberOfBlocks, 1, userComparator);

        lastKey = Slices.EMPTY_SLICE;
    }

    public long getEntryCount()
    {
        return entryCount;
    }

    public long getFileSize()
            throws IOException
    {
        return position + dataBlockBuilder.currentSizeEstimate();
    }

    public void add(BlockEntry blockEntry)
            throws IOException
    {
        requireNonNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(Slice key, Slice value)
            throws IOException
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        checkState(!closed, "table is finished");

        //String info0 = "before userComparator.compare, key:"+ new String(key.getBytes());
        //StorageService.instance.printInfo(info0);
        // If we just wrote a block, we can now add the handle to index block
        if (pendingIndexEntry) {
            checkState(dataBlockBuilder.isEmpty(), "Internal error: Table has a pending index entry but data block builder is empty");

            Slice shortestSeparator = userComparator.findShortestSeparator(lastKey, key);
            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(shortestSeparator, handleEncoding);
            pendingIndexEntry = false;
        }

        lastKey = key;
        entryCount++;
        dataBlockBuilder.add(key, value);

        int estimatedBlockSize = dataBlockBuilder.currentSizeEstimate();
        if (estimatedBlockSize >= blockSize) {
            flush();
        }
    }

    private void flush()
            throws IOException
    {
        checkState(!closed, "table is finished");
        if (dataBlockBuilder.isEmpty()) {
            return;
        }

        checkState(!pendingIndexEntry, "Internal error: Table already has a pending index entry to flush");

        pendingHandle = writeBlock(dataBlockBuilder);
        pendingIndexEntry = true;
    }

    private BlockHandle writeBlock(BlockBuilder blockBuilder)
            throws IOException
    {
        // close the block
        Slice raw = blockBuilder.finish();

        // attempt to compress the block
        Slice blockContents = raw;
        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, blockContents.length());

        // write data and trailer
        //position += fileChannel.write(new ByteBuffer[] {blockContents.toByteBuffer(), trailer.toByteBuffer()});
        position += fileChannel.write(blockContents.toByteBuffer());

        // clean up state
        blockBuilder.reset();

        return blockHandle;
    }

    private BlockHandle writeBlock(BlockBuilder blockBuilder, Map<Integer, Slice> tableMetaMap)
            throws IOException
    {
        // close the block
        Slice raw = blockBuilder.finish();

        // attempt to compress the block
        Slice blockContents = raw;
        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, blockContents.length());

        // write data and trailer
        //position += fileChannel.write(new ByteBuffer[] {blockContents.toByteBuffer(), trailer.toByteBuffer()});
        position += fileChannel.write(blockContents.toByteBuffer());

        // clean up state
        blockBuilder.reset();

        return blockHandle;
    }

    private static int maxCompressedLength(int length)
    {
        // Compressed data can be defined as:
        //    compressed := item* literal*
        //    item       := literal* copy
        //
        // The trailing literal sequence has a space blowup of at most 62/60
        // since a literal of length 60 needs one tag byte + one extra byte
        // for length information.
        //
        // Item blowup is trickier to measure.  Suppose the "copy" op copies
        // 4 bytes of data.  Because of a special check in the encoding code,
        // we produce a 4-byte copy only if the offset is < 65536.  Therefore
        // the copy op takes 3 bytes to encode, and this type of item leads
        // to at most the 62/60 blowup for representing literals.
        //
        // Suppose the "copy" op copies 5 bytes of data.  If the offset is big
        // enough, it will take 5 bytes to encode the copy op.  Therefore the
        // worst case here is a one-byte literal followed by a five-byte copy.
        // I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
        //
        // This last factor dominates the blowup, so the final estimate is:
        return 32 + length + (length / 6);
    }

    public void finish(Map<Integer, Slice> tableMetaMap)
            throws IOException
    {
        checkState(!closed, "table is finished");

        // flush current data block
        flush();

        // mark table as closed
        closed = true;

        // write (empty) meta index block
        BlockBuilder metaIndexBlockBuilder = new BlockBuilder(256, blockRestartInterval, new BytewiseComparator());
        // TODO(postrelease): Add stats and other meta blocks
        BlockHandle metaindexBlockHandle = writeBlock(metaIndexBlockBuilder);

        // add last handle to index block
        if (pendingIndexEntry) {
            //String info = "in finish, lastKey:{}"+ lastKey;
            //StorageService.instance.printInfo(info);
            Slice shortSuccessor = userComparator.findShortSuccessor(lastKey);

            Slice handleEncoding = BlockHandle.writeBlockHandle(pendingHandle);
            indexBlockBuilder.add(shortSuccessor, handleEncoding);
            pendingIndexEntry = false;
        }

        // write index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockBuilder, tableMetaMap); //////

        // write footer
        Footer footer = new Footer(metaindexBlockHandle, indexBlockHandle);
        Slice footerEncoding = Footer.writeFooter(footer);
    
        position += fileChannel.write(footerEncoding.toByteBuffer());
    }

    public void abandon()
    {
        checkState(!closed, "table is finished");
        closed = true;
    }

    public static int crc32c(Slice data, CompressionType type)
    {
        PureJavaCrc32C crc32c = new PureJavaCrc32C();
        crc32c.update(data.getRawArray(), data.getRawOffset(), data.length());
        crc32c.update(type.persistentId() & 0xFF);
        return crc32c.getMaskedValue();
    }

    public void ensureCompressedOutputCapacity(int capacity)
    {
        if (compressedOutput != null && compressedOutput.length() > capacity) {
            return;
        }
        compressedOutput = Slices.allocate(capacity);
    }
}
