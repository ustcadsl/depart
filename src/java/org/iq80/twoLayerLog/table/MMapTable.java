package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.ByteBufferSupport;
import org.iq80.twoLayerLog.util.Closeables;
import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.Snappy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Comparator;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static org.iq80.twoLayerLog.CompressionType.SNAPPY;

public class MMapTable
        extends Table
{
    private MappedByteBuffer data;

    public MMapTable(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums, Slice indexBlockSlice, Slice footerSlice, int flagR)
            throws IOException
    {
        super(name, fileChannel, comparator, verifyChecksums, indexBlockSlice, footerSlice, flagR);
        checkArgument(fileChannel.size() <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long size = fileChannel.size();
        data = fileChannel.map(MapMode.READ_ONLY, 0, size);
        Slice footerSlice = Slices.copiedBuffer(data, (int) size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(footerSlice);
    }

    @Override
    public Callable<?> closer()
    {
        return new Closer(name, fileChannel, data);
    }

    private static class Closer
            implements Callable<Void>
    {
        private final String name;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        public Closer(String name, Closeable closeable, MappedByteBuffer data)
        {
            this.name = name;
            this.closeable = closeable;
            this.data = data;
        }

        public Void call()
        {
            ByteBufferSupport.unmap(data);
            Closeables.closeQuietly(closeable);
            return null;
        }
    }

    @SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "AssignmentToStaticFieldFromInstanceMethod"})
    @Override
    protected Block readBlock(BlockHandle blockHandle, int indexFlag)
            throws IOException
    {
        // decompress data
        Slice uncompressedData;
        ByteBuffer uncompressedBuffer = null;
        if(indexFlag==1){
            uncompressedBuffer = readIndex(this.data, (int) blockHandle.getOffset(), blockHandle.getDataSize());
        }else{
            uncompressedBuffer = read(this.data, (int) blockHandle.getOffset(), blockHandle.getDataSize());
        }
        uncompressedData = Slices.copiedBuffer(uncompressedBuffer);

        return new Block(uncompressedData, comparator);
    }

    public static ByteBuffer readIndex(MappedByteBuffer data, int offset, int length)
            throws IOException
    {
        int newPosition = data.position() + offset;
        ByteBuffer block = (ByteBuffer) data.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition);
        return block;
    }

    public static ByteBuffer read(MappedByteBuffer data, int offset, int length)
            throws IOException
    {
        int newPosition = data.position() + offset;
        ByteBuffer block = ByteBuffer.allocate(length);
        //ByteBuffer block = (ByteBuffer) data.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition);
        return block;
    }
}
