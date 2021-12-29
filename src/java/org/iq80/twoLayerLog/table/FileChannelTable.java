package org.iq80.twoLayerLog.table;

import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.nio.*;
import java.util.LinkedList;
import java.util.Queue;

import static org.iq80.twoLayerLog.CompressionType.SNAPPY;

import org.apache.cassandra.service.StorageService;

public class FileChannelTable
        extends Table
{
    public FileChannelTable(String name, FileChannel fileChannel, Comparator<Slice> comparator, boolean verifyChecksums, Slice indexBlockSlice, Slice footerSlice, int flagR)
            throws IOException
    {
        super(name, fileChannel, comparator, verifyChecksums, indexBlockSlice, footerSlice, flagR);
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long size = fileChannel.size();
        long startTime = System.currentTimeMillis();
        //ByteBuffer footerData = read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        ByteBuffer footerData = readIndex(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        if(flagR==1) StorageService.instance.readIndexBlock += System.currentTimeMillis() - startTime; 
        if(flagR==1) StorageService.instance.ReadGroupBytes += footerData.limit();
        //return Footer.readFooter(Slices.copiedBuffer(footerData));
        return Footer.readFooter(new Slice(footerData.array()));
    }

    @SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod", "NonPrivateFieldAccessedInSynchronizedContext"})
    @Override
    protected Block readBlock(BlockHandle blockHandle, int indexFlag)
            throws IOException
    {
        // decompress data
        ByteBuffer uncompressedBuffer = null;
        Slice uncompressedData;
        //if(indexFlag==1 || flagR==1){
        if(flagR==1 || StorageService.instance.maxSegNumofGroup >= 10){
            long startTime = System.currentTimeMillis();
            uncompressedBuffer = readIndex(blockHandle.getOffset(), blockHandle.getDataSize());
            //uncompressedData = Slices.copiedBuffer(uncompressedBuffer);
            int length = uncompressedBuffer.limit() - uncompressedBuffer.position();
            ByteBuffer copyBuffer = uncompressedBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            uncompressedData = new Slice(copyBuffer.array(), 0, length);
            if(indexFlag==1) StorageService.instance.readIndexBlock += System.currentTimeMillis() - startTime;
            else StorageService.instance.readSSTables += System.currentTimeMillis() - startTime; 
        }else{
            if(indexFlag==1){
                uncompressedBuffer = readMeta(blockHandle.getOffset(), blockHandle.getDataSize());
            }else{
                uncompressedBuffer = read(blockHandle.getOffset(), blockHandle.getDataSize());
            }
            int length = uncompressedBuffer.limit() - uncompressedBuffer.position();
            ByteBuffer copyBuffer = uncompressedBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            //String info = "------in readBlock, length:"+ length + ", position:" + uncompressedBuffer.position() + ", limit:"+uncompressedBuffer.limit();
            //StorageService.instance.printInfo(info);
            uncompressedData = new Slice(copyBuffer.array(), 0, length);
        }
        
        return new Block(uncompressedData, comparator);
    }

    private ByteBuffer readIndex(long offset, int length)
            throws IOException
    {
        //long startTime = System.currentTimeMillis();
        ByteBuffer uncompressedBuffer = ByteBuffer.allocate(length);
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        if(flagR==1) StorageService.instance.ReadGroupBytes += length;
        uncompressedBuffer.clear();
        //if(flagR==1) StorageService.instance.readIndexBlock += System.currentTimeMillis() - startTime; 
        return uncompressedBuffer;
    }

    private ByteBuffer readMeta(long offset, int length)
            throws IOException
    {
        byte[] metaByte = null;
        StorageService.instance.printInfo("in readMeta");
        if(StorageService.instance.MetaByteQueue.size() < 20 ){ //<=1,30
            StorageService.instance.printInfo("Allocate new bytes, queue size:"+StorageService.instance.MetaByteQueue.size());
            metaByte = new byte[12*1024*1024];
        }else{
            metaByte = StorageService.instance.MetaByteQueue.poll();
        }
        ByteBuffer uncompressedBuffer = ByteBuffer.wrap(metaByte, 0, length);
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.rewind();
        String info = "in readMeta, length:"+ length + ", position:" + uncompressedBuffer.position() + ", limit:"+uncompressedBuffer.limit()+", dataByteQueue:"+StorageService.instance.MetaByteQueue.size();
        StorageService.instance.printInfo(info);
        try{
            StorageService.instance.MetaByteQueue.add(metaByte);
        }catch(Throwable e){
            StorageService.instance.printInfo("add MetaByteQueue failed!!");
        }
        return uncompressedBuffer;
    }

    private ByteBuffer read(long offset, int length)
            throws IOException
    {
        byte[] dataByte = null;
        if(StorageService.instance.dataByteQueue.size() < 100){ //<=1
            StorageService.instance.printInfo("Allocate new bytes, queue size:"+StorageService.instance.dataByteQueue.size());
            dataByte = new byte[32*1024];
        }else{
            //dataByte = StorageService.instance.dataByteQueue.poll();
            dataByte = StorageService.instance.dataByteQueue.remove();
            if(dataByte == null){
                StorageService.instance.printInfo("dataByteQueue return null!");
                dataByte = new byte[32*1024];
            }
        }
        ByteBuffer uncompressedBuffer = ByteBuffer.wrap(dataByte, 0, length);
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.rewind();
        //String info = "in read, length:"+ length + ", position:" + uncompressedBuffer.position() + ", limit:"+uncompressedBuffer.limit()+", dataByteQueue:"+StorageService.instance.dataByteQueue.size();
        //StorageService.instance.printInfo(info);
        try{
            StorageService.instance.dataByteQueue.add(dataByte);
        }catch(Throwable e){
            StorageService.instance.printInfo("add dataByteQueue failed!!");
        }
        return uncompressedBuffer;
    }

    /*private ByteBuffer read(long offset, int length)
            throws IOException
    {
        //long startTime = System.currentTimeMillis();
        ByteBuffer uncompressedBuffer = ByteBuffer.allocate(length);
        //if(flagR==1){
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.clear();
       return uncompressedBuffer;
    }*/
}
