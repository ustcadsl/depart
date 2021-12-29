package org.iq80.twoLayerLog.impl;

import java.util.concurrent.atomic.AtomicInteger;
import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.Snappy;

public class FileMetaData
{
	private final String groupID;
	
    private final long number;

    public Slice footer;

    public Slice indexBlock;

    public int flagR;

    /**
     * File size in bytes
     */
    private final long fileSize;

    /**
     * Smallest internal key served by table
     */
    private final InternalKey smallest;

    /**
     * Largest internal key served by table
     */
    private final InternalKey largest;

    /**
     * Seeks allowed until compaction
     */
    // todo this mutable state should be moved elsewhere
    //private final AtomicInteger allowedSeeks = new AtomicInteger(1 << 30);

    public FileMetaData(String groupID, long number, Slice indexBlock, Slice footer, long fileSize, InternalKey smallest, InternalKey largest)
    {
    	this.groupID = groupID;
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
        this.indexBlock = indexBlock;////
        this.footer = footer;
        this.flagR = 0;
    }

    public long getFileSize()
    {
        return fileSize;
    }
    
    public String getGroupID()
    {
        return groupID;
    }

    public long getNumber()
    {
        return number;
    }

    public InternalKey getSmallest()
    {
        return smallest;
    }

    public InternalKey getLargest()
    {
        return largest;
    }
/*
    public int getAllowedSeeks()
    {
        return allowedSeeks.get();
    }

    public void setAllowedSeeks(int allowedSeeks)
    {
        this.allowedSeeks.set(allowedSeeks);
    }

    public void decrementAllowedSeeks()
    {
        allowedSeeks.getAndDecrement();
    }
*/
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(number);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        //sb.append(", allowedSeeks=").append(allowedSeeks);
        sb.append('}');
        return sb.toString();
    }
}
