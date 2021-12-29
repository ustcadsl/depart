package org.iq80.twoLayerLog.impl;

import org.iq80.twoLayerLog.table.UserComparator;
import org.iq80.twoLayerLog.util.InternalTableIterator;
import org.iq80.twoLayerLog.util.Level0Iterator;
import org.iq80.twoLayerLog.util.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.iq80.twoLayerLog.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.twoLayerLog.impl.ValueType.VALUE;
import org.apache.cassandra.service.StorageService;

// todo this class should be immutable
public class Level0
        implements SeekingIterable<InternalKey, Slice>
{
    private final TableCache tableCache;
    private final InternalKeyComparator internalKeyComparator;
    //private final List<FileMetaData> files;
    private List<FileMetaData> files;
    private List<Long> filesID;

    public static final Comparator<FileMetaData> NEWEST_FIRST = new Comparator<FileMetaData>()
    {
        @Override
        public int compare(FileMetaData fileMetaData, FileMetaData fileMetaData1)
        {
            return (int) (fileMetaData1.getNumber() - fileMetaData.getNumber());
        }
    };

    public Level0(TableCache tableCache, InternalKeyComparator internalKeyComparator, List<Long> filesID)
    {
        //requireNonNull(files, "files is null");
        requireNonNull(tableCache, "tableCache is null");
        requireNonNull(internalKeyComparator, "internalKeyComparator is null");
        this.filesID = filesID;
        //this.files = files;
        this.files = new ArrayList<FileMetaData>();
        this.tableCache = tableCache;
        this.internalKeyComparator = internalKeyComparator;
    }

    public int getLevelNumber()
    {
        return 0;
    }

    public List<FileMetaData> getFiles()
    {
        return files;
    }

    @Override
    public Level0Iterator iterator()
    {
        return new Level0Iterator(tableCache, files, internalKeyComparator);
    }

    public LookupResult get(LookupKey key, ReadStats readStats)
    {
        if (files.isEmpty()) {
            return null;
        }

        List<FileMetaData> fileMetaDataList = new ArrayList<>(files.size());
        for (FileMetaData fileMetaData : files) {
            //if (internalKeyComparator.getUserComparator().compare(key.getUserKey().copySlice(), fileMetaData.getSmallest().getUserKey().copySlice()) >= 0 &&
            //        internalKeyComparator.getUserComparator().compare(key.getUserKey().copySlice(), fileMetaData.getLargest().getUserKey().copySlice()) <= 0) {
            if (internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getSmallest().getUserKey()) >= 0 &&
                    internalKeyComparator.getUserComparator().compare(key.getUserKey(), fileMetaData.getLargest().getUserKey()) <= 0) {
                fileMetaDataList.add(fileMetaData);
            }
        }

        Collections.sort(fileMetaDataList, NEWEST_FIRST);

        //String str0 = "check fileID:"+fileMetaDataList+", look key:"+ key.getInternalKey();
        //StorageService.instance.printInfo(str0);
        for (FileMetaData fileMetaData : fileMetaDataList) {
            // open the iterator
            fileMetaData.flagR = 1;
            InternalTableIterator iterator = tableCache.newIterator(fileMetaData);
            // seek to the key
            iterator.seekInternal(key.getInternalKey());
            //iterator.seek(key.getInternalKey());
            Entry<InternalKey, Slice> entry1 = iterator.getNextElement();
            InternalKey internalKey1 = entry1.getKey();
            //StorageService.instance.printInfo("######entry1:"+entry1);
            StorageService.instance.ReadGroupBytes += internalKey1.getUserKey().length();
            StorageService.instance.ReadGroupBytes += entry1.getValue().length();
            if (entry1!=null && key.getUserKey().equals(internalKey1.getUserKey())) {
                if (internalKey1.getValueType() == ValueType.DELETION) {
                    return LookupResult.deleted(key);                  
                }
                else if (internalKey1.getValueType() == VALUE) {
                    return LookupResult.ok(key, entry1.getValue());
                }
            }
            fileMetaData.flagR = 0;
            
        }

        return null;
    }

    public boolean someFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey)
    {
        InternalKey smallestInternalKey = new InternalKey(smallestUserKey, MAX_SEQUENCE_NUMBER, VALUE);
        int index = findFile(smallestInternalKey);

        UserComparator userComparator = internalKeyComparator.getUserComparator();
        return ((index < files.size()) &&
                userComparator.compare(largestUserKey, files.get(index).getSmallest().getUserKey()) >= 0);
    }

    private int findFile(InternalKey targetKey)
    {
        if (files.isEmpty()) {
            return files.size();
        }

        // todo replace with Collections.binarySearch
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (internalKeyComparator.compare(files.get(mid).getLargest(), targetKey) < 0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            }
            else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    public void addFile(FileMetaData fileMetaData)
    {
        files.add(fileMetaData);
    }


    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Level0");
        sb.append("{files=").append(files);
        sb.append('}');
        return sb.toString();
    }
}
