package org.iq80.twoLayerLog.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.iq80.twoLayerLog.table.FileChannelTable;
import org.iq80.twoLayerLog.table.MMapTable;
import org.iq80.twoLayerLog.table.Table;
import org.iq80.twoLayerLog.table.UserComparator;
import org.iq80.twoLayerLog.util.Closeables;
import org.iq80.twoLayerLog.util.Finalizer;
import org.iq80.twoLayerLog.util.InternalTableIterator;
import org.iq80.twoLayerLog.util.Slice;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.service.StorageService;

import static java.util.Objects.requireNonNull;

public class TableCache
{
    private final LoadingCache<FileMetaData, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);

    public TableCache(final File databaseDir, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums)
    {
        requireNonNull(databaseDir, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<FileMetaData, TableAndFile>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<FileMetaData, TableAndFile> notification)
                    {
                        Table table = notification.getValue().getTable();
                        finalizer.addCleanup(table, table.closer());
                    }
                })
                .build(new CacheLoader<FileMetaData, TableAndFile>()
                {
                    @Override
                    public TableAndFile load(FileMetaData fileMetaData)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileMetaData.getNumber(), userComparator, verifyChecksums, fileMetaData);
                    }
                });
    }

    public InternalTableIterator newIterator(FileMetaData file)
    {
        //return newIterator(file.getNumber());
        return newIterator(file.getNumber(), file);
    }

    public InternalTableIterator newIterator(long number, FileMetaData fileMetaData)
    {
        return new InternalTableIterator(getTable(fileMetaData).iterator());
    }

    public long getApproximateOffsetOf(FileMetaData file, Slice key)
    {
        return getTable(file).getApproximateOffsetOf(key);
    }

    private Table getTable(FileMetaData fileMetaData)
    {
        Table table;
        try {
            //StorageService.instance.printInfo("in getTable, number:"+number);
            table = cache.get(fileMetaData).getTable();
            //StorageService.instance.printInfo("in getTable, table:"+table);

        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + fileMetaData.getNumber(), cause);
        }
        return table;
    }

    public void close()
    {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void closeTableFile(long number)
    {
    }
    
    public void evict(long number)
    {
        cache.invalidate(number);
    }

    private final class TableAndFile
    {
        private final Table table;
        
        private TableAndFile(File databaseDir, long fileNumber, UserComparator userComparator, boolean verifyChecksums, FileMetaData fileMetaData)
                throws IOException
        {
            String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            FileInputStream fis = null;
            //FileChannel fileChannel = null;
            try {
            	fis = new FileInputStream(tableFile);
                FileChannel fileChannel = fis.getChannel();
                //fileChannel = fis.getChannel();
                if (Iq80DBFactory.USE_MMAP) {
                    table = new MMapTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums, fileMetaData.indexBlock, fileMetaData.footer, fileMetaData.flagR);
                    // We can close the channel and input stream as the mapping does not need them
                    Closeables.closeQuietly(fis);
                    //Closeables.closeQuietly(fileChannel);
                }
                else {
                    table = new FileChannelTable(tableFile.getAbsolutePath(), fileChannel, userComparator, verifyChecksums, fileMetaData.indexBlock, fileMetaData.footer, fileMetaData.flagR);
                    //Closeables.closeQuietly(fis);
                    //Closeables.closeQuietly(fileChannel);
                }
                //fis.close(); 
            	//fileChannel.close();
            }
            catch (IOException ioe) {
              Closeables.closeQuietly(fis);
              System.out.println("inputStream open IOException:" + ioe.getMessage());  
              throw ioe;
            }
        }
        
        public Table getTable()
        {
            return table;
        }
    }
}
