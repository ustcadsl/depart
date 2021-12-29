package org.iq80.twoLayerLog.impl;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.iq80.twoLayerLog.CompressionType;
import org.iq80.twoLayerLog.DB;
import org.iq80.twoLayerLog.DBComparator;
import org.iq80.twoLayerLog.DBException;
import org.iq80.twoLayerLog.Options;
import org.iq80.twoLayerLog.Range;
import org.iq80.twoLayerLog.ReadOptions;
import org.iq80.twoLayerLog.Snapshot;
import org.iq80.twoLayerLog.WriteBatch;
import org.iq80.twoLayerLog.WriteOptions;
import org.iq80.twoLayerLog.impl.Filename.FileInfo;
import org.iq80.twoLayerLog.impl.Filename.FileType;
import org.iq80.twoLayerLog.impl.MemTable.MemTableIterator;
import org.iq80.twoLayerLog.impl.WriteBatchImpl.Handler;
import org.iq80.twoLayerLog.table.BytewiseComparator;
import org.iq80.twoLayerLog.table.CustomUserComparator;
import org.iq80.twoLayerLog.table.TableBuilder;
import org.iq80.twoLayerLog.table.UserComparator;
import org.iq80.twoLayerLog.util.DbIterator;
import org.iq80.twoLayerLog.util.MergingIterator;
import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.SliceInput;
import org.iq80.twoLayerLog.util.SliceOutput;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.Snappy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.io.util.DataInputBuffer;
import java.net.InetAddress;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.db.*;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

import org.apache.cassandra.dht.Token;
import java.nio.ByteBuffer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.RTBoundValidator.Stage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;;
import static org.iq80.twoLayerLog.impl.DbConstants.NUM_LEVELS;
import static org.iq80.twoLayerLog.impl.SequenceNumber.MAX_SEQUENCE_NUMBER;
import static org.iq80.twoLayerLog.impl.ValueType.DELETION;
import static org.iq80.twoLayerLog.impl.ValueType.VALUE;
import static org.iq80.twoLayerLog.util.SizeOf.SIZE_OF_INT;
import static org.iq80.twoLayerLog.util.SizeOf.SIZE_OF_LONG;
import static org.iq80.twoLayerLog.util.Slices.readLengthPrefixedBytes;
import static org.iq80.twoLayerLog.util.Slices.writeLengthPrefixedBytes;
import org.iq80.twoLayerLog.impl.WriteBatchImpl;

import java.util.concurrent.ScheduledExecutorService;
import org.iq80.twoLayerLog.util.InternalTableIterator;
import org.iq80.twoLayerLog.util.Closeables;

// todo make thread safe and concurrent
@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public class DbImpl
        implements DB
{
    private final Options options;
    private final File databaseDir;
    private final DbLock dbLock;
    
    private Map<String, TableCache> groupTableCacheMap = new HashMap<String,TableCache>(); 
    public Map<String, VersionSet> groupVersionSetMap = new HashMap<String,VersionSet>();

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    public final ReentrantLock mutex = new ReentrantLock();
    private final Condition backgroundCondition = mutex.newCondition();

    private final List<Long> pendingOutputs = new ArrayList<>(); // todo

    private LogWriter log;

    private MemTable memTable;
    private MemTable immutableMemTable;

    private final InternalKeyComparator internalKeyComparator;

    private volatile Throwable backgroundException;
    private final ExecutorService compactionExecutor;
    private final ExecutorService groupGCExecutor;
    private final ExecutorService flushExecutor;
    private Future<?> backgroundCompaction;
    private Future<?> backgroundSplit;
    private Future<?> backgroundGroupGC;
    private Future<?> backgroundFlush;
    private int backgroundSplitFlag = 0;
    private int duringBackgroundGC = 0; 
    private CompactionState splitCompactionState = null;
    //////////////////////////////////////////////////////////////////////////
    public static String globalLogName = "globalLog"; 
    public static final int TARGET_FILE_SIZE = 2000 * 1048576; //2MB 50
    private static int writeBufferSize = 120 << 20; //2MB 50
    private static int GCBuilderSSTableSize = 6000 * 1048576;
    private static int splitDelay = 60;//s 20
    private static int gcCheckDelay =10;// 5;//30
    //private static int maxSegNumofGroup = 8;//2
    private static int splitSegNum = 8; //10
    public static int SplitTriggeredLogNumber = 8;//10
    public static long groupGCTriggerSize = 10485760; //10485760; //10GB
    ////////////////////////////////////////////////////////////////////////
    private Map<String,TableBuilder> builderMap = new HashMap<String,TableBuilder>();
    private Map<String,FileChannel> outfileMap = new HashMap<String,FileChannel>();
    private Map<String,FileOutputStream> outStreamMap = new HashMap<String,FileOutputStream>();
    //private Map<String,TableBuilder> GCBuilderMap = new HashMap<String,TableBuilder>();

    public String dbKeySpaceName;

    private ManualCompaction manualCompaction;
    
    ArrayBlockingQueue<Long> splitedLogQueue = new  ArrayBlockingQueue<Long>(200);
    public List<String> doneGCLogDirList = new ArrayList<String>();
	
	public Map<String,WriteBatchImpl> writeBatchMap = new HashMap<String,WriteBatchImpl>();
	
    public Map<String,LogWriter> replicasLogMap = new HashMap<String,LogWriter>();
    
    public Map<Integer,File> replicasDirMap = new HashMap<Integer,File>();
    public Map<Integer,ExecutorService> replicasThreadMap = new HashMap<Integer,ExecutorService>();
    public Map<String,Integer> grouptoIDMap=new HashMap<String,Integer>();
	public Map<Integer,List<String>> rangeUpperBoundMap=new HashMap<Integer,List<String>>();
	
	public Map<String,Integer> currentLogSizeMap=new HashMap<String,Integer>();
	public Map<String,Integer> continueWriteGroupMap=new HashMap<String,Integer>();
  	public Map<String,CountDownLatch> groupCountDownMap = new HashMap<String,CountDownLatch>();
    public Map<Integer, Slice> tableMetaMap = new HashMap<Integer, Slice>();
	
	public int totalLogDirNumber;
    
    private final ScheduledExecutorService splitExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService gcExecutor = Executors.newSingleThreadScheduledExecutor();

    public DbImpl(Options options, File databaseDir)
            throws IOException
    {
        requireNonNull(options, "options is null");
        requireNonNull(databaseDir, "databaseDir is null");
        this.options = options;

        if (this.options.compressionType() == CompressionType.SNAPPY && !Snappy.available()) {
            // Disable snappy if it's not available.
            String str0 = "######CompressionType.NONE";
            StorageService.instance.printInfo(str0);
            this.options.compressionType(CompressionType.NONE);
        }

        this.databaseDir = databaseDir;

        //use custom comparator if set
        DBComparator comparator = options.comparator();
        UserComparator userComparator;
        if (comparator != null) {
            userComparator = new CustomUserComparator(comparator);/////////////////////////////////////
        }
        else {
            userComparator = new BytewiseComparator();
        }
        internalKeyComparator = new InternalKeyComparator(userComparator);
        memTable = new MemTable(internalKeyComparator);
        immutableMemTable = null;

        ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("twoLayerLog-compaction-%s")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {
                        // todo need a real UncaughtExceptionHandler
                        System.out.printf("%s%n", t);
                        e.printStackTrace();
                    }
                })
                .build();
        compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory);
        
        ThreadFactory groupGCThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("group-GC-%s")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {
                        // todo need a real UncaughtExceptionHandler
                        System.out.printf("%s%n", t);
                        e.printStackTrace();
                    }
                })
                .build();
        groupGCExecutor = Executors.newSingleThreadExecutor(groupGCThreadFactory);
        
        ThreadFactory flushThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("flush-batch-%s")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {
                        // todo need a real UncaughtExceptionHandler
                        System.out.printf("%s%n", t);
                        e.printStackTrace();
                    }
                })
                .build();
        flushExecutor = Executors.newSingleThreadExecutor(flushThreadFactory);

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = options.maxOpenFiles() - 10;
        TableCache tableCache = new TableCache(databaseDir, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());

        groupTableCacheMap.put(globalLogName, tableCache);
        // create the version set

        // create the database dir if it does not already exist
        databaseDir.mkdirs();
        checkArgument(databaseDir.exists(), "Database directory '%s' does not exist and could not be created", databaseDir);
        checkArgument(databaseDir.isDirectory(), "Database directory '%s' is not a directory", databaseDir);

        mutex.lock();
        try {
            // lock the database dir
            dbLock = new DbLock(new File(databaseDir, Filename.lockFileName()));

            // verify the "current" file
            File currentFile = new File(databaseDir, Filename.currentFileName());
            if (!currentFile.canRead()) {
                checkArgument(options.createIfMissing(), "Database '%s' does not exist and the create if missing option is disabled", databaseDir);
            }
            else {
                checkArgument(!options.errorIfExists(), "Database '%s' exists and the error if exists option is enabled", databaseDir);
            }
            //List<FileMetaData> files = new ArrayList<FileMetaData>();
            List<Long> filesID = new ArrayList<Long>();
            VersionSet versions = new VersionSet(databaseDir, tableCache, internalKeyComparator, filesID);

            groupVersionSetMap.put(globalLogName, versions);
            // load  (and recover) current version
            versions.recover();

            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of twoLayerLog.
            long minLogNumber = versions.getLogNumber();
            long previousLogNumber = versions.getPrevLogNumber();
            List<File> filenames = Filename.listFiles(databaseDir);

            List<Long> logs = new ArrayList<>();
            for (File filename : filenames) {
                FileInfo fileInfo = Filename.parseFileName(filename);

                if (fileInfo != null &&
                        fileInfo.getFileType() == FileType.LOG &&
                        ((fileInfo.getFileNumber() >= minLogNumber) || (fileInfo.getFileNumber() == previousLogNumber))) {
                    logs.add(fileInfo.getFileNumber());
                }
            }

            // Recover in the order in which the logs were generated
            VersionEdit edit = new VersionEdit();
            Collections.sort(logs);
            for (Long fileNumber : logs) {
                long maxSequence = recoverLogFile(fileNumber, edit);
                if (versions.getLastSequence() < maxSequence) {
                    versions.setLastSequence(maxSequence);
                }
            }

            // open transaction log
            long logFileNumber = versions.getNextFileNumber();
            this.log = Logs.createLogWriter(new File(databaseDir, Filename.logFileName(logFileNumber)), logFileNumber);
            edit.setLogNumber(log.getFileNumber());

            // apply recovered edits
            versions.logAndApply(edit, 1);
            // cleanup unused files
            deleteObsoleteFiles(globalLogName, databaseDir);
            // schedule compactions
            //maybeScheduleCompaction();
        }
        finally {
            mutex.unlock();
        }
        
        /*
        gcExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                	//maybeScheduleBackgroundSplit();
                	//mutex.lock();
                    //if(!StorageService.instance.doingGroupMerge && !StorageService.instance.doingGlobalSplit){
                    if(!StorageService.instance.doingGroupMerge){
                	    performGroupMerge();
                    }
                	//mutex.unlock();
                } catch (Exception e) {
                	System.out.println("schule split task error " + e.getMessage());
                }
            }
        }, 5, 60, TimeUnit.SECONDS);//30
        */
    }

    public void performGroupMerge(){
        mutex.lock();
        StorageService.instance.printInfo("---in performGroupMerge");
        StorageService.instance.doingGroupMerge = true;
        String groupID = null;
        for (Map.Entry<String,VersionSet> batchEntry: groupVersionSetMap.entrySet()) {        	
            VersionSet curVersionSet = batchEntry.getValue();
            //StorageService.instance.printInfo("---groupID:"+batchEntry.getKey()+", numberOfFilesInLevel:" + curVersionSet.numberOfFilesInLevel(0));	
            if(curVersionSet.numberOfFilesInLevel(0) >= StorageService.instance.maxSegNumofGroup) { // && StorageService.instance.groupAccessNumMap.get(groupID)>3                		
                //mutex.lock();//////
                groupID = batchEntry.getKey();		
                StorageService.instance.printInfo("performGroupMerge in groupID:"+groupID+", numberOfFilesInLevel:" + curVersionSet.numberOfFilesInLevel(0));
                int nodeID = grouptoIDMap.get(groupID);
                try{
                    backgroundGroupGC(nodeID, groupID);
                    StorageService.instance.groupAccessNumMap.put(groupID, 0);
                } catch (Exception e) {
                	StorageService.instance.printInfo("schule backgroundGroupGC error " + e.getMessage());
                }
                //mutex.unlock();//////
            }
        }
        StorageService.instance.doingGroupMerge = false;
        mutex.unlock();
    }

    @Override
    public void close()
    {
        if (shuttingDown.getAndSet(true)) {
            return;
        }

        mutex.lock();
        try {
            while (backgroundCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            while (backgroundGroupGC != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            while (backgroundFlush != null) {
                backgroundCondition.awaitUninterruptibly();
            }

        }
        finally {
            mutex.unlock();
        }
        splitExecutor.shutdown();
        gcExecutor.shutdown();
        compactionExecutor.shutdown();
        groupGCExecutor.shutdown();
        flushExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
            splitExecutor.awaitTermination(1, TimeUnit.DAYS);
            gcExecutor.awaitTermination(1, TimeUnit.DAYS);
            groupGCExecutor.awaitTermination(1, TimeUnit.DAYS);
             flushExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
        	for(Entry<String, VersionSet> versionEntry: groupVersionSetMap.entrySet()) {
        		versionEntry.getValue().destroy();
        	}
            //versions.destroy();
        }
        catch (IOException ignored) {
        }
        try {
            log.close();
        }
        catch (IOException ignored) {
        }
        
        for(Entry<String, TableCache> tableCacheEntry: groupTableCacheMap.entrySet()) {
        	tableCacheEntry.getValue().close();
    	}
        //tableCache.close();
        dbLock.release();
    }
    
    public void createReplicaDir(int noeID, List<String> strTokensList, String keySpaceName) throws IOException {
    	String replicaDBName = databaseDir.getAbsoluteFile() + File.separator + "replica" + String.valueOf(noeID);           
        File replicaFile = new File(replicaDBName); 
        dbKeySpaceName = keySpaceName;
        if(!replicaFile.exists()){
        	replicaFile.mkdirs();         	
        	List<String> tokensList = new ArrayList<String>();        	 
        	for(String curToken: strTokensList) {   
        		//String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + String.valueOf(rangeUpperBound[i]);           
        		String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + curToken;                   	    
        		File rangeGroupFile = new File(rangeGroupName); 
        	    if(!rangeGroupFile.exists()) rangeGroupFile.mkdirs();       	        	
        		
        		WriteBatchImpl newWriteBatch = new WriteBatchImpl();
        		writeBatchMap.put(curToken, newWriteBatch);
            	tokensList.add(curToken);
            	totalLogDirNumber++;
            	grouptoIDMap.put(curToken, noeID);
            	continueWriteGroupMap.put(curToken, 0);
            } 
        	//groupDirMapTest.put(noeID, strTokensList.get(0));////
        	rangeUpperBoundMap.put(noeID, tokensList);
        	replicasDirMap.put(noeID, replicaFile);  
        	ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        	replicasThreadMap.put(noeID, singleThreadExecutor);
        }
    }
    
    public int getRangeGroupRowNumber(int NodeID, String groupID) {
    	VersionSet versions = groupVersionSetMap.get(groupID);
    	int level = 0;
    	int totoalPairsNumber = 0;
        if(versions == null) {
    		return 0;
    	}
    	List<FileMetaData> groupFileMeta = versions.getCurrent().getFiles(level);
    	if(groupFileMeta.size() == 0) {
    		return 0;
    	}else {
    		for(FileMetaData curMeta: groupFileMeta) {
    			int currentLogPairs = (int)curMeta.getFileSize()/1024 + 1;
        		totoalPairsNumber+=currentLogPairs;
    		}
    		return totoalPairsNumber;
    	}
    }
    
    public long getGlobalLogSize(long segmentID) {  
    	VersionSet globalVersions = groupVersionSetMap.get(globalLogName);
    	int level = 0;  	
    	List<FileMetaData> globalFileMeta = globalVersions.getCurrent().getFiles(level);
    	for(FileMetaData curMeta: globalFileMeta) {
    		if(curMeta.getNumber()==segmentID) {
    			return curMeta.getFileSize();
    		}			
		}
    	return 0;    
    }
    
    public int getRangeGroupSegemntNumber(int NodeID, String groupID) {   
    	VersionSet versions = groupVersionSetMap.get(groupID);
    	int level = 0;   	
        if(versions == null) {
    		return 0;
    	}
    	List<FileMetaData> groupFileMeta = versions.getCurrent().getFiles(level);
    	if(groupFileMeta.size() == 0) {
    		return 0;
    	}else {		
    		return groupFileMeta.size();
    	}
    }
    
    public long[] getGroupSegementID(int NodeID, String groupID) {
    	VersionSet versions = groupVersionSetMap.get(groupID);
    	int level = 0;  
    	long[] LogID = null;
        if(versions == null) {
    		return null;
    	}

    	List<FileMetaData> groupFileMeta = versions.getCurrent().getFiles(level);
    	if(groupFileMeta.size() == 0) {
    		return null;
    	}else {		
    		List<Long> logs = new ArrayList<>();
    		for(FileMetaData curMeta: groupFileMeta) {
    			logs.add(curMeta.getNumber());
    			Collections.sort(logs);
	    		//System.out.println("log file size:" + logs.size());
	    		LogID = new long[logs.size()];
	    		int count = 0;
	    		for (Long fileNumber : logs) {
	    			LogID[count++]=fileNumber;		  			
	    		}
    		}
    		return LogID;
    	}   	    	   	
    }
    
    public long[] getGlobalSegementID() { 
    	long[] LogID = null;
    	VersionSet globalVersions = groupVersionSetMap.get(globalLogName);
    	int level = 0;  	
    	List<Long> globalLogs = new ArrayList<>();
    	List<FileMetaData> globalFileMeta = globalVersions.getCurrent().getFiles(level);
    	for(FileMetaData curMeta: globalFileMeta) {
    		globalLogs.add(curMeta.getNumber());		
		}
    	Collections.sort(globalLogs);
	  	LogID = new long[globalLogs.size()];
		int count = 0;
		for (Long fileNumber : globalLogs) {
			LogID[count++]=fileNumber;		  			
		}		        	
		return LogID;           	      
    }
    
    public void getGroupAllBytes(String keyspace, int NodeID, String left, String right, String groupID, FileMetaData fileMeta, List<ByteBuffer> rangeGroupBufferList) {
        //mutex.lock();
        //ByteBuffer groupDataBuffer = null;
        TableCache tableCache = groupTableCacheMap.get(groupID);
        InternalTableIterator iterator = tableCache.newIterator(fileMeta);
        iterator.seekToFirstInternal();
        WriteBatchImpl groupWriteBatch = new WriteBatchImpl();
        long totalBytes = 0;
        while(iterator.hasNext()) {
            Entry<InternalKey, Slice> entry = iterator.next();
            InternalKey internalKey = entry.getKey();
            if (internalKey.getValueType() == ValueType.DELETION) {
                totalBytes += internalKey.getUserKey().getBytes().length;
                groupWriteBatch.delete(internalKey.getUserKey().getBytes());
            }
            else if (internalKey.getValueType() == VALUE) {
                byte[] keyBytes = internalKey.getUserKey().getBytes();
                byte[] valueBytes = entry.getValue().getBytes();
                //String keyStr = new String(keyBytes);
                //if(Long.valueOf(left) <= Long.valueOf(keyStr) && Long.valueOf(keyStr) <= Long.valueOf(right)){
                    totalBytes += keyBytes.length;
                    totalBytes += valueBytes.length;
                    groupWriteBatch.put(keyBytes, valueBytes);
                //}
                /*if(Long.valueOf(keyStr) > Long.valueOf(right)){
                    StorageService.instance.printInfo("Long.valueOf(keyStr):"+Long.valueOf(keyStr)+", Long.valueOf(right):"+Long.valueOf(right));
                    break;
                }*/
            }

            if(totalBytes > StorageService.instance.replicaBufferSize){
                Slice groupRecord0 = writeWriteBatch(groupWriteBatch);
                ByteBuffer groupDataBuffer0 = groupRecord0.toByteBuffer();
                groupDataBuffer0.rewind();	
                rangeGroupBufferList.add(groupDataBuffer0);
                StorageService.instance.printInfo("groupDataBuffer0 size:"+groupDataBuffer0.limit());
                groupWriteBatch.clear();
                totalBytes = 0;
            }
        }
        Slice groupRecord = writeWriteBatch(groupWriteBatch);
        ByteBuffer groupDataBuffer = groupRecord.toByteBuffer();
        groupDataBuffer.rewind();		
        //groupDataBuffer.flip();
        //mutex.unlock();
        rangeGroupBufferList.add(groupDataBuffer);
        StorageService.instance.printInfo("groupDataBuffer size:"+groupDataBuffer.limit());
        StorageService.instance.printInfo("rangeGroupBufferList size:"+rangeGroupBufferList.size());
    	//return groupDataBuffer;
    }
    
    public int getRangeRowAndInsertValidator(int NodeID, String rangeLeft, String rangeRight, Map<String, byte[]> keyValueMap) {
        //public Slice getRangeRowAndInsertValidator(int NodeID, String rangeLeft, String rangeRight) {
        	//WriteBatchImpl groupWriteBatch = new WriteBatchImpl();
        	File perNodeLogDir = replicasDirMap.get(NodeID);
        	String rangeGroupName = perNodeLogDir.getAbsoluteFile() + File.separator + "group" + rangeRight;    	
        	File rangeGroupFile = new File(rangeGroupName);        
            if(!rangeGroupFile.exists()){
            	System.out.println("file:" + rangeGroupFile +"doesn't exist!!");
            	return 0;
            }
            //doBackgroundGCWithinPernodeLog(rangeGroupFile);     
            List<File> filenames = Filename.listFiles(rangeGroupFile);
    		List<Long> logs = new ArrayList<>();
    		for (File filename : filenames) {
    			FileInfo fileInfo = Filename.parseFileName(filename);
    			if (fileInfo != null && fileInfo.getFileType() == FileType.LOG) {
    				logs.add(fileInfo.getFileNumber());
    			}
    		}
    		Collections.sort(logs);
    		
    		return 0;
    }
    
    public boolean globalSegmentIsExist(long segmentID) {
    	VersionSet globalVersions = groupVersionSetMap.get(globalLogName);
    	int level = 0;  	
    	//List<Long> globalLogs = new ArrayList<>();
    	List<FileMetaData> globalFileMeta = globalVersions.getCurrent().getFiles(level);
    	for(FileMetaData curMeta: globalFileMeta) {
    		//globalLogs.add(curMeta.getNumber());
    		if(curMeta.getNumber()==segmentID) return true;
		}    	
		return false;
    }

    @Override
    public String getProperty(String name)
    {
        checkBackgroundException();
        return null;
    }

    private void deleteObsoleteFiles(String groupID, File deleteDir)
    {
        checkState(mutex.isHeldByCurrentThread());

        // Make a set of all of the live files
        List<Long> live = new ArrayList<>(this.pendingOutputs);
        VersionSet versions = groupVersionSetMap.get(groupID);
        TableCache tableCache = groupTableCacheMap.get(groupID);
        for (FileMetaData fileMetaData : versions.getLiveFiles()) {       
            live.add(fileMetaData.getNumber());
        }
        /*for (Long fileNum : versions.filesID) {       
            live.add(fileMetaData.getNumber(fileNum));
        }*/
        //System.out.println("live file num:" + live.size());
        //for (File file : Filename.listFiles(databaseDir)) {
        for (File file : Filename.listFiles(deleteDir)) {
            FileInfo fileInfo = Filename.parseFileName(file);
            if (fileInfo == null) {
                continue;
            }
            long number = fileInfo.getFileNumber();
            boolean keep = true;
            switch (fileInfo.getFileType()) {
                case LOG:
                    keep = ((number >= versions.getLogNumber()) ||
                            (number == versions.getPrevLogNumber()));
                    break;
                case DESCRIPTOR:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions.getManifestFileNumber());
                    break;
                case TABLE:
                    keep = live.contains(number);
                    break;
                case TEMP:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = live.contains(number);
                    break;
                case CURRENT:
                case DB_LOCK:
                case INFO_LOG:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (fileInfo.getFileType() == FileType.TABLE) {
                    tableCache.evict(number);
                    //File tableFile = new File(databaseDir, Filename.tableFileName(number));
                    //System.gc();
                    FileOutputStream out = null;
                    try {
                        out = new FileOutputStream(file);
                        out.write(new byte[1]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        if (out != null) {
                            try {
                                out.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    if (file.delete()) {
                        //System.out.println("delete file " + file + "successfully!");    
                        //StorageService.instance.printInfo("delete file " + file + "successfully!");                
                    } else {
                        //System.out.println("delete file " + file + "failed!"); 
                        StorageService.instance.printInfo("delete file " + file + "failed!");                   
                    }
                }else {
	                if (file.delete()) {
	                    //System.out.println("delete file" + fileInfo + "successfully!");                    
	                } else {
	                    System.out.println("delete file" + fileInfo + "failed!");                  
	                }
                }
            }
        }
    }
    
    public void flushMemTable()
    {
        mutex.lock();
        try {
            // force compaction
            makeRoomForWrite(true);
            // todo bg_error code
            while (immutableMemTable != null) {
                backgroundCondition.awaitUninterruptibly();
            }

        }
        finally {
            mutex.unlock();
        }
    }

    public void compactRange(int level, Slice start, Slice end)
    {
        checkArgument(level >= 0, "level is negative");
        checkArgument(level + 1 < NUM_LEVELS, "level is greater than or equal to %s", NUM_LEVELS);
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");

        mutex.lock();
        try {
            while (this.manualCompaction != null) {
                backgroundCondition.awaitUninterruptibly();
            }
            ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
            this.manualCompaction = manualCompaction;
            //maybeScheduleCompaction();
            while (this.manualCompaction == manualCompaction) {
                backgroundCondition.awaitUninterruptibly();
            }
        }
        finally {
            mutex.unlock();
        }

    }

    private void maybeScheduleCompaction()
    {
        checkState(mutex.isHeldByCurrentThread());
        //deleteObsoleteFiles(globalLogName);
        System.out.println("in maybeScheduleCompaction");
        if (backgroundCompaction != null || backgroundGroupGC != null || backgroundFlush != null ) { //|| backgroundFlush != null
        	// || groupVersionSetMap.get(globalLogName).numberOfFilesInLevel(0)< SplitTriggeredLogNumber
        	// Already scheduled
        	return;
        }        
        else if (shuttingDown.get()) {
            // DB is being shutdown; no more background compactions
        }
        else {
            backgroundCompaction = compactionExecutor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    try {
                        backgroundCall();
                    }
                    catch (DatabaseShutdownException ignored) {
                    }
                    catch (Throwable e) {
                        backgroundException = e;
                    }
                    return null;
                }
            });
        }
    }

    private void maybeScheduleFlush()
    {
        checkState(mutex.isHeldByCurrentThread());
        //deleteObsoleteFiles(globalLogName);
        //System.out.println("in maybeScheduleCompaction");
        if (backgroundFlush != null) {        
        	// || groupVersionSetMap.get(globalLogName).numberOfFilesInLevel(0)< SplitTriggeredLogNumber
        	// Already scheduled
        	return;
        }        
        else if (shuttingDown.get()) {
            // DB is being shutdown; no more background compactions
        }
        else {
            backgroundFlush = flushExecutor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    try {
                        backgroundCallFlush();
                    }
                    catch (DatabaseShutdownException ignored) {
                    }
                    catch (Throwable e) {
                        backgroundException = e;
                    }
                    return null;
                }
            });
        }
    }
    
    private void maybeScheduleBackgroundGC(String groupID)
    //private void maybeScheduleBackgroundGC()
    {
        if(groupID == null) return;
        int nodeID = grouptoIDMap.get(groupID);
        StorageService.instance.printInfo("in maybeScheduleBackgroundGC, groupID:"+groupID);
        
        if (backgroundGroupGC != null || backgroundCompaction != null) {
            // Already scheduled
        	return;
        }
        else if (shuttingDown.get()) {
            // DB is being shutdown; no more background compactions
        }
        else {
        	backgroundGroupGC = groupGCExecutor.submit(new Callable<Void>()
            {
                @Override
                public Void call()
                        throws Exception
                {
                    try {                   	
                        backgroundGroupGCCall(nodeID, groupID);
                    }
                    catch (DatabaseShutdownException ignored) {
                    }
                    catch (Throwable e) {
                        backgroundException = e;
                    }
                    return null;
                }
            });
        }
    }

    public void checkBackgroundException()
    {
        Throwable e = backgroundException;
        if (e != null) {
            throw new BackgroundProcessingException(e);
        }
    }

    private void backgroundCall()
            throws IOException
    {
        mutex.lock();
        try {
            if (backgroundCompaction == null) {
                return;
            }

            try {
                if (!shuttingDown.get()) {
                    backgroundCompaction();
                }
            }
            finally {
                backgroundCompaction = null;
            }
        }
        finally {
            try {
                // Previous compaction may have produced too many files in a level,
                // so reschedule another compaction if needed.
                //maybeScheduleCompaction();
            }
            finally {
                try {
                    backgroundCondition.signalAll();
                }
                finally {
                    mutex.unlock();
                }
            }
        }
    }

    private void backgroundCallFlush()
            throws IOException
    {
        mutex.lock();
        try {
            if (backgroundFlush == null) {
                return;
            }

            try {
                if (!shuttingDown.get()) {
                    backgroundFlush();
                }
            }
            finally {
                backgroundFlush = null;
            }
        }
        finally {
            try {
                // Previous compaction may have produced too many files in a level,
                // so reschedule another compaction if needed.
                //maybeScheduleCompaction();
            }
            finally {
                try {
                    backgroundCondition.signalAll();
                }
                finally {
                    mutex.unlock();
                }
            }
        }
    }
    
    private void backgroundGroupGCCall(int nodeID, String groupID)
            throws IOException
    {
        mutex.lock();
        try {
            if (backgroundGroupGC == null) {
                return;
            }

            try {
                if (!shuttingDown.get()) {
                    StorageService.instance.printInfo("before backgroundGroupGC, groupID:"+groupID);
                    backgroundGroupGC(nodeID, groupID);
                }
            }
            finally {
            	backgroundGroupGC = null;
            }
        }
        finally {            
                backgroundCondition.signalAll();           
                mutex.unlock();             
        }     
    }
    
    private void backgroundGroupGC(int nodeID, String groupID)
            throws IOException
    {
    	//duringBackgroundGC = 1;
        checkState(mutex.isHeldByCurrentThread());
        long startTime = System.currentTimeMillis();
        Compaction compaction;      
        VersionSet versions = groupVersionSetMap.get(groupID);
        StorageService.instance.printInfo("in backgroundGroupGC, groupID:"+groupID);
        compaction = versions.pickGroupSegmentGC(nodeID, groupID);
        
        if (compaction == null) {
            // no compaction
        }
        else {
            StorageService.instance.printInfo("before new CompactionState");
            CompactionState compactionState = new CompactionState(compaction);
            StorageService.instance.printInfo("after new CompactionState");
            doGroupGCWork(compactionState, groupID);
            cleanupGroupMerge(compactionState);
            continueWriteGroupMap.put(groupID, 0);
        }      
        //duringBackgroundGC = 0;
        StorageService.instance.mergeSort += System.currentTimeMillis() - startTime;
        StorageService.instance.mergeSortNum ++;
    }

    private void backgroundFlush()
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        compactMemTableInternal();
    }

    private void backgroundCompaction()
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        
        VersionSet versions = groupVersionSetMap.get(globalLogName);
        
        if (versions.numberOfFilesInLevel(0)< SplitTriggeredLogNumber) {
            // Already scheduled
        	System.out.println("versions.numberOfFilesInLevel(0)"+versions.numberOfFilesInLevel(0));
        	return;
        }

        Compaction compaction;
        compaction = versions.pickGlobalLogCompaction(splitSegNum);

        if (compaction == null) {
            // no compaction
        }
        else {
            CompactionState compactionState = new CompactionState(compaction);
            doCompactionWork(compactionState);
            cleanupCompaction(compactionState);
        }
        // manual compaction complete
        if (manualCompaction != null) {
            manualCompaction = null;
        }
    }
    
    private void cleanupCompaction(CompactionState compactionState)
    {
        checkState(mutex.isHeldByCurrentThread());
        for (Map.Entry<String,TableBuilder> batchEntry: builderMap.entrySet()) {
        	String groupID = batchEntry.getKey();
        	TableBuilder curBuilder = batchEntry.getValue();
	        //if (compactionState.builder != null) {
        	if (curBuilder != null) {       	
        		curBuilder.abandon();
	        }
	        else {
	            //checkArgument(compactionState.outfile == null);
	        	//checkArgument(compactionState.outfileMap.get(groupID) == null);
	        }
        }

        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    private void cleanupSplit(CompactionState compactionState)
    {
        //checkState(mutex.isHeldByCurrentThread());
        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }

    private void cleanupGroupMerge(CompactionState compactionState)
    {
        checkState(mutex.isHeldByCurrentThread());

        if (compactionState.builder != null) {
            compactionState.builder.abandon();
        }
        else {
            checkArgument(compactionState.outfile == null);
        }

        for (FileMetaData output : compactionState.outputs) {
            pendingOutputs.remove(output.getNumber());
        }
    }
    
    private long recoverLogFile(long fileNumber, VersionEdit edit)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        File file = new File(databaseDir, Filename.logFileName(fileNumber));
        try (FileInputStream fis = new FileInputStream(file);
                FileChannel channel = fis.getChannel()) {
            LogMonitor logMonitor = LogMonitors.logMonitor();
            LogReader logReader = new LogReader(channel, logMonitor, true, 0);

            // Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);
            // Read all the records and add to a memtable
            long maxSequence = 0;
            MemTable memTable = null;
            for (Slice record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
                SliceInput sliceInput = record.input();
                // read header
                if (sliceInput.available() < 12) {
                    logMonitor.corruption(sliceInput.available(), "log record too small");
                    continue;
                }
                long sequenceBegin = sliceInput.readLong();
                int updateSize = sliceInput.readInt();

                // read entries
                WriteBatchImpl writeBatch = readWriteBatch(sliceInput, updateSize);

                // apply entries to memTable
                if (memTable == null) {
                    memTable = new MemTable(internalKeyComparator);
                }
                writeBatch.forEach(new InsertIntoHandler(memTable, sequenceBegin));

                // update the maxSequence
                long lastSequence = sequenceBegin + updateSize - 1;
                if (lastSequence > maxSequence) {
                    maxSequence = lastSequence;
                }

                // flush mem table if necessary
                //if (memTable.approximateMemoryUsage() > options.writeBufferSize()) {
                if (memTable.approximateMemoryUsage() > writeBufferSize) {
                    writeLevel0Table(memTable, edit, null);
                    memTable = null;
                }
            }

            // flush mem table
            if (memTable != null && !memTable.isEmpty()) {
                writeLevel0Table(memTable, edit, null);
            }

            return maxSequence;
        }
    }

    @Override
    public byte[] get(byte[] key)
            throws DBException
    {
        return get(key, new ReadOptions());
    }

    @Override
    public byte[] get(byte[] key, ReadOptions options)
            throws DBException
    {
        LookupKey lookupKey;
        //mutex.lock();
        try {
            SnapshotImpl snapshot = getSnapshot(options);
            lookupKey = new LookupKey(Slices.wrappedBuffer(key), snapshot.getLastSequence());
        }
        finally {
            //mutex.unlock();
        }
        LookupResult lookupResult = null;
		String rangGroupID = getRangGroupID(new String(key));
        VersionSet curVersion = groupVersionSetMap.get(rangGroupID);
        //String str = "look key:"+new String(key)+", rangGroupID:"+rangGroupID+", curVersion numberOfFilesInLevel:"+curVersion.numberOfFilesInLevel(0);
        //StorageService.instance.printInfo(str);
		lookupResult = curVersion.get(lookupKey);

        if (lookupResult != null) {
            Slice value = lookupResult.getValue();
            //String str1 = "find key:"+new String(key);
            //StorageService.instance.printInfo(str1);
            if (value != null) {
                return value.getBytes();
            }
        } else{
            //StorageService.instance.printInfo("Not find key:"+ new String(key));
        }
        return null;
    }

    @Override
    public void put(byte[] key, byte[] value)
            throws DBException
    {
        put(key, value, new WriteOptions());
    }

    @Override
    public Snapshot put(byte[] key, byte[] value, WriteOptions options)
            throws DBException
    {
        return writeInternal(new WriteBatchImpl().put(key, value), options);
    }

    @Override
    public void delete(byte[] key)
            throws DBException
    {
        writeInternal(new WriteBatchImpl().delete(key), new WriteOptions());
    }

    @Override
    public Snapshot delete(byte[] key, WriteOptions options)
            throws DBException
    {
        return writeInternal(new WriteBatchImpl().delete(key), options);
    }

    @Override
    public void write(WriteBatch updates)
            throws DBException
    {
        writeInternal((WriteBatchImpl) updates, new WriteOptions());
    }

    @Override
    public Snapshot write(WriteBatch updates, WriteOptions options)
            throws DBException
    {
        return writeInternal((WriteBatchImpl) updates, options);
    }

    public Snapshot writeInternal(WriteBatchImpl updates, WriteOptions options)
            throws DBException
    {
        checkBackgroundException();
        mutex.lock();
        try {
        	VersionSet versions = groupVersionSetMap.get(globalLogName);
            long sequenceEnd;
            if (updates.size() != 0) {
                makeRoomForWrite(false);
               
                // Get sequence numbers for this change set
                long sequenceBegin = versions.getLastSequence() + 1;
                sequenceEnd = sequenceBegin + updates.size() - 1;

                // Reserve this sequence in the version set
                versions.setLastSequence(sequenceEnd);

                // Log write
                Slice record = writeWriteBatch(updates, sequenceBegin);
                try {
                    log.addRecord(record, options.sync());
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                // Update memtable
                updates.forEach(new InsertIntoHandler(memTable, sequenceBegin));
            }
            else {
                sequenceEnd = versions.getLastSequence();
            }

            if (options.snapshot()) {
                return new SnapshotImpl(versions.getCurrent(), sequenceEnd);
            }
            else {
                return null;
            }
        }
        finally {
            mutex.unlock();
        }
    }

    @Override
    public WriteBatch createWriteBatch()
    {
        checkBackgroundException();
        return new WriteBatchImpl();
    }

    @Override
    public SeekingIteratorAdapter iterator()
    {
        return iterator(new ReadOptions());
    }

    @Override
    public SeekingIteratorAdapter iterator(ReadOptions options)
    {
        checkBackgroundException();
        mutex.lock();
        try {
            DbIterator rawIterator = internalIterator();

            // filter any entries not visible in our snapshot
            SnapshotImpl snapshot = getSnapshot(options);
            SnapshotSeekingIterator snapshotIterator = new SnapshotSeekingIterator(rawIterator, snapshot, internalKeyComparator.getUserComparator());
            return new SeekingIteratorAdapter(snapshotIterator);
        }
        finally {
            mutex.unlock();
        }
    }

    public void iterator(String groupID, String startKey, String stopKey, List<byte[]> valueList, int isFirstGroup, List<UnfilteredPartitionIterator> iterators, boolean isForThrift)
    { 
        ReadOptions options = new ReadOptions();
        //mutex.lock();
        try {
            StorageService.instance.printInfo("###in iterator, groupID:"+groupID);  
            DbIterator dbIterator = internalIterator(groupID);
            if(dbIterator==null) return;
            // filter any entries not visible in our snapshot
            //return dbIterator;
            InternalKey interStartKey = new InternalKey(new Slice(startKey.getBytes()), 0, VALUE);     
            InternalKey interStopKey = new InternalKey(new Slice(stopKey.getBytes()), 0, VALUE);   
            //StorageService.instance.printInfo("###interStartKey:"+interStartKey);  
            if(isFirstGroup==1){
                dbIterator.seekInternal(interStartKey);
            }else{
                dbIterator.seekToFirstInternal();
            }

            StorageService.instance.printInfo("###after seekInternal, interStartKey:"+interStartKey+", interStopKey:"+interStopKey);  
            int count = 0;
            for(; dbIterator.hasNext(); dbIterator.next()) {
                InternalKey curKey = dbIterator.peek().getKey();

                if(internalKeyComparator.compare(curKey, interStopKey) > 0) break;
                if(internalKeyComparator.compare(curKey, interStartKey) < 0) continue;
                byte[] value = dbIterator.peek().getValue().getBytes();
                //valueList.add(value);
                //if(value!=null && value.length > 1000){
                if(value!=null){
                    //logger.debug("look startKey:{}, KVEntry size:{}", startKey, KVEntry.length);             
                    try{
                        Mutation remutation = Mutation.serializer.deserializeToMutation(new DataInputBuffer(value), MessagingService.current_version);
                        for (PartitionUpdate update : remutation.getPartitionUpdates()){
                            UnfilteredRowIterator partition = update.unfilteredIterator();
                            iterators.add(RTBoundValidator.validate(new SingletonUnfilteredPartitionIterator(partition, isForThrift), RTBoundValidator.Stage.SSTABLE, false));                     
                        }
                    }catch (IOException e){
                        StorageService.instance.printInfo("Mutation.serializer.deserialize failed in queryLocalRegion range query!");
                    }
                }
                count++;
                if(count >= StorageService.instance.maxScanNumber) break;
                //StorageService.instance.printInfo("###interStopKey:"+interStopKey+", curKey:"+curKey);          
            }
           
        }
        finally {
            //mutex.unlock();
        }
    }

    SeekingIterable<InternalKey, Slice> internalIterable()
    {
        return new SeekingIterable<InternalKey, Slice>()
        {
            @Override
            public DbIterator iterator()
            {
                return internalIterator();
            }
        };
    }

    DbIterator internalIterator()
    {
        mutex.lock();
        try {
            // merge together the memTable, immutableMemTable, and tables in version set
            MemTableIterator iterator = null;
            if (immutableMemTable != null) {
                iterator = immutableMemTable.iterator();
            }
            Version current = groupVersionSetMap.get(globalLogName).getCurrent();
            return new DbIterator(memTable.iterator(), iterator, current.getLevel0Files(), current.getLevelIterators(), internalKeyComparator);
        }
        finally {
            mutex.unlock();
        }
    }

    DbIterator internalIterator(String groupID)
    {
        //mutex.lock();
        try {
            //StorageService.instance.printInfo("###before new groupVersionSetMap, groupID:"+groupID); 
            Version current = groupVersionSetMap.get(groupID).getCurrent();
            StorageService.instance.printInfo("###before new DbIterator, current.getLevel0Files() size:"+current.getLevel0Files().size()); 
            //return new DbIterator(memTable.iterator(), iterator, current.getLevel0Files(), current.getLevelIterators(), internalKeyComparator);
            if(current.getLevel0Files().size() > 0){
                return new DbIterator(null, null, current.getLevel0Files(), null, internalKeyComparator);
            }else{
                return null;
            }
        }
        finally {
            //mutex.unlock();
        }
    }

    public void getGroupFileMeta(String groupID, List<FileMetaData> groupMetaList){
        VersionSet curVersionSet = groupVersionSetMap.get(groupID);
        if(curVersionSet!=null){
            //return curVersionSet.getCurrent().getLevel0FileMeta();
            groupMetaList.addAll(curVersionSet.getCurrent().getLevel0FileMeta());
            StorageService.instance.printInfo("###in getGroupFileMeta, groupID:"+groupID+", size:"+curVersionSet.getCurrent().getLevel0FileMeta().size()); 
        }
    }

    @Override
    public Snapshot getSnapshot()
    {
        checkBackgroundException();
        mutex.lock();
        try {
        	VersionSet versions = groupVersionSetMap.get(globalLogName);
            return new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
        }
        finally {
            mutex.unlock();
        }
    }

    private SnapshotImpl getSnapshot(ReadOptions options)
    {
        SnapshotImpl snapshot;
        if (options.snapshot() != null) {
            snapshot = (SnapshotImpl) options.snapshot();
        }
        else {
        	VersionSet versions = groupVersionSetMap.get(globalLogName);
            snapshot = new SnapshotImpl(versions.getCurrent(), versions.getLastSequence());
            snapshot.close(); // To avoid holding the snapshot active..
        }
        return snapshot;
    }
    
    private void makeRoomForWrite(boolean force)
    {
        checkState(mutex.isHeldByCurrentThread());

        boolean allowDelay = !force;
        VersionSet versions = groupVersionSetMap.get(globalLogName);
        
        while (true) {
            //if (!force && memTable.approximateMemoryUsage() <= options.writeBufferSize()) {
        	if (!force && memTable.approximateMemoryUsage() <= writeBufferSize) {
                // There is room in current memtable
                break;
            }
            else if (immutableMemTable != null) {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                backgroundCondition.awaitUninterruptibly();
            }
            else {
                // Attempt to switch to a new memtable and trigger compaction of old
                checkState(versions.getPrevLogNumber() == 0);
                // close the existing log
                try {
                    log.close();
                }
                catch (IOException e) {
                    throw new RuntimeException("Unable to close log file " + log.getFile(), e);
                }

                // open a new log
                long logNumber = versions.getNextFileNumber();
                try {
                    this.log = Logs.createLogWriter(new File(databaseDir, Filename.logFileName(logNumber)), logNumber);
                }
                catch (IOException e) {
                    throw new RuntimeException("Unable to open new log file " +
                            new File(databaseDir, Filename.logFileName(logNumber)).getAbsoluteFile(), e);
                }

                // create a new mem table
                immutableMemTable = memTable;
                memTable = new MemTable(internalKeyComparator);

                // Do not force another compaction there is space available
                force = false;             
                maybeScheduleFlush();               
            }
        }
    }

    public void compactMemTable()
            throws IOException
    {
        mutex.lock();
        try {
            compactMemTableInternal();
        }
        finally {
            mutex.unlock();
        }
    }

    private void compactMemTableInternal()
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        if (immutableMemTable == null) {
            return;
        }

        try {
            // Save the contents of the memtable as a new Table
            VersionEdit edit = new VersionEdit();
            //Version base = versions.getCurrent();         
            Version base = groupVersionSetMap.get(globalLogName).getCurrent();
            
            writeLevel0Table(immutableMemTable, edit, base);

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("Database shutdown during memtable compaction");
            }

            // Replace immutable memtable with the generated Table
            edit.setPreviousLogNumber(0);
            edit.setLogNumber(log.getFileNumber());  // Earlier logs no longer needed
            //versions.logAndApply(edit);
            groupVersionSetMap.get(globalLogName).logAndApply(edit, 1);

            immutableMemTable = null;

            //deleteObsoleteFiles(globalLogName, databaseDir);
        }
        finally {
            backgroundCondition.signalAll();
        }
    }

    private void writeLevel0Table(MemTable mem, VersionEdit edit, Version base)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());

        // skip empty mem table
        if (mem.isEmpty()) {
            return;
        }

        VersionSet versions = groupVersionSetMap.get(globalLogName);
        // write the memtable to a new sstable
        long fileNumber = versions.getNextFileNumber();
        pendingOutputs.add(fileNumber);
        mutex.unlock();
        FileMetaData meta;
        try {
            meta = buildTable(mem, fileNumber);
        }
        finally {
            mutex.lock();
        }
        pendingOutputs.remove(fileNumber);

        // Note that if file size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (meta != null && meta.getFileSize() > 0) {
            edit.addFile(level, meta);
        }
    }

    private FileMetaData buildTable(SeekingIterable<InternalKey, Slice> data, long fileNumber)
            throws IOException
    {
        File file = new File(databaseDir, Filename.tableFileName(fileNumber));
        try {
            InternalKey smallest = null;
            InternalKey largest = null;
            FileChannel channel = new FileOutputStream(file).getChannel();
            tableMetaMap.clear();
            try {
                TableBuilder tableBuilder = new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));

                for (Entry<InternalKey, Slice> entry : data) {
                    // update keys
                    InternalKey key = entry.getKey();
                    if (smallest == null) {
                        smallest = key;
                    }
                    largest = key;

                    tableBuilder.add(key.encode(), entry.getValue());
                }
                
                tableBuilder.finish(tableMetaMap);
            }
            finally {
                try {
                    channel.force(true);
                }
                finally {
                    channel.close();
                }
            }

            if (smallest == null) {
                return null;
            }
            
            FileMetaData fileMetaData = new FileMetaData(globalLogName, fileNumber, tableMetaMap.get(0), tableMetaMap.get(1), file.length(), smallest, largest);

            // verify table can be opened
            //tableCache.newIterator(fileMetaData);
            pendingOutputs.remove(fileNumber);
            return fileMetaData;

        }
        catch (IOException e) {
            file.delete();
            throw e;
        }
    }

    public void splitToRangeGroups(byte[] curKey, byte[] value, String rangGroupID, CompactionState splitCompactionState) {
        //mutex.lock();
        int nodeID = StorageService.instance.findNodeIDAccordingToken(dbKeySpaceName, new String(curKey));
        //String rangGroupID = getRangGroupID(StorageService.instance.getTokenFactory().toString(remutation.key().getToken()));
        TableBuilder curBuilder = builderMap.get(rangGroupID);              
        //String info0 = "nodeID:"+nodeID+", rangGroupID:"+rangGroupID+", key:"+new String(curKey);
        //StorageService.instance.printInfo(info0);
        try{
            if (curBuilder == null) {
                openCompactionOutputFile(splitCompactionState, nodeID, rangGroupID);
                curBuilder = builderMap.get(rangGroupID);
            }
            //String info1 = "curBuilder.getEntryCount:"+curBuilder.getEntryCount();
            //StorageService.instance.printInfo(info1);
            Slice curSliceKey = new Slice(curKey);
            Slice curSliceValue = new Slice(value);  
            //long sequenceBegin = groupVersionSetMap.get(rangGroupID).getLastSequence() + 1;
            InternalKey internalCurKey = new InternalKey(curSliceKey, 0, VALUE);                
            if (curBuilder.getEntryCount() == 0) {
                splitCompactionState.currentSmallestMap.put(rangGroupID, internalCurKey);
            }
            splitCompactionState.currentLargestMap.put(rangGroupID, internalCurKey);
            //groupVersionSetMap.get(rangGroupID).setLastSequence(sequenceBegin);
            //String info2 = "currentLargest:"+splitCompactionState.currentLargest;
            //StorageService.instance.printInfo(info2);
            //compactionState.builder.add(key.encode(), iterator.peek().getValue());
            curBuilder.add(internalCurKey.encode(), curSliceValue);
            // Close output file if it is big enough                   
            if (curBuilder.getFileSize() > TARGET_FILE_SIZE) {
                finishCompactionOutputFile(splitCompactionState, rangGroupID, 1);
                deleteBulderFromMap(rangGroupID);
                builderMap.remove(rangGroupID);
                outfileMap.remove(rangGroupID);
                outStreamMap.remove(rangGroupID);
            }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                StorageService.instance.printInfo("in splitToRangeGroups failed, curKey: "+curKey);
                e.printStackTrace();
			}
        //mutex.unlock();
    }

    public void installSplitResults(CompactionState splitCompactionState) throws IOException {
        mutex.lock();
        String info = "----in installSplitResults, before finishCompactionOutputFile, builderMap size:"+builderMap.size();
        StorageService.instance.printInfo(info);
        for (Map.Entry<String,TableBuilder> batchEntry: builderMap.entrySet()) {
            String groupID = batchEntry.getKey();
            TableBuilder curBuilder = batchEntry.getValue();
            if (curBuilder != null) {
                //String info1 = "###groupID:"+groupID;
                //StorageService.instance.printInfo(info1);
                finishCompactionOutputFile(splitCompactionState, groupID, 1);
                deleteBulderFromMap(groupID);
            }
        }
        builderMap.clear();
        outfileMap.clear();
        outStreamMap.clear();
        // todo port CompactionStats code
        installSplitResults(splitCompactionState, globalLogName);
        StorageService.instance.printInfo("installSplitResults, after installSplitResults");
        mutex.unlock();
    }

    private void doCompactionWork(CompactionState compactionState)
            throws IOException
    {
    	VersionSet versions = groupVersionSetMap.get(globalLogName);
        checkState(mutex.isHeldByCurrentThread());
        compactionState.smallestSnapshot = versions.getLastSequence();
        int count = 0;
        // Release mutex while we're actually doing the compaction work
        mutex.unlock();
        try {
            MergingIterator iterator = versions.makeInputIterator(compactionState.compaction);
            System.out.println("after makeInputIterator");
            Slice currentUserKey = null;
            boolean hasCurrentUserKey = false;

            long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            while (iterator.hasNext() && !shuttingDown.get()) {
                InternalKey key = iterator.peek().getKey();
                // Handle key/value, add to state, etc.
                boolean drop = false;
                // todo if key doesn't parse (it is corrupted),
                if (false /*!ParseInternalKey(key, &ikey)*/) {
                    // do not hide error keys
                    currentUserKey = null;
                    hasCurrentUserKey = false;
                    lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                }
                else {
                    StorageService.instance.printInfo("-key.getUserKey():" + key.getUserKey() + ", currentUserKey:" + currentUserKey);
            
                    //if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey().copySlice(), currentUserKey.copySlice()) != 0) {
                    if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                        // First occurrence of this user key
                        //currentUserKey = key.getUserKey().copySlice();
                        currentUserKey = key.getUserKey();
                        hasCurrentUserKey = true;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }

                    if (lastSequenceForKey <= compactionState.smallestSnapshot) {
                        // Hidden by an newer entry for same user key
                        drop = true; // (A)
                    }
                    else if (key.getValueType() == DELETION &&
                            key.getSequenceNumber() <= compactionState.smallestSnapshot ) {
                    	//&& compactionState.compaction.isBaseLevelForKey(key.getUserKey())
                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop (by rule (A) above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true;
                    }

                    lastSequenceForKey = key.getSequenceNumber();
                }

                if (!drop) {
			        /////////////////////////////
                    //String curKey = key.getUserKey().toString();
                    byte[] curKey = key.getUserKey().getBytes();
                    //String strKey = new String(curKey);
                    int nodeID = StorageService.instance.findNodeIDAccordingToken(dbKeySpaceName, new String(curKey));
                	String rangGroupID = getRangGroupID(new String(curKey));
                    //String rangGroupID = getRangGroupID(StorageService.instance.getTokenFactory().toString(remutation.key().getToken()));
                	TableBuilder curBuilder = builderMap.get(rangGroupID);
                    // Open output file if necessary
                	if (curBuilder == null) {
                        openCompactionOutputFile(compactionState, nodeID, rangGroupID);
                        curBuilder = builderMap.get(rangGroupID);
                    }

                    //String info = "MESSAGE, rangGroupID"+rangGroupID+", builderSize:"+curBuilder.getFileSize()+", builderMap size:"+ builderMap.size()+", outFileSize:"+compactionState.compaction.getMaxOutputFileSize();
                    //StorageService.instance.printInfo(info);
                    //if (compactionState.builder.getEntryCount() == 0) {
                	if (curBuilder.getEntryCount() == 0) {
                        compactionState.currentSmallestMap.put(rangGroupID, key);
                    }
                    compactionState.currentLargestMap.put(rangGroupID, key);
                    //compactionState.builder.add(key.encode(), iterator.peek().getValue());
                    curBuilder.add(key.encode(), iterator.peek().getValue());

                    // Close output file if it is big enough
                    //if (compactionState.builder.getFileSize() >= compactionState.compaction.getMaxOutputFileSize()) {
                    if (curBuilder.getFileSize() > TARGET_FILE_SIZE) {
                        finishCompactionOutputFile(compactionState, rangGroupID, 1);
                        deleteBulderFromMap(rangGroupID);
                        builderMap.remove(rangGroupID);
                        outfileMap.remove(rangGroupID);
                        outStreamMap.remove(rangGroupID);
                    }
                }
                /*else {
                	System.out.println("####split drop key:"+ key.getUserKey().getBytes());
                }*/
                iterator.next();
            }

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("DB shutdown during compaction");
            }
            System.out.println("before finishCompactionOutputFile");
            String info = "------before finishCompactionOutputFile, builderMap size:"+builderMap.size();
            StorageService.instance.printInfo(info);
            for (Map.Entry<String,TableBuilder> batchEntry: builderMap.entrySet()) {
            	String groupID = batchEntry.getKey();
            	TableBuilder curBuilder = batchEntry.getValue();
            	if (curBuilder != null) {
                    //String info1 = "###groupID:"+groupID;
                    //StorageService.instance.printInfo(info1);
                    finishCompactionOutputFile(compactionState, groupID, 1);
                    deleteBulderFromMap(groupID);          
                }
            }
            builderMap.clear();
            outfileMap.clear();
            outStreamMap.clear(); 
        }
        finally {
            mutex.lock();
        }
    
        // todo port CompactionStats code
        installCompactionResults(compactionState, globalLogName);
        StorageService.instance.printInfo("after installCompactionResults");   
    }
    
    private void doGroupGCWork(CompactionState compactionState, String rangGroupID)
            throws IOException
    {
    	VersionSet versions = groupVersionSetMap.get(rangGroupID);
        checkState(mutex.isHeldByCurrentThread());
        compactionState.smallestSnapshot = versions.getLastSequence();
        int count = 0;
        int nodeID = grouptoIDMap.get(rangGroupID);
        //mutex.unlock();
        try {
            MergingIterator iterator = versions.makeInputIterator(compactionState.compaction);
            Slice currentUserKey = null;
            boolean hasCurrentUserKey = false;

            long lastSequenceForKey = MAX_SEQUENCE_NUMBER;
            //iterator.seekToFirstInternal();
            //Entry<InternalKey, Slice> KVEntry = iterator.getNextElement();
            while (iterator.hasNext() && !shuttingDown.get()) {
            //while (KVEntry!=null && !shuttingDown.get()) {
                InternalKey key = iterator.peek().getKey();
                //InternalKey key = KVEntry.getKey();
                //StorageService.instance.printInfo("-----key:"+key);

                // Handle key/value, add to state, etc.
                boolean drop = false;
                // todo if key doesn't parse (it is corrupted),
                if (false /*!ParseInternalKey(key, &ikey)*/) {
                    // do not hide error keys
                    currentUserKey = null;
                    hasCurrentUserKey = false;
                    lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                }
                else {
                    //StorageService.instance.printInfo("-key.getUserKey():" + key.getUserKey() + ", currentUserKey:" + currentUserKey);
                    //if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey().copySlice(), currentUserKey.copySlice()) != 0) {
                    //if (!hasCurrentUserKey || internalKeyComparator.getUserComparator().compare(key.getUserKey(), currentUserKey) != 0) {
                    if (!hasCurrentUserKey || key.getUserKey().compareTo(currentUserKey) != 0) {
                        // First occurrence of this user key
                        currentUserKey = key.getUserKey();
                        //currentUserKey = key.getUserKey().copySlice();
                        hasCurrentUserKey = true;
                        lastSequenceForKey = MAX_SEQUENCE_NUMBER;
                    }                   
                    if (key.getValueType() == DELETION) {
                    	//&& key.getSequenceNumber() <= compactionState.smallestSnapshot
                    	//&& compactionState.compaction.isBaseLevelForKey(key.getUserKey())
                    	
                        // For this user key:
                        // (1) there is no data in higher levels
                        // (2) data in lower levels will have larger sequence numbers
                        // (3) data in layers that are being compacted here and have
                        //     smaller sequence numbers will be dropped in the next
                        //     few iterations of this loop (by rule (A) above).
                        // Therefore this deletion marker is obsolete and can be dropped.
                        drop = true;
                    }

                    lastSequenceForKey = key.getSequenceNumber();
                }

                if (!drop) {
			        /////////////////////////////
					// Open output file if necessary
                    if (compactionState.builder == null) {
                        StorageService.instance.printInfo("-----before openCompactionOutputFileForGC");        
                        openCompactionOutputFileForGC(compactionState, nodeID, rangGroupID);
                    }
                    if (compactionState.builder.getEntryCount() == 0) {
                        //compactionState.currentSmallest = key;
                        compactionState.currentSmallest = new InternalKey(key.getUserKey().copySlice(), 0, VALUE);
                    }
                    compactionState.currentLargest = key;
                    compactionState.builder.add(key.encode(), iterator.peek().getValue());
                    //compactionState.builder.add(key.encode(), KVEntry.getValue());
                    //StorageService.instance.printInfo("-----before curBuilder.add");        

                    // Close output file if it is big enough
                    //if (curBuilder.getFileSize() >= compactionState.compaction.getMaxOutputFileSize()) {
                    if (compactionState.builder.getFileSize() >= GCBuilderSSTableSize) {
                        finishCompactionOutputFileForGC(compactionState, rangGroupID);
                    }
                }/*else {
                	System.out.println("####gc drop key:"+ key.getUserKey().getBytes());
                }*/
                iterator.next();
                //KVEntry = iterator.getNextElement();
            }

            if (shuttingDown.get()) {
                throw new DatabaseShutdownException("DB shutdown during compaction");
            }
            StorageService.instance.printInfo("in GC, before finishCompactionOutputFile");
            if (compactionState.builder != null) {
                finishCompactionOutputFileForGC(compactionState, rangGroupID);
            }
        }
        finally {
            //mutex.lock();
        }
        // todo port CompactionStats code
        installCompactionResultsForGC(compactionState, rangGroupID);           
    }

	public String getRangGroupID(String keyToken) {
		return StorageService.instance.findBoundTokenAccordingTokeny(keyToken);
	} 

    public File getGroupFile(int nodeID, String rangGroupID)
            throws FileNotFoundException
    {
        //mutex.lock();
        File replicaFile = replicasDirMap.get(nodeID);
        String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + rangGroupID;                   	    
		File rangeGroupFile = new File(rangeGroupName); 
		//String info = "in openCompactionOutputFile,"+rangeGroupFile;
        //StorageService.instance.printInfo(info);

        VersionSet versions = groupVersionSetMap.get(rangGroupID);
        if(versions==null) {
        	int tableCacheSize = options.maxOpenFiles() - 10;
            TableCache curtableCache = new TableCache(rangeGroupFile, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());
            groupTableCacheMap.put(rangGroupID, curtableCache);
            
            VersionSet groupVersions = null;
			try {
                //List<FileMetaData> files = new ArrayList<FileMetaData>();
                List<Long> filesID = new ArrayList<Long>();
				groupVersions = new VersionSet(rangeGroupFile, curtableCache, internalKeyComparator, filesID);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            groupVersionSetMap.put(rangGroupID, groupVersions);
            String info1 = "after groupVersionSetMap.put,"+rangGroupID;
            StorageService.instance.printInfo(info1);
            versions = groupVersionSetMap.get(rangGroupID);
        }
        return rangeGroupFile;
    }
    
    private void openCompactionOutputFile(CompactionState compactionState, int nodeID, String rangGroupID)
            throws FileNotFoundException
    {
        requireNonNull(compactionState, "compactionState is null");
        mutex.lock();
        File replicaFile = replicasDirMap.get(nodeID);
        String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + rangGroupID;                   	    
		File rangeGroupFile = new File(rangeGroupName); 
		//String info = "in openCompactionOutputFile,"+rangeGroupFile;
        //StorageService.instance.printInfo(info);

        VersionSet versions = groupVersionSetMap.get(rangGroupID);
        if(versions==null) {
        	int tableCacheSize = options.maxOpenFiles() - 10;
            TableCache curtableCache = new TableCache(rangeGroupFile, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());
            groupTableCacheMap.put(rangGroupID, curtableCache);
            
            VersionSet groupVersions = null;
			try {
                //List<FileMetaData> files = new ArrayList<FileMetaData>();
                List<Long> filesID = new ArrayList<Long>();
				groupVersions = new VersionSet(rangeGroupFile, curtableCache, internalKeyComparator, filesID);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            groupVersionSetMap.put(rangGroupID, groupVersions);
            versions = groupVersionSetMap.get(rangGroupID);
        }
        
        try {
            long fileNumber = versions.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumberMap.put(rangGroupID, fileNumber);
            //StorageService.instance.printInfo("fileNumber:"+fileNumber);
            compactionState.currentFileSize = 0;

            //File file = new File(databaseDir, Filename.tableFileName(fileNumber));
            File file = new File(rangeGroupFile, Filename.tableFileName(fileNumber));
            //@SuppressWarnings("resource")
            FileOutputStream outfileStream = new FileOutputStream(file);
            FileChannel outfileChannel = outfileStream.getChannel();
            outStreamMap.put(rangGroupID, outfileStream);
            outfileMap.put(rangGroupID, outfileChannel);
            TableBuilder fileBuilder = new TableBuilder(options, outfileChannel, new InternalUserComparator(internalKeyComparator));
            builderMap.put(rangGroupID, fileBuilder);
        }
        finally {
            mutex.unlock();
        }
    }

    private void openCompactionOutputFileForGC(CompactionState compactionState, int nodeID, String rangGroupID)
            throws FileNotFoundException
    {
        //mutex.lock();
        try {
            File replicaFile = replicasDirMap.get(nodeID);
            String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + rangGroupID;                   	    
            File rangeGroupFile = new File(rangeGroupName); 
            //String info = "in openCompactionOutputFileForGC,"+rangeGroupFile;
            //StorageService.instance.printInfo(info);
            VersionSet versions = groupVersionSetMap.get(rangGroupID);
            if(versions==null) {
                int tableCacheSize = options.maxOpenFiles() - 10;
                TableCache curtableCache = new TableCache(rangeGroupFile, tableCacheSize, new InternalUserComparator(internalKeyComparator), options.verifyChecksums());
                groupTableCacheMap.put(rangGroupID, curtableCache);
                
                VersionSet groupVersions = null;
                try {
                    //List<FileMetaData> files = new ArrayList<FileMetaData>();
                    List<Long> filesID = new ArrayList<Long>();
                    groupVersions = new VersionSet(rangeGroupFile, curtableCache, internalKeyComparator, filesID);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                groupVersionSetMap.put(rangGroupID, groupVersions);            
                versions = groupVersionSetMap.get(rangGroupID);
            }
            long fileNumber = versions.getNextFileNumber();
            pendingOutputs.add(fileNumber);
            compactionState.currentFileNumber = fileNumber;
            compactionState.currentFileSize = 0;
            compactionState.currentSmallest = null;
            compactionState.currentLargest = null;
            StorageService.instance.printInfo("##Create new file:" + fileNumber);
            //File file = new File(databaseDir, Filename.tableFileName(fileNumber));
            File file = new File(rangeGroupFile, Filename.tableFileName(fileNumber));
            //pactionState.outfile = new FileOutputStream(file).getChannel();
            compactionState.outStream = new FileOutputStream(file);
            compactionState.outfile = compactionState.outStream.getChannel();
            compactionState.builder = new TableBuilder(options, compactionState.outfile, new InternalUserComparator(internalKeyComparator));
        }
        finally {
            //mutex.unlock();
        }
    }

    private void finishCompactionOutputFile(CompactionState compactionState, String rangGroupID, int splitWrite)
            throws IOException
    {
        requireNonNull(compactionState, "compactionState is null");
        //StorageService.instance.printInfo("in finishCompactionOutputFile, rangGroupID:"+rangGroupID);
        long outputNumber = compactionState.currentFileNumberMap.get(rangGroupID);
        StorageService.instance.printInfo("rangGroupID:"+rangGroupID+", outputNumber:"+outputNumber);
        checkArgument(outputNumber != 0);

        tableMetaMap.clear();
        long currentEntries = builderMap.get(rangGroupID).getEntryCount();
        builderMap.get(rangGroupID).finish(tableMetaMap);
        //long currentBytes = compactionState.builder.getFileSize();
        long currentBytes = builderMap.get(rangGroupID).getFileSize();
        compactionState.currentFileSize = currentBytes;
        StorageService.instance.writeGroupBytes += currentBytes;
        compactionState.totalBytes += currentBytes;
        
        if(splitWrite==1) {
	        int newContinueWriteGroupSize = continueWriteGroupMap.get(rangGroupID) + (int)(currentBytes/1024);
			continueWriteGroupMap.put(rangGroupID, newContinueWriteGroupSize);	
        }
		InternalKey smallest = compactionState.currentSmallestMap.get(rangGroupID);
        InternalKey largest = compactionState.currentLargestMap.get(rangGroupID);
        //String info = "smallest:"+ smallest + "largest:" + largest + "currentFileSize:"+currentBytes;
        //StorageService.instance.printInfo(info);

        FileMetaData currentFileMetaData = new FileMetaData(rangGroupID, compactionState.currentFileNumberMap.get(rangGroupID),
                tableMetaMap.get(0),
                tableMetaMap.get(1),
                compactionState.currentFileSize,
                smallest,
                largest);
        compactionState.outputs.add(currentFileMetaData);

        if (currentEntries > 0) {
            // Verify that the table is usable
            //tableCache.newIterator(outputNumber);
        }
    }

    private void finishCompactionOutputFileForGC(CompactionState compactionState, String rangGroupID)
            throws IOException
    {
        long outputNumber = compactionState.currentFileNumber;
        checkArgument(outputNumber != 0);

        long currentEntries = compactionState.builder.getEntryCount();
        tableMetaMap.clear();
        compactionState.builder.finish(tableMetaMap);

        long currentBytes = compactionState.builder.getFileSize();
        compactionState.currentFileSize = currentBytes;
        StorageService.instance.writeGroupBytes += currentBytes;
        compactionState.totalBytes += currentBytes;
        InternalKey currentLargest = new InternalKey(compactionState.currentLargest.getUserKey().copySlice(), 0, VALUE);   //////
        FileMetaData currentFileMetaData = new FileMetaData(rangGroupID, compactionState.currentFileNumber,
                tableMetaMap.get(0),
                tableMetaMap.get(1),
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                //compactionState.currentLargest
                currentLargest);
        compactionState.outputs.add(currentFileMetaData);
        StorageService.instance.printInfo("####compaction output total data size:"+currentBytes/1048576 +"MB");

        compactionState.builder = null;

        compactionState.outfile.force(true);
        //compactionState.outfile.close();
        Closeables.closeQuietly(compactionState.outfile);
        compactionState.outfile = null;
        //compactionState.outStream.close();
        Closeables.closeQuietly(compactionState.outStream);
        compactionState.outStream = null;

        if (currentEntries > 0) {
            // Verify that the table is usable
            //tableCache.newIterator(outputNumber);
        }
    }

    public void deleteBulderFromMap(String rangGroupID){
        try{
            TableBuilder builder = builderMap.get(rangGroupID);
            builder = null;
            //builderMap.remove(rangGroupID);
            FileChannel channel = outfileMap.get(rangGroupID);
            channel.force(true);
            //channel.close();
            Closeables.closeQuietly(channel);
            channel = null;
            FileOutputStream outfileStream = outStreamMap.get(rangGroupID);
            //outfileStream.close();
            Closeables.closeQuietly(outfileStream);
            outfileStream = null;
        } catch (Exception e) {
            String str = "delete deleteBulderFromMap failed! groupID:"+rangGroupID;
            StorageService.instance.printInfo(str);
        }
        //outfileMap.remove(rangGroupID);
    }

    private void installSplitResults(CompactionState compact, String splitOrGCgroupID)
            throws IOException
    {       
        // Add split outputs
        int level = 0;
        List<String> groupIDList = new ArrayList<String>();
        //groupIDList.add(splitOrGCgroupID);
        StorageService.instance.printInfo("after addInputDeletions:"+compact.outputs.size()+", level:"+level);
        for (FileMetaData output : compact.outputs) {
            //compact.compaction.getEdit().addFile(level + 1, output);
        	String groupID = output.getGroupID();
        	if(!groupIDList.contains(groupID)) groupIDList.add(groupID);
        	VersionEdit groupEdit = compact.editMap.get(groupID);
            if(groupEdit == null ) {
            	VersionEdit edit = new VersionEdit();
            	compact.editMap.put(groupID, edit);
            	groupEdit = compact.editMap.get(groupID);
            }
            groupEdit.addFile(0, output);
        	//compact.compaction.getEdit(groupID).addFile(1, output);
            pendingOutputs.remove(output.getNumber());
        }
        //VersionSet versions = groupVersionSetMap.get(rangGroupID);
        //StorageService.instance.printInfo("groupIDList size:"+groupIDList.size());
        for(String curGroupID: groupIDList) {
	        //StorageService.instance.printInfo("before logAndApply,curGroupID:"+curGroupID);
	        groupVersionSetMap.get(curGroupID).logAndApply(compact.editMap.get(curGroupID), 0);	
            //StorageService.instance.printInfo("after logAndApply,curGroupID:"+curGroupID);				
        }
    }

    private void installCompactionResults(CompactionState compact, String splitOrGCgroupID)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());     
        //System.out.println("splitOrGCgroupID:" + splitOrGCgroupID);
        VersionEdit beginEdit = compact.compaction.getEdit(splitOrGCgroupID);
        if(beginEdit ==null ) {
        	VersionEdit edit = new VersionEdit();
        	compact.compaction.editMap.put(splitOrGCgroupID, edit);
        	beginEdit = compact.compaction.getEdit(splitOrGCgroupID);
        }
        compact.compaction.addInputDeletions(beginEdit);
        int level = compact.compaction.getLevel();
        StorageService.instance.printInfo("after addInputDeletions:"+compact.outputs.size());
        for (FileMetaData output : compact.outputs) {
        	String groupID = output.getGroupID();
        	VersionEdit groupEdit = compact.compaction.getEdit(groupID);
            if(groupEdit == null ) {
            	VersionEdit edit = new VersionEdit();
            	compact.compaction.editMap.put(groupID, edit);
            	groupEdit = compact.compaction.getEdit(groupID);
            }
            groupEdit.addFile(0, output);
        	//compact.compaction.getEdit(groupID).addFile(1, output);
            pendingOutputs.remove(output.getNumber());
        }

        StorageService.instance.printInfo("after logAndApply:"+splitOrGCgroupID);
        if(splitOrGCgroupID.equals(globalLogName)) {
                groupVersionSetMap.get(splitOrGCgroupID).logAndApply(compact.compaction.getEdit(splitOrGCgroupID), 1);			
                StorageService.instance.printInfo("after logAndApply,curGroupID:"+splitOrGCgroupID);
				deleteObsoleteFiles(splitOrGCgroupID, databaseDir);
                //StorageService.instance.printInfo("after deleteObsoleteFiles");
		}else {
				int nodeID = grouptoIDMap.get(splitOrGCgroupID);
				File replicaFile = replicasDirMap.get(nodeID);
		        String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + splitOrGCgroupID;                   	    
				File rangeGroupFile = new File(rangeGroupName); 
				//System.out.println("delete dir:"+rangeGroupFile);
				deleteObsoleteFiles(splitOrGCgroupID, rangeGroupFile);
                StorageService.instance.printInfo("delete group:"+rangeGroupFile);
		}
		//deleteObsoleteFiles();
    }

    private void installCompactionResultsForGC(CompactionState compact, String splitOrGCgroupID)
            throws IOException
    {
        checkState(mutex.isHeldByCurrentThread());
        //mutex.lock();
        // Add compaction outputs
        compact.compaction.addInputDeletions(compact.compaction.getEdit());
        int level = 0;
        for (FileMetaData output : compact.outputs) {
            compact.compaction.getEdit().addFile(level, output);
            pendingOutputs.remove(output.getNumber());
        }

        try {
            groupVersionSetMap.get(splitOrGCgroupID).logAndApply(compact.compaction.getEdit(), 0);

            int nodeID = grouptoIDMap.get(splitOrGCgroupID);
			File replicaFile = replicasDirMap.get(nodeID);
		    String rangeGroupName = replicaFile.getAbsoluteFile() + File.separator + "group" + splitOrGCgroupID;                   	    
			File rangeGroupFile = new File(rangeGroupName); 
			//System.out.println("delete dir:"+rangeGroupFile);
            deleteObsoleteFiles(splitOrGCgroupID, rangeGroupFile);
            StorageService.instance.printInfo("splitOrGCgroupID file number:"+groupVersionSetMap.get(splitOrGCgroupID).getCurrent().numberOfFilesInLevel(0));
        }
        catch (IOException e) {
            // Compaction failed for some reason.  Simply discard the work and try again later.

            // Discard any files we may have created during this failed compaction
            for (FileMetaData output : compact.outputs) {
                File file = new File(databaseDir, Filename.tableFileName(output.getNumber()));
                file.delete();
            }
            compact.outputs.clear();
        }
        //mutex.unlock();
    }

    int numberOfFilesInLevel(int level)
    {
        return groupVersionSetMap.get(globalLogName).getCurrent().numberOfFilesInLevel(level);
    }

    @Override
    public long[] getApproximateSizes(Range... ranges)
    {
        requireNonNull(ranges, "ranges is null");
        long[] sizes = new long[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            sizes[i] = getApproximateSizes(range);
        }
        return sizes;
    }

    public long getApproximateSizes(Range range)
    {
        //Version v = versions.getCurrent();
    	
    	Version v = groupVersionSetMap.get(globalLogName).getCurrent();
        InternalKey startKey = new InternalKey(Slices.wrappedBuffer(range.start()), MAX_SEQUENCE_NUMBER, VALUE);
        InternalKey limitKey = new InternalKey(Slices.wrappedBuffer(range.limit()), MAX_SEQUENCE_NUMBER, VALUE);
        long startOffset = v.getApproximateOffsetOf(startKey);
        long limitOffset = v.getApproximateOffsetOf(limitKey);

        return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
    }

    public long getMaxNextLevelOverlappingBytes()
    {
        return groupVersionSetMap.get(globalLogName).getMaxNextLevelOverlappingBytes();
    }

    public static class CompactionState
    {
        public final Compaction compaction;

        public final List<FileMetaData> outputs = new ArrayList<>();

        public Map<String, VersionEdit> editMap = new HashMap<String, VersionEdit>();

        public long smallestSnapshot;

        // State kept for output being generated
        private FileOutputStream outStream;
        private FileChannel outfile;
        private TableBuilder builder;
        // Current file being generated
        public long currentFileNumber;
        public Map<String, Long> currentFileNumberMap = new HashMap<String, Long>();
        public long currentFileSize;
        public InternalKey currentSmallest;
        public InternalKey currentLargest;

        public Map<String, InternalKey> currentSmallestMap = new HashMap<String, InternalKey>();
        public Map<String, InternalKey> currentLargestMap = new HashMap<String, InternalKey>();

        public long totalBytes;

        public CompactionState(Compaction compaction)
        {
            this.compaction = compaction;
            this.builder = null;
            this.outfile = null;
            this.outStream = null;
        }

        public Compaction getCompaction()
        {
            return compaction;
        }
    }

    private static class ManualCompaction
    {
        private final int level;
        private final Slice begin;
        private final Slice end;

        private ManualCompaction(int level, Slice begin, Slice end)
        {
            this.level = level;
            this.begin = begin;
            this.end = end;
        }
    }

    private WriteBatchImpl readWriteBatch(SliceInput record, int updateSize)
            throws IOException
    {
        WriteBatchImpl writeBatch = new WriteBatchImpl();
        int entries = 0;
        while (record.isReadable()) {
            entries++;
            ValueType valueType = ValueType.getValueTypeByPersistentId(record.readByte());
            if (valueType == VALUE) {
                Slice key = readLengthPrefixedBytes(record);
                Slice value = readLengthPrefixedBytes(record);
                writeBatch.put(key, value);
            }
            else if (valueType == DELETION) {
                Slice key = readLengthPrefixedBytes(record);
                writeBatch.delete(key);
            }
            else {
                throw new IllegalStateException("Unexpected value type " + valueType);
            }
        }

        if (entries != updateSize) {
            throw new IOException(String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
        }

        return writeBatch;
    }

    private Slice writeWriteBatch(WriteBatchImpl updates) {
	    Slice record = Slices.allocate(updates.getApproximateSize());
		final SliceOutput sliceOutput = record.output();
		updates.forEach(new Handler() {
			@Override
			public void put(Slice key, Slice value) {
				sliceOutput.writeByte(VALUE.getPersistentId());
				writeLengthPrefixedBytes(sliceOutput, key);
				writeLengthPrefixedBytes(sliceOutput, value);
			}

			@Override
			public void delete(Slice key) {
				sliceOutput.writeByte(DELETION.getPersistentId());
				writeLengthPrefixedBytes(sliceOutput, key);
			}
		});
		return record.slice(0, sliceOutput.size());
	}

    private Slice writeWriteBatch(WriteBatchImpl updates, long sequenceBegin)
    {
        Slice record = Slices.allocate(SIZE_OF_LONG + SIZE_OF_INT + updates.getApproximateSize());
        final SliceOutput sliceOutput = record.output();
        sliceOutput.writeLong(sequenceBegin);
        sliceOutput.writeInt(updates.size());
        updates.forEach(new Handler()
        {
            @Override
            public void put(Slice key, Slice value)
            {
                sliceOutput.writeByte(VALUE.getPersistentId());
                writeLengthPrefixedBytes(sliceOutput, key);
                writeLengthPrefixedBytes(sliceOutput, value);
            }

            @Override
            public void delete(Slice key)
            {
                sliceOutput.writeByte(DELETION.getPersistentId());
                writeLengthPrefixedBytes(sliceOutput, key);
            }
        });
        return record.slice(0, sliceOutput.size());
    }

    private static class InsertIntoHandler
            implements Handler
    {
        private long sequence;
        private final MemTable memTable;

        public InsertIntoHandler(MemTable memTable, long sequenceBegin)
        {
            this.memTable = memTable;
            this.sequence = sequenceBegin;
        }

        @Override
        public void put(Slice key, Slice value)
        {
            memTable.add(sequence++, VALUE, key, value);
        }

        @Override
        public void delete(Slice key)
        {
            memTable.add(sequence++, DELETION, key, Slices.EMPTY_SLICE);
        }
    }

    public static class DatabaseShutdownException
            extends DBException
    {
        public DatabaseShutdownException()
        {
        }

        public DatabaseShutdownException(String message)
        {
            super(message);
        }
    }

    public static class BackgroundProcessingException
            extends DBException
    {
        public BackgroundProcessingException(Throwable cause)
        {
            super(cause);
        }
    }

    private final Object suspensionMutex = new Object();
    private int suspensionCounter;

    @Override
    public void suspendCompactions()
            throws InterruptedException
    {
        compactionExecutor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    synchronized (suspensionMutex) {
                        suspensionCounter++;
                        suspensionMutex.notifyAll();
                        while (suspensionCounter > 0 && !compactionExecutor.isShutdown()) {
                            suspensionMutex.wait(500);
                        }
                    }
                }
                catch (InterruptedException e) {
                }
            }
        });
        synchronized (suspensionMutex) {
            while (suspensionCounter < 1) {
                suspensionMutex.wait();
            }
        }
    }

    @Override
    public void resumeCompactions()
    {
        synchronized (suspensionMutex) {
            suspensionCounter--;
            suspensionMutex.notifyAll();
        }
    }

    @Override
    public void compactRange(byte[] begin, byte[] end)
            throws DBException
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
