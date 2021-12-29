/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;

import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.utils.JVMStabilityInspector;
import static org.apache.cassandra.utils.Throwables.extractIOExceptionCause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.util.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.net.InetAddress;
import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.nio.ByteBuffer;

import org.iq80.twoLayerLog.impl.DbImpl;
import org.iq80.twoLayerLog.util.Slice;
import org.iq80.twoLayerLog.util.SliceInput;
import org.iq80.twoLayerLog.util.SliceOutput;
import org.iq80.twoLayerLog.util.Slices;
import org.iq80.twoLayerLog.util.VariableLengthQuantity;
import org.iq80.twoLayerLog.impl.ValueType;
import static org.iq80.twoLayerLog.impl.ValueType.DELETION;
import static org.iq80.twoLayerLog.impl.ValueType.VALUE;
import static org.iq80.twoLayerLog.util.SizeOf.SIZE_OF_INT;
import static org.iq80.twoLayerLog.util.SizeOf.SIZE_OF_LONG;
import static org.iq80.twoLayerLog.util.Slices.readLengthPrefixedBytes;
import static org.iq80.twoLayerLog.util.Slices.writeLengthPrefixedBytes;
import org.apache.commons.lang3.ArrayUtils;

/**
 * IncomingReplicaFileMessage is used to receive the part(or whole) of a ECBlock data file.
 */

public class IncomingReplicaFileMessage extends StreamMessage
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingReplicaFileMessage.class);
    public static Serializer<IncomingReplicaFileMessage> serializer = new Serializer<IncomingReplicaFileMessage>()
    {
        @SuppressWarnings("resource")
        public IncomingReplicaFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputPlus input = new DataInputStreamPlus(Channels.newInputStream(in));
            IncomingReplicaFileMessage FileMessage= null;
            int migrationFlag = input.readInt();//////
            UUID cfId = UUIDSerializer.serializer.deserialize(input, MessagingService.current_version);
            int sequenceNumber = input.readInt();
            int GroupBufferListSize= input.readInt();
            logger.debug("IncomingReplicaFileMessage migrationFlag:{}, GroupBufferListSize:{}", migrationFlag, GroupBufferListSize);
            byte ip[] = session.peer.getAddress();  
            int nodeID = (int)ip[3];
            for(int i=0; i< GroupBufferListSize; i++){
                int replicaFileSize = input.readInt();
                logger.debug("cfId:{}, sequenceNumber:{}, replicaFileSize:{}, nodeID:{}",cfId, sequenceNumber, replicaFileSize, nodeID);
                if(replicaFileSize>0){
                    recieveReplicaFileAndRecord(in, replicaFileSize, nodeID);       
                }     
            }
            FileMessage=new IncomingReplicaFileMessage(cfId, sequenceNumber);
            return FileMessage;
        }

        public void recieveReplicaFileAndRecord(ReadableByteChannel in, int replicaFileSize, int nodeID){
            
            BlockingQueue<ByteBuffer> nodeReplicaQueue = StorageService.instance.replicaBlockMap.get(nodeID);
            if(nodeReplicaQueue==null){
                nodeReplicaQueue = new ArrayBlockingQueue<ByteBuffer>(20);
                ByteBuffer rowBytesBuffer = ByteBuffer.allocateDirect((int)(StorageService.instance.replicaBufferSize*1.1)); 
                try{
                    nodeReplicaQueue.put(rowBytesBuffer);
                    StorageService.instance.replicaBlockMap.put(nodeID, nodeReplicaQueue);
                }catch(Throwable e){
                    logger.debug("add nodeReplicaQueue failed!!");
                }
                logger.debug("add to replicaBlockMap, size:{}", StorageService.instance.replicaBlockMap.size());
            }

            ByteBuffer replicaFileBuffer = nodeReplicaQueue.poll();
            if(replicaFileBuffer==null){
                logger.debug("build new replicaFileBuffer");
                replicaFileBuffer = ByteBuffer.allocateDirect((int)(StorageService.instance.replicaBufferSize*1.1));   
            }else{
                replicaFileBuffer.clear();
            }

            TrackedInputStream ECBlockIn = new TrackedInputStream(Channels.newInputStream(in));
            int totalRead = 0;
            IOException readException = null;
            int r = 0;
            ////////////////////////recieve replicaFile data from network////////////////
            //int readLength = 8388608;//
            int readLength = 1048576;
            BlockingQueue<byte[]> nodeECBlockQueue = StorageService.instance.ECBlockMap.get(nodeID);
            if(nodeECBlockQueue==null){
                nodeECBlockQueue = new ArrayBlockingQueue<byte[]>(20);
                byte[] rowBytes = new byte[readLength];
                try{
                    nodeECBlockQueue.put(rowBytes);
                    StorageService.instance.ECBlockMap.put(nodeID, nodeECBlockQueue);
                }catch(Throwable e){
                    logger.debug("add nodeECBlockQueue failed!!");
                }
                logger.debug("add to ECBlockMap, size:{}", StorageService.instance.ECBlockMap.size());
            }
            byte[] ECBlock = nodeECBlockQueue.poll();
            if(ECBlock==null){
                logger.debug("build new ECBlock");
                ECBlock = new byte[readLength];               
            }

            BlockingQueue<byte[]> nodeKeyQueue = StorageService.instance.keyMap.get(nodeID);
            BlockingQueue<byte[]> nodeValueQueue = StorageService.instance.valueMap.get(nodeID);
            if(nodeKeyQueue==null){
                nodeKeyQueue = new ArrayBlockingQueue<byte[]>(20);
                nodeValueQueue = new ArrayBlockingQueue<byte[]>(20);
                byte[] key = new byte[50];
                byte[] value = new byte[65536];//
                try{
                    nodeKeyQueue.put(key);
                    nodeValueQueue.put(value);
                    StorageService.instance.keyMap.put(nodeID, nodeKeyQueue);
                    StorageService.instance.valueMap.put(nodeID, nodeValueQueue);
                }catch(Throwable e){
                    logger.debug("add nodeKeyQueue failed!!");
                }
                logger.debug("add to keyMap, size:{}", StorageService.instance.keyMap.size());
            }
            
            byte[] key = nodeKeyQueue.poll();
            if(key==null){
                logger.debug("build new key");
                key = new byte[50];               
            }
            byte[] value = nodeValueQueue.poll();
            if(value==null){
                logger.debug("build new value");
                value = new byte[65536];//        
            }
            
            while(totalRead < replicaFileSize){            
                int bufferRead = 0;
                if(replicaFileSize-totalRead < readLength) readLength = replicaFileSize-totalRead;
                //byte[] ECBlock = new byte[readLength]; 
                while (bufferRead < readLength){
                    try{
                        r = ECBlockIn.read(ECBlock, bufferRead, readLength - bufferRead);
                        if (r < 0){
                            readException = new EOFException("No chunk available");                         
                        }
                        if(r==0) break;
                        bufferRead += r;
                        totalRead += r;
                        if(totalRead == replicaFileSize) break;
                    }catch (IOException e){
                        logger.warn("Error while reading compressed input stream.", e);
                    }
                }                         
                try{
                    //logger.debug("before ECBlockQueue put, size:{}, totalRead:{}, ECBlockSize:{}, r:{}",ECBlockQueue.size(),totalRead, ECBlockSize, r);                     
                    //replicaFileBuffer.put(ArrayUtils.subarray(ECBlock,0,readLength));
                    replicaFileBuffer.put(ECBlock, 0, readLength);
                }catch (Throwable t){
                    logger.warn("Error while add to ECBlock queue.", t);
                }
                if(r==0) break;
            }
            replicaFileBuffer.flip(); 
            logger.debug("replicaFileBuffer position:{}, replicaFileBuffer.limit:{}", replicaFileBuffer.position(), replicaFileBuffer.limit());
                
            InetAddress LOCAL = FBUtilities.getBroadcastAddress();
            int batchBytesSize = 0, entries = 0;
            long beginTime = System.currentTimeMillis();
            while(replicaFileBuffer.position() < replicaFileBuffer.limit()){           
                ValueType valueType = ValueType.getValueTypeByPersistentId(replicaFileBuffer.get());
                if (valueType == VALUE) {										  
                    int keyLength = VariableLengthQuantity.readVariableLengthInt(replicaFileBuffer);
                    //byte[] dst = new byte[length]; 
                    replicaFileBuffer.get(key, 0, keyLength);
                    int valueLength = VariableLengthQuantity.readVariableLengthInt(replicaFileBuffer);         
                    replicaFileBuffer.get(value, 0, valueLength);
                       
                    batchBytesSize += keyLength;
                    batchBytesSize += valueLength;
                    try{
                        Mutation remutation = Mutation.serializer.deserializeToMutation(new DataInputBuffer(value,0,valueLength), MessagingService.current_version);                          
                        Keyspace ks = Keyspace.open(remutation.getKeyspaceName());
                        //ks.apply(remutation, true, true, true);
                        ks.apply(remutation, false, true, true); //disable commitlog
                        //logger.debug("keyToken:{}, strKey1:{} is stored in SSTable, main copy ID:{}", theKey, strKey1, ID);
                   }catch(Throwable e){
                       logger.debug("Mutation.serializer.deserialize failed in recieveReplicaFileAndRecord!");
              		}

                } else if (valueType == DELETION) {				
                    int keyLength = VariableLengthQuantity.readVariableLengthInt(replicaFileBuffer);                 
                    replicaFileBuffer.get(key, 0, keyLength);                        
                    batchBytesSize += keyLength;
                } else {
                    throw new IllegalStateException("Unexpected value type " + valueType);
                }
                entries++;
            }

            long endTime = System.currentTimeMillis();
            StorageService.instance.recieveWriteData[nodeID]+= endTime-beginTime;
            logger.debug("totalSegemntSize:{}, entries:{}", batchBytesSize, entries);
            try{
                nodeReplicaQueue.put(replicaFileBuffer);
                nodeECBlockQueue.put(ECBlock);
                nodeKeyQueue.put(key);
                nodeValueQueue.put(value);
            }catch(Throwable e){
                logger.debug("add nodeReplicaQueue2 failed!!");
            }
            logger.debug("add to nodeReplicaQueue, size:{}, nodeKeyQueue size:{}, nodeValueQueue size:{}", nodeReplicaQueue.size(), nodeKeyQueue.size(), nodeValueQueue.size());   
        }

        public void serialize(IncomingReplicaFileMessage message, DataOutputStreamPlus out, int version, StreamSession session)
        {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming file");
        }
    };

    public UUID cfId;//
    public int sequenceNumber;//

    public IncomingReplicaFileMessage(UUID cfId, int sequenceNumber)
    {
        super(Type.REPLICAFILE);
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString()
    {
        return "REPLICAFILE (" + cfId + ", sequenceNumber: " + sequenceNumber + ")";
    }
}

