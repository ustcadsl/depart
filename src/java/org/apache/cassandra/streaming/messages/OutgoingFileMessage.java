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
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.streaming.compress.CompressedStreamWriter;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import org.apache.cassandra.utils.UUIDSerializer;
import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import sun.nio.ch.DirectBuffer;
import java.net.InetAddress;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import org.iq80.twoLayerLog.impl.*;

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
public class OutgoingFileMessage extends StreamMessage
{
    private static final Logger logger = LoggerFactory.getLogger(OutgoingFileMessage.class);

    public static Serializer<OutgoingFileMessage> serializer = new Serializer<OutgoingFileMessage>()
    {
        public OutgoingFileMessage deserialize(ReadableByteChannel in, int version, StreamSession session)
        {
            throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing file");
        }

        public void serialize(OutgoingFileMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            message.startTransfer();
            try
            {
                out.writeInt(message.migrationFlag);//////
                //logger.debug("OutgoingFileMessage migrationFlag:{}", message.migrationFlag);
                if(message.type == Type.FILE){ //serialize sstable file
                    message.serialize(out, version, session);
                    session.fileSent(message.header);
                }
                //////////////////////////////////////////
                if(message.type == Type.REPLICAFILE){ //serialize replica file
                    List<ByteBuffer> rangeGroupBufferList = new ArrayList<ByteBuffer>();
                    rangeGroupBufferList.clear();
                    UUIDSerializer.serializer.serialize(message.cfId, out, version);
                    out.writeInt(message.sequenceNumber);
                    //int blockSize = message.replicaFileData.getInt();
                    //ByteBuffer rangeGroupBuffer = StorageService.instance.getGroupAllBytes(message.keyspace, message.NodeID, message.right, message.rightBound, message.fileMeta, message.globalFlag, rangeGroupBufferList);/////////
                    StorageService.instance.db.getGroupAllBytes(message.keyspace, message.NodeID, StorageService.instance.getTokenFactory().toString(message.left), StorageService.instance.getTokenFactory().toString(message.right), StorageService.instance.getTokenFactory().toString(message.rightBound), message.fileMeta, rangeGroupBufferList);/////////
                    logger.debug("rangeGroupBufferList size:{}", rangeGroupBufferList.size());
                    //if(rangeGroupBuffer!=null){
                    if(rangeGroupBufferList.size() > 0){
                        out.writeInt(rangeGroupBufferList.size());
                        for(int i=0; i< rangeGroupBufferList.size(); i++){
                            //int blockSize = rangeGroupBuffer.limit();
                            int blockSize = rangeGroupBufferList.get(i).limit();
                            logger.debug("message.type:{},replicaFileData position:{}, replicaFileData limit:{}, message.cfId:{},message.NodeID:{}, blockSize:{}, message.rightBound:{}",message.type, rangeGroupBufferList.get(i).position(),rangeGroupBufferList.get(i).limit(),message.cfId,message.NodeID, blockSize, message.rightBound);
                            out.writeInt(blockSize);                     
                            //out.write(rangeGroupBuffer);
                            out.write(rangeGroupBufferList.get(i));
                        }
                        StorageService.instance.transferingFiles--;
                        rangeGroupBufferList.clear();
                    }else{
                        out.writeInt(1);
                        out.writeInt(0);  
                    }
                }
                //////////////////////////////////////////

            }
            finally
            {
                message.finishTransfer();
            }
        }
    };

    public final FileMessageHeader header;
    private final Ref<SSTableReader> ref;
    private final String filename;
    private boolean completed = false;
    private boolean transferring = false;

    private ByteBuffer replicaFileData;
    public int migrationFlag = 0;
    public UUID cfId;
    public int sequenceNumber;

    public int NodeID;
    public Token left;
    public Token right;
    public Token rightBound;
    public FileMetaData fileMeta;
    public boolean globalFlag;
    public String keyspace;

    public OutgoingFileMessage(Ref<SSTableReader> ref, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt, boolean keepSSTableLevel)
    {
        super(Type.FILE);
        this.ref = ref;

        SSTableReader sstable = ref.get();
        this.sequenceNumber = sequenceNumber;
        this.cfId = sstable.metadata.cfId;
        this.migrationFlag = 0;
        
        filename = sstable.getFilename();
        this.header = new FileMessageHeader(sstable.metadata.cfId,
                                            sequenceNumber,
                                            sstable.descriptor.version,
                                            sstable.descriptor.formatType,
                                            estimatedKeys,
                                            sections,
                                            sstable.compression ? sstable.getCompressionMetadata() : null,
                                            repairedAt,
                                            keepSSTableLevel ? sstable.getSSTableLevel() : 0,
                                            sstable.header == null ? null : sstable.header.toComponent());
    }

    public OutgoingFileMessage(Ref<SSTableReader> ref, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt, boolean keepSSTableLevel, int migrationFlag)
    {
        super(Type.FILE);
        this.ref = ref;
        this.migrationFlag = migrationFlag;
        SSTableReader sstable = ref.get();
        filename = sstable.getFilename();
        this.sequenceNumber = sequenceNumber;
        this.cfId = sstable.metadata.cfId;
        this.header = new FileMessageHeader(sstable.metadata.cfId,
                                            sequenceNumber,
                                            sstable.descriptor.version,
                                            sstable.descriptor.formatType,
                                            estimatedKeys,
                                            sections,
                                            sstable.compression ? sstable.getCompressionMetadata() : null,
                                            repairedAt,
                                            keepSSTableLevel ? sstable.getSSTableLevel() : 0,
                                            sstable.header == null ? null : sstable.header.toComponent());
    }

    public OutgoingFileMessage(int sequenceNumber, UUID cfId, int NodeID, Token left, Token right, Token rightBound, FileMetaData fileMeta, boolean globalFlag, String keyspace)
    { 
        super(Type.REPLICAFILE);
        this.migrationFlag = 0;
        this.sequenceNumber = sequenceNumber;
        this.replicaFileData = null;       
        this.ref = null;
        this.header = null;
        this.filename="replicaFileData";
        this.cfId = cfId;

        this.NodeID = NodeID;
        this.left = left;
        this.right = right;
        this.rightBound = rightBound;
        this.fileMeta = fileMeta;
        this.globalFlag = globalFlag;
        this.keyspace = keyspace;
      
    }  

    public synchronized void serialize(DataOutputStreamPlus out, int version, StreamSession session) throws IOException
    {
        if (completed)
        {
            return;
        }

        CompressionInfo compressionInfo = FileMessageHeader.serializer.serialize(header, out, version);

        final SSTableReader reader = ref.get();
        StreamWriter writer = compressionInfo == null ?
                                      new StreamWriter(reader, header.sections, session) :
                                      new CompressedStreamWriter(reader, header.sections,
                                                                 compressionInfo, session);
        writer.write(out);
    }

    @VisibleForTesting
    public synchronized void finishTransfer()
    {
        transferring = false;
        //session was aborted mid-transfer, now it's safe to release
        if (completed && ref!=null)//&& ref!=null
        {
            ref.release();
        }
    }

    @VisibleForTesting
    public synchronized void startTransfer()
    {
        if (completed)
            throw new RuntimeException(String.format("Transfer of file %s already completed or aborted (perhaps session failed?).",
                                                     filename));
        transferring = true;
    }

    public synchronized void complete()
    {
        if (!completed)
        {
            completed = true;
            //release only if not transferring
            if (!transferring && ref!=null)//&& ref!=null
            {
                ref.release();
            }
        }
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + filename + ")";
    }
}

