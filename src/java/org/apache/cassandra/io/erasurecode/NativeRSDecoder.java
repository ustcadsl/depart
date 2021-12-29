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

package org.apache.cassandra.io.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeRSDecoder extends ErasureDecoder
{
    private static Logger logger = LoggerFactory.getLogger(NativeRSEncoder.class.getName());

    // Protect ISA-L coder data structure in native layer from being accessed and
    // updated concurrently by the init, release and decode functions.
    protected final ReentrantReadWriteLock decoderLock = new ReentrantReadWriteLock();

    static {
        try {
            System.loadLibrary("ec");
            logger.debug("NativeRSDecoder - Loaded the native-erasurecode library");
        } catch (Throwable t) {
            logger.debug("NativeRSDecoder - Failed to load native-erasurecode with error: " + t);
            logger.debug("java.library.path=" + System.getProperty("java.library.path"));
        }
    }

    public NativeRSDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        decoderLock.writeLock().lock();
        try {
            initImpl(coderOptions.getNumDataUnits(),
                     coderOptions.getNumParityUnits());
        } finally {
            decoderLock.writeLock().unlock();
        }
    }

    protected void doDecode(ByteBufferDecodingState decodingState)
    throws IOException {
        decoderLock.readLock().lock();
        try {
            if (nativeCoder == 0) {
                throw new IOException(String.format("%s closed", getClass().getSimpleName()));
            }
            decodeImpl(decodingState.inputs, decodingState.inputOffsets,
                       decodingState.decodeLength, decodingState.erasedIndexes,
                       decodingState.outputs, decodingState.outputOffsets);
        } finally {
            decoderLock.readLock().unlock();
        }
    }

    protected void doDecode(ByteArrayDecodingState decodingState)
    throws IOException {
        ByteBufferDecodingState bbdState = decodingState.convertToByteBufferState();
        doDecode(bbdState);

        for (int i = 0; i < decodingState.outputs.length; i++) {
            bbdState.outputs[i].get(decodingState.outputs[i],
                                    decodingState.outputOffsets[i], decodingState.decodeLength);
        }
    }

    @Override
    public void release() {
        decoderLock.writeLock().lock();
        try {
            destroyImpl();
        } finally {
            decoderLock.writeLock().unlock();
        }
    }

    private native void initImpl(int numDataUnits, int numParityUnits);

    private native void decodeImpl(
    ByteBuffer[] inputs, int[] inputOffsets, int dataLen, int[] erased,
    ByteBuffer[] outputs, int[] outputOffsets) throws IOException;

    private native void destroyImpl();

    // To link with the underlying data structure in the native layer.
    // No get/set as only used by native codes.
    private long nativeCoder;
}
