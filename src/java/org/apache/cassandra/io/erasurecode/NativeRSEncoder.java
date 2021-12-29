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

public class NativeRSEncoder extends ErasureEncoder
{
    private static Logger logger = LoggerFactory.getLogger(NativeRSEncoder.class.getName());

    // Protect ISA-L coder data structure in native layer from being accessed and
    // updated concurrectly by the init, encode function
    final ReentrantReadWriteLock encoderLock = new ReentrantReadWriteLock();

    static {
        try {
            System.loadLibrary("ec");
            logger.debug("NativeRSEncoder - Loaded the native-erasurecode library");
        } catch (Throwable t) {
            logger.debug("NativeRSEncoder - Failed to load native-erasurecode with error: " + t);
            logger.debug("java.library.path=" + System.getProperty("java.library.path"));
        }
    }

    public NativeRSEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        encoderLock.writeLock().lock();
        try {
            initImpl(coderOptions.getNumDataUnits(), coderOptions.getNumParityUnits());
        } finally {
            encoderLock.writeLock().unlock();
        }

    }

    protected void doEncode(ByteBufferEncodingState bbeState) throws IOException {
        encoderLock.readLock().lock();
        try {
            if (nativeCoder == 0) {
                throw new IOException(String.format("%s closed", getClass().getSimpleName()));
            }
            encodeImpl(bbeState.inputs, bbeState.inputOffsets, bbeState.encodeLength,
                       bbeState.outputs, bbeState.outputOffsets);
        } finally {
            encoderLock.readLock().unlock();
        }
    }

    protected void doEncode(ByteArrayEncodingState baeState) throws IOException {
        ByteBufferEncodingState bbeState = baeState.convertToByteBufferState();
        doEncode(bbeState);

        for (int i = 0; i < baeState.outputs.length; i++) {
            bbeState.outputs[i].get(baeState.outputs[i], baeState.outputOffsets[i],
                                    baeState.encodeLength);
        }
    }

    @Override
    public void release() {
        encoderLock.writeLock().lock();
        try {
            destroyImpl();
        } finally {
            encoderLock.writeLock().unlock();
        }
    }

    // To link with the underlying data structure in the native layer.
    // No get/set as only used by native codes.
    private long nativeCoder;

    private native void initImpl(int numDataUnits, int numParityUnits);
    private native void encodeImpl(ByteBuffer[] inputs, int[] inputOffsets, int dataLen,
                                   ByteBuffer[] outputs, int[] outputOffsets) throws IOException;
    private native void destroyImpl();
}
