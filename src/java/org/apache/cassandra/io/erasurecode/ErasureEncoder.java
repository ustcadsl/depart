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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ErasureEncoder extends ErasureCoder
{
    private static Logger logger = LoggerFactory.getLogger(ErasureEncoder.class.getName());

    public ErasureEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
    }

    /**
     * Encode with inputs and generates outputs
     */
    public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs)
    throws IOException {
        ByteBufferEncodingState bbestate = new ByteBufferEncodingState(this, inputs, outputs);
        boolean usingDirectBuffer = bbestate.usingDirectBuffer;
        int dataLen = bbestate.encodeLength;
        if (dataLen == 0) {
            return;
        }

        // Figure out inputs' position
        int[] inputPositions = new int[inputs.length];
        for (int i = 0; i < inputPositions.length; i++) {
            if (inputs[i] != null) {
                inputPositions[i] = inputs[i].position();
            }
        }

        // Perform encoding
        if (usingDirectBuffer) {
            doEncode(bbestate);
        } else {
            ByteArrayEncodingState baeState = bbestate.convertToByteArrayState();
            doEncode(baeState);
        }

        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] != null) {
                // update inputs' positon with the amount of bytes consumed
                inputs[i].position(inputPositions[i] + dataLen);
            }
        }
    }

    public void encode(byte[][] inputs, byte[][] outputs) throws IOException {
        ByteArrayEncodingState baeState = new ByteArrayEncodingState(this, inputs, outputs);
        if (baeState.encodeLength == 0) {
            return;
        }

        doEncode(baeState);
    }

    /**
     * Perform the real encoding using direct bytebuffer.
     * @param encodingState, the encoding state.
     * @throws IOException
     */
    protected abstract void doEncode(ByteBufferEncodingState encodingState) throws IOException;

    /**
     * Perform the real encoding using byte array, supporting offsets and lengths.
     * @param encodingState, the encoding state.
     * @throws IOException
     */
    protected abstract void doEncode(ByteArrayEncodingState encodingState) throws IOException;
}
