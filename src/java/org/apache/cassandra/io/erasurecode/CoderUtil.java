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

import java.nio.ByteBuffer;

import org.apache.cassandra.exceptions.ErasureCodeException;

public final class CoderUtil
{
    /**
     * Find the valid input from all the inputs.
     * @param inputs input buffers to look for valid input
     * @return the first valid input
     */
    static<T> T findFirstValidInput(T[] inputs) {
        for (T input : inputs) {
            if (input != null) {
                return input;
            }
        }
        throw new ErasureCodeException("Invalid inputs, all being null!");
    }

    /**
     * Check and validate encoding parameters.
     * @param inputs input buffers to check
     * @param outputs output buffers to check
     * @param numDataUnits number of data units per stripe
     * @param numParityUnits number of parity units per stripe
     */
    static<T> void checkParameters(T[] inputs, T[] outputs, int numDataUnits, int numParityUnits) {
        if (inputs.length != numDataUnits) {
            throw new ErasureCodeException("Invalid inputs length for encoding!");
        }
        if (outputs.length != numParityUnits) {
            throw new ErasureCodeException("Invalid outputs length for encoding!");
        }
    }

    /**
     * Check and validate decoding parameters, throw exception accordingly. The
     * checking assumes it's a MDS code. Other code  can override this.
     * @param inputs input buffers to check
     * @param erasedIndexes indexes of erased units in the inputs array
     * @param outputs output buffers to check
     * @param numAllUnits number of all units per stripe (data + parity)
     * @param numParityUnits number of parity units per stripe
     */
    static<T> void checkParameters(T[] inputs, int[] erasedIndexes,
                             T[] outputs, int numAllUnits, int numParityUnits) {
        if (inputs.length != numAllUnits) {
            throw new ErasureCodeException("Invalid inputs length for decoding");
        }

        if (erasedIndexes.length != outputs.length) {
            throw new ErasureCodeException(
            "erasedIndexes and outputs mismatch in length");
        }

        if (erasedIndexes.length > numParityUnits) {
            throw new ErasureCodeException(
            "Too many erased, not recoverable");
        }
    }

    /**
     * Check and ensure the buffers are of the desired length and type (direct buffers or not).
     * @param buffers the buffers to check
     * @param encodeLength the length of unit to encode
     * @param usingDirectBuffer whether using direct ByteBuffer
     */
    static void checkBuffers(ByteBuffer[] buffers, int encodeLength, boolean usingDirectBuffer) {
        for (ByteBuffer buffer : buffers) {
            if (buffer == null) {
                throw new ErasureCodeException("Invalid buffer, being null");
            }

            if (buffer.remaining() != encodeLength) {
                throw new ErasureCodeException("Invalid buffer, not of length " + encodeLength);
            }

            if (buffer.isDirect() != usingDirectBuffer) {
                throw new ErasureCodeException("Invalid buffer, isDirect should be " + usingDirectBuffer);
            }
        }
    }

    /**
     * Check and ensure the buffer are of the desired length.
     * @param buffers the buffers to check
     * @param encodeLength the length of unit to encode
     */
    static void checkBuffers(byte[][] buffers, int encodeLength) {
        for (byte[] buffer : buffers) {
            if (buffer == null) {
                throw new ErasureCodeException("Invalid buffer, being null");
            }

            if (buffer.length != encodeLength) {
                throw new ErasureCodeException("Invalid buffer, not of length " + encodeLength);
            }
        }
    }

    /**
     * Clone an input byte array as direct ByteBuffer.
     * @param input the input byte array to clone from
     * @param offset the offset to start to clone
     * @param len the length of bytes to clone
     * @return
     */
    static ByteBuffer cloneAsDirectByteBuffer(byte[] input, int offset, int len) {
        if (input == null) {
            // An input can be null, if it is erased or not to read
            return null;
        }

        ByteBuffer directBuffer = ByteBuffer.allocateDirect(len);
        directBuffer.put(input, offset, len);
        directBuffer.flip();

        return directBuffer;
    }
}
