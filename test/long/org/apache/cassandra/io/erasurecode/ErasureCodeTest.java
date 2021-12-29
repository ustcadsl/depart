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
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErasureCodeTest
{
    private static Logger logger = LoggerFactory.getLogger(ErasureCodeTest.class.getName());

    @Test
    public void test() throws IOException {
        final int k = 6, m = 3;
        int codeLength = 1048576;
        Random random = new Random((long)123);

        // Generate encoder and decoder
        ErasureCoderOptions ecOptions = new ErasureCoderOptions(6, 3);
        ErasureEncoder encoder = new NativeRSEncoder(ecOptions);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);

        // Encoding input and output
        ByteBuffer[] data = new ByteBuffer[k];
        ByteBuffer[] parity = new ByteBuffer[m];

        // Decoding input and output
        ByteBuffer[] inputs = new ByteBuffer[k+m];
        int[] eraseIndexes = {0};
        ByteBuffer[] outputs = new ByteBuffer[1];

        // Prepare inputs for encoding
        byte[] tmpArray = new byte[codeLength];
        for (int i = 0; i < k; i++) {
            data[i] = ByteBuffer.allocateDirect(codeLength);
            random.nextBytes(tmpArray);
            data[i].put(tmpArray);
            data[i].rewind();
        }
        // Prepare outputs for encoding
        for (int i = 0; i < m; i++) {
            parity[i] = ByteBuffer.allocateDirect(codeLength);
        }

        // Encode
        logger.debug("ErasureCodeTest - next to call encode()!");
        encoder.encode(data, parity);

        // Prepare inputs for decoding
        inputs[0] = null;
        for (int i = 1; i < k; i++) {
            data[i].rewind();
            inputs[i] = data[i];
            logger.debug("inputs["+ i + "]: position() = " + inputs[i].position()
                         + ", remaining() = " + inputs[i].remaining());
        }
        inputs[k+0] = parity[0];
        for (int i = k+1; i < k+m; i++) {
            inputs[i] = null;
        }

        // Prepare outputs for decoding
        outputs[0] = ByteBuffer.allocateDirect(codeLength);

        // Decode
        logger.debug("ErasureCodeTest - next to call decode()!");
        decoder.decode(inputs, eraseIndexes, outputs);

        data[0].rewind();
        if (outputs[0].compareTo(data[0]) == 0) {
            logger.debug("ErasureCodeTest - decoding Succeeded, same recovered data!");
        } else {
            logger.debug("ErasureCodeTest - decoding Failed, diffent recovered data ");
        }

        encoder.release();
        decoder.release();
    }

}
