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

public class ByteBufferEncodingState
{
    ErasureEncoder encoder;
    int encodeLength;
    ByteBuffer[] inputs;
    ByteBuffer[] outputs;
    boolean usingDirectBuffer;
    int[] inputOffsets;
    int[] outputOffsets;

    ByteBufferEncodingState(ErasureEncoder encoder, ByteBuffer[] inputs, ByteBuffer[] outputs) {
        ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
        this.encodeLength = validInput.remaining();
        this.usingDirectBuffer = validInput.isDirect();
        this.encoder = encoder;
        this.inputs = inputs;
        this.outputs = outputs;

        CoderUtil.checkParameters(inputs, outputs, encoder.getNumDataUnits(), encoder.getNumParityUnits());
        CoderUtil.checkBuffers(inputs, this.encodeLength, this.usingDirectBuffer);
        CoderUtil.checkBuffers(outputs, this.encodeLength, this.usingDirectBuffer);

        this.inputOffsets = new int[inputs.length];
        this.outputOffsets = new int[outputs.length];
        ByteBuffer buffer;

        for (int i = 0; i < inputs.length; i++) {
            buffer = inputs[i];
            if (buffer.hasArray()) {
                inputOffsets[i] = buffer.arrayOffset() + buffer.position();
            } else {
                inputOffsets[i] = buffer.position();
            }
        }
        for (int i = 0; i < outputs.length; i++) {
            buffer = outputs[i];
            if (buffer.hasArray()) {
                outputOffsets[i] = buffer.arrayOffset() + buffer.position();
            } else {
                outputOffsets[i] = buffer.position();
            }
        }

    }
    ByteBufferEncodingState(ErasureEncoder encoder, int encodeLength,
                            ByteBuffer[] inputs, ByteBuffer[] outputs,
                            boolean usingDirectBuffer) {
        this.encoder = encoder;
        this.encodeLength = encodeLength;
        this.inputs = inputs;
        this.outputs = outputs;
        this.usingDirectBuffer = usingDirectBuffer;

        this.inputOffsets = new int[inputs.length];
        this.outputOffsets = new int[outputs.length];
        ByteBuffer buffer;

        for (int i = 0; i < inputs.length; i++) {
            buffer = inputs[i];
            if (buffer.hasArray()) {
                inputOffsets[i] = buffer.arrayOffset() + buffer.position();
            } else {
                inputOffsets[i] = buffer.position();
            }
        }
        for (int i = 0; i < outputs.length; i++) {
            buffer = outputs[i];
            if (buffer.hasArray()) {
                outputOffsets[i] = buffer.arrayOffset() + buffer.position();
            } else {
                outputOffsets[i] = buffer.position();
            }
        }

    }

    ByteArrayEncodingState convertToByteArrayState() {
       byte[][] newInputs = new byte[inputs.length][];
       byte[][] newOutputs = new byte[outputs.length][];

       ByteBuffer buffer;
       for (int i = 0; i < inputs.length; i++) {
           buffer = inputs[i];
           inputOffsets[i] = buffer.arrayOffset() + buffer.position();
           newInputs[i] = buffer.array();
       }

       for (int i = 0; i < outputs.length; i++) {
           buffer = outputs[i];
           outputOffsets[i] = buffer.arrayOffset() + buffer.position();
           newOutputs[i] = buffer.array();
       }

       ByteArrayEncodingState baeState = new ByteArrayEncodingState(encoder, encodeLength,
                                                                   newInputs, inputOffsets,
                                                                   newOutputs, outputOffsets);
       return baeState;
    }

}
