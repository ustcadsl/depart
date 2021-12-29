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

public class ByteArrayEncodingState
{
    ErasureEncoder encoder;
    int encodeLength;
    byte[][] inputs;
    byte[][] outputs;
    int[] inputOffsets;
    int[] outputOffsets;

    ByteArrayEncodingState(ErasureEncoder encoder, byte[][] inputs, byte[][] outputs) {
        byte[] valideInput = CoderUtil.findFirstValidInput(inputs);
        this.encodeLength = valideInput.length;

        CoderUtil.checkParameters(inputs, outputs, encoder.getNumDataUnits(), encoder.getNumParityUnits());
        CoderUtil.checkBuffers(inputs, this.encodeLength);
        CoderUtil.checkBuffers(outputs, this.encodeLength);

        this.encoder = encoder;
        this.inputs = inputs;
        this.outputs = outputs;
        this.inputOffsets = new int[inputs.length];
        this.outputOffsets = new int[outputs.length];
    }

    ByteArrayEncodingState(ErasureEncoder encoder, int encodeLength,
                           byte[][] inputs, int[] inputsOffset,
                           byte[][] outputs, int[] outputOffsets) {
        this.encoder = encoder;
        this.encodeLength = encodeLength;
        this.inputs = inputs;
        this.outputs = outputs;
        this.inputOffsets = inputsOffset;
        this.outputOffsets = outputOffsets;
    }

    ByteBufferEncodingState convertToByteBufferState() {
        ByteBuffer[] newInputs = new ByteBuffer[inputs.length];
        ByteBuffer[] newOutputs = new ByteBuffer[outputs.length];

        for (int i = 0; i < inputs.length; i++) {
            newInputs[i] = CoderUtil.cloneAsDirectByteBuffer(inputs[i], inputOffsets[i], encodeLength);
        }

        for (int i = 0; i < outputs.length; i++) {
            newOutputs[i] = ByteBuffer.allocateDirect(encodeLength);
        }
        ByteBufferEncodingState bbeState = new ByteBufferEncodingState(encoder, encodeLength,
                                                                       newInputs, newOutputs,
                                                                       true);
        return bbeState;
    }


}
