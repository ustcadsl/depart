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

public abstract class ErasureCoder
{
    private final ErasureCoderOptions coderOptions;

    public ErasureCoder(ErasureCoderOptions coderOptions) {
        this.coderOptions = coderOptions;
    }

    /**
     * The number of data input units for the coding. A unit can be a byte, chunk
     * or buffer or even a block.
     * @return count of data input units
     */
    public int getNumDataUnits() {
        return this.coderOptions.getNumDataUnits();
    }

    /**
     * The number of parity output units for the coding. A unit can be a byte,
     * chunk, buffer or even a block.
     * @return count of parity output units
     */
    public int getNumParityUnits() {
        return this.coderOptions.getNumParityUnits();
    }

    /**
     * The number of data and parity units for the coding. A unit can be a byte,
     * chunk, buffer or even a block.
     * @return count of data and parity units
     */
    public int getNumAllUnits() {
        return this.coderOptions.getNumAllUnits();
    }

    /**
     * The options of erasure coder.
     * @return erasure coder options
     */
    public ErasureCoderOptions getOptions() {
        return this.coderOptions;
    }

    /**
     * Should be called When releasing this coder.
     * E.g., release some buffer.
     */
    public void release() {
        // Currently nothing to do here.
    }

    /**
     * Allow dump verbose debug info or not.
     * @return true if verbose debug info is desired, false otherwise
     */
    public boolean allowVerboseDump() {
        return this.coderOptions.allowVerboseDump();
    }
}
