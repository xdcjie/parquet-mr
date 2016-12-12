/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.bloomfilter;

/**
 * Created by baidu on 16/11/23.
 */
import org.apache.parquet.column.bloomfilter.BloomFilter;
import org.apache.parquet.column.bloomfilter.FilterBuilder;

import org.apache.parquet.Log;
import java.util.BitSet;

public class BloomFilterMemory<T> extends BloomFilter<T> {
    private static final Log LOG = Log.getLog(BloomFilterMemory.class);
    private static final long serialVersionUID = -5962895807963888856L;
    private final FilterBuilder config;
    protected BitSet bloom;

    public BloomFilterMemory(FilterBuilder config) {
        config.complete();
        bloom = new BitSet(config.getBits());
        this.config = config;
        setByteLength(getByteSize());
    }

    public BloomFilterMemory(FilterBuilder config, long fileOffset, long byteLength) {
        config.complete();
        bloom = new BitSet(0);
        this.config = config;
        setFileOffset(fileOffset);
        setByteLength(byteLength);
    }

    @Override
    public FilterBuilder config() {
        return config;
    }

    @Override
    public synchronized boolean addRaw(byte[] element) {
        boolean added = false;
        for (int position : hash(element)) {
            if (!getBit(position)) {
                added = true;
                setBit(position, true);
            }
        }
        return added;
    }

    @Override
    public synchronized void clear() {
        bloom.clear();
    }

    @Override
    public synchronized boolean contains(byte[] element) {
        for (int position : hash(element)) {
            if (!getBit(position)) {
                return false;
            }
        }
        return true;
    }

    protected boolean getBit(int index) {
        return bloom.get(index);
    }

    protected void setBit(int index, boolean to) {
        bloom.set(index, to);
    }

    @Override
    public BitSet getBitSet() {
        return bloom;
    }

    @Override
    public byte[] getBytes() {
        return bloom.toByteArray();
    }

    @Override
    public synchronized boolean union(BloomFilter<T> other) {
        if (compatible(other)) {
            bloom.or(other.getBitSet());
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean intersect(BloomFilter<T> other) {
        if (compatible(other)) {
            bloom.and(other.getBitSet());
            return true;
        }
        return false;
    }


    @Override
    public synchronized boolean isEmpty() {
        return bloom.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized BloomFilter<T> clone() {
        BloomFilterMemory<T> o = new BloomFilterMemory<T>(config);
        o.bloom = (BitSet) bloom.clone();
        return o;
    }

    @Override
    public int getByteSize() {
        return (int) Math.ceil(bloom.length() / 8.0);
    }

    @Override
    public synchronized String toString() {
        return asString();
    }

    @Override
    public synchronized void setBitSet(BitSet bloom) {
        this.bloom = bloom;
    }

    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BloomFilterMemory)) {
            return false;
        }

        BloomFilterMemory that = (BloomFilterMemory) o;

        if (bloom != null ? !bloom.equals(that.bloom) : that.bloom != null) {
            return false;
        }
        if (config != null ? !config.isCompatibleTo(that.config) : that.config != null) {
            return false;
        }

        return true;
    }

}
