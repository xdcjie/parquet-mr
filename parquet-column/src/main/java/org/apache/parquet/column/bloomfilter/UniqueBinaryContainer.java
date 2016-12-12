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

import org.apache.parquet.Log;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.column.UnknownColumnTypeException;

import java.util.HashSet;

/**
 * Created by baidu on 16/11/9.
 */
public class UniqueBinaryContainer<E> {
    private static final Log LOG = Log.getLog(UniqueBinaryContainer.class);

    private int insertedElements = 0;
    private HashSet uniqueElements;
    private BloomFilterProperties bloomFilterProperties = null;

    public UniqueBinaryContainer() {
        uniqueElements = new HashSet<E>();
    }

    public UniqueBinaryContainer(BloomFilterProperties bfProp) {
        this();
        bloomFilterProperties = bfProp;
    }

    /**
     * Returns the typed UniqueContainer object based on the passed type parameter
     * @param type PrimitiveTypeName type of the column
     * @return instance of a typed UniqueBinaryContainer class
     */
    public static UniqueBinaryContainer getContainerBasedOnType(PrimitiveTypeName type) {
        switch(type) {
            case INT32:
                return new UniqueBinaryContainer<Integer>();
            case INT64:
                return new UniqueBinaryContainer<Long>();
            case FLOAT:
                return new UniqueBinaryContainer<Float>();
            case DOUBLE:
                return new UniqueBinaryContainer<Double>();
            case BOOLEAN:
                return new UniqueBinaryContainer<Boolean>();
            case BINARY:
                return new UniqueBinaryContainer<Binary>();
            case INT96:
                return new UniqueBinaryContainer<Binary>();
            case FIXED_LEN_BYTE_ARRAY:
                return new UniqueBinaryContainer<Binary>();
            default:
                throw new UnknownColumnTypeException(type);
        }
    }

    public void setProperties(BloomFilterProperties bfProp) {
        this.bloomFilterProperties = bfProp;
    }

    /**
     * Returns the unique elements size
     * @return unique elements size
     */
    public int getUniqueSize() {
        return uniqueElements.size();
    }

    /**
     * Add a element
     * @param value the added value
     */
    public void add(E value) {
        if (bloomFilterProperties != null) {
            uniqueElements.add(value);
            insertedElements++;
        }
    }

    /**
     * Returns the bloom filter with unique elements.
     * @return bloom filter
     */
    public BloomFilter getBloomFilter() {
        if (bloomFilterProperties == null) {
            return null;
        }
        BloomFilter bloomFilter = null;
        if (insertedElements > 0 
                && insertedElements >= bloomFilterProperties.getValueCountThreshold()) {
            LOG.info(String.format(
                        "BloomFilter Info : %d unique elems, %f false positive.", 
                        uniqueElements.size(), bloomFilterProperties.getFalsePositive()));
            bloomFilter = new FilterBuilder(
                    uniqueElements.size(),
                    bloomFilterProperties.getFalsePositive())
                .buildBloomFilter();
            bloomFilter.addAll(uniqueElements);
        }
        resetBloomFilter();
        return bloomFilter;
    }

    /**
     * Clear the elements cache
     */
    public void resetBloomFilter() {
        insertedElements = 0;
        uniqueElements.clear();
    }
}
