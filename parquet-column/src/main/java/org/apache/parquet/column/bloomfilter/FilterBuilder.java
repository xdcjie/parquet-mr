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

import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * Builder for Bloom Filters.
 */
public class FilterBuilder implements Cloneable, Serializable {
    private Integer expectedElements;
    private Integer bits;
    private Integer hashes;
    private Double falsePositiveProbability;
    private HashProvider.HashMethod hashMethod =
            new HashProvider.HashMethod(new HashProvider.Murmur3());
    private static transient Charset defaultCharset = Charset.forName("UTF-8");
    private boolean done = false;

    /**
     * Constructs a new builder for Bloom filters.
     */
    public FilterBuilder() {
    }

    /**
     * Constructs a new Bloom Filter Builder by specifying the expected size of the filter and the tolerable false
     * positive probability. The size of the BLoom filter in in bits and the optimal number of hash functions will be
     * inferred from this.
     *
     * @param expectedElements         expected elements in the filter
     * @param falsePositiveProbability tolerable false positive probability
     */
    public FilterBuilder(int expectedElements, double falsePositiveProbability) {
        this.expectedElements(expectedElements).setFalsePositiveProbability(falsePositiveProbability);
    }

    /**
     * Constructs a new Bloom Filter Builder using the specified size in bits and the specified number of hash
     * functions.
     *
     * @param size   bit size of the Bloom filter
     * @param hashes number of hash functions to use
     */
    public FilterBuilder(int size, int hashes) {
        this.setBits(size).setHashes(hashes);
    }

    /**
     * Sets the number of expected elements. In combination with the tolerable false positive probability, this is used
     * to infer the optimal size and optimal number of hash functions of the filter.
     *
     * @param expectedElements number of expected elements.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder expectedElements(int expectedElements) {
        this.expectedElements = expectedElements;
        return this;
    }

    /**
     * Sets the size of the filter in bits.
     *
     * @param size size of the filter in bits
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder setBits(int size) {
        this.bits = size;
        return this;
    }

    /**
     * Sets the tolerable false positive probability. In combination with the number of expected elements, this is used
     * to infer the optimal size and optimal number of hash functions of the filter.
     *
     * @param falsePositiveProbability the tolerable false
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder setFalsePositiveProbability(double falsePositiveProbability) {
        this.falsePositiveProbability = falsePositiveProbability;
        return this;
    }

    /**
     * Set the number of hash functions to be used.
     *
     * @param numberOfHashes number of hash functions used by the filter.
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder setHashes(int numberOfHashes) {
        this.hashes = numberOfHashes;
        return this;
    }

    /**
     * Sets the method used to generate hash values. Possible hash methods are documented in the corresponding enum
     * {@link HashProvider.HashMethod}. <p><b>Default</b>: MD5</p>
     * <p>
     * For the generation of hash values the String representation of objects is used.
     *
     * @param hashMethod the method used to generate hash values
     * @return the modified FilterBuilder (fluent interface)
     */
    public FilterBuilder setHashMethod(HashProvider.HashMethod hashMethod) {
        this.hashMethod = hashMethod;
        return this;
    }

    /**
     * Constructs a Bloom filter using the specified parameters and computing missing parameters if possible (e.g. the
     * optimal Bloom filter bit size).
     *
     * @param <T> the type of element contained in the Bloom filter.
     * @return the constructed Bloom filter
     */
    public <T> BloomFilter<T> buildBloomFilter() {
        complete();
        return new BloomFilterMemory<T>(this);
    }

    public <T> BloomFilter<T> buildBloomFilter(long offset, long length) {
        complete();
        return new BloomFilterMemory<T>(this, offset, length);
    }


    /**
     * Checks if all necessary parameters were set and tries to infer optimal parameters (e.g. size and hashes from
     * given expectedElements and falsePositiveProbability). This is done automatically.
     *
     * @return the completed FilterBuilder
     */
    public FilterBuilder complete() {
        if (done) { return this; }
        if (bits == null && expectedElements != null && falsePositiveProbability != null) {
            bits = optimalM(expectedElements, falsePositiveProbability);
        }
        if (hashes == null && expectedElements != null && bits != null) { hashes = optimalK(expectedElements, bits); }
        if (bits == null || hashes == null) {
            throw new NullPointerException(
                "Neither (expectedElements, falsePositiveProbability) nor (bits, hashes) were specified.");
        }
        if (expectedElements == null) { expectedElements = optimalN(hashes, bits); }
        if (falsePositiveProbability == null) { falsePositiveProbability = optimalP(hashes, bits, expectedElements); }

        done = true;
        return this;
    }


    @Override
    public FilterBuilder clone() {
        Object clone;
        try {
            clone = super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Cloning failed.");
        }
        return (FilterBuilder) clone;
    }


    /**
     * @return the number of expected elements for the Bloom filter
     */
    public int getExpectedElements() {
        return expectedElements;
    }

    /**
     * @return the bits of the Bloom filter in bits
     */
    public int getBits() {
        return bits;
    }

    /**
     * @return the number of hashes used by the Bloom filter
     */
    public int getHashes() {
        return hashes;
    }

    /**
     * @return the tolerable false positive probability of the Bloom filter
     */
    public double getFalsePositiveProbability() {
        return falsePositiveProbability;
    }

    /**
     * @return The hash method to be used by the Bloom filter
     */
    public HashProvider.HashMethod hashMethod() {
        return hashMethod;
    }

    /**
     * @return the actual hash function to be used by the Bloom filter
     */
    public HashProvider.HashFunction hashFunction() {
        return hashMethod.getHashFunction();
    }

    /**
     * @return Return the default Charset used for conversion of String values into byte arrays used for hashing
     */
    public static Charset defaultCharset() {
        return defaultCharset;
    }

    /**
     * Checks whether a configuration is compatible to another configuration based on the bits of the Bloom filter and
     * its hash functions.
     *
     * @param other the other configuration
     * @return {@code true} if the configurations are compatible
     */
    public boolean isCompatibleTo(FilterBuilder other) {
        return this.getBits() == other.getBits()
            && this.getHashes() == other.getHashes()
            && this.hashMethod() == other.hashMethod();
    }

    public static int alignByte(int bit) {
        return (((bit - 1) >> 3) + 1) << 3;
    }

    /**
     * Calculates the optimal size <i>size</i> of the bloom filter in bits given <i>expectedElements</i> (expected
     * number of elements in bloom filter) and <i>falsePositiveProbability</i> (tolerable false positive rate).
     *
     * @param n Expected number of elements inserted in the bloom filter
     * @param p Tolerable false positive rate
     * @return the optimal size <i>size</i> of the bloom filter in bits
     */
    public static int optimalM(long n, double p) {
        return (int) Math.ceil(-1 * (n * Math.log(p)) / Math.pow(Math.log(2), 2));
    }

    /**
     * Calculates the optimal <i>hashes</i> (number of hash function) given <i>expectedElements</i> (expected number of
     * elements in bloom filter) and <i>size</i> (size of bloom filter in bits).
     *
     * @param n Expected number of elements inserted in the bloom filter
     * @param m The size of the bloom filter in bits.
     * @return the optimal amount of hash functions hashes
     */
    public static int optimalK(long n, long m) {
        return (int) Math.ceil((Math.log(2) * m) / n);
    }

    /**
     * Calculates the amount of elements a Bloom filter for which the given configuration of size and hashes is
     * optimal.
     *
     * @param k number of hashes
     * @param m The size of the bloom filter in bits.
     * @return amount of elements a Bloom filter for which the given configuration of size and hashes is optimal.
     */
    public static int optimalN(long k, long m) {
        return (int) Math.ceil((Math.log(2) * m) / k);
    }

    /**
     * Calculates the best-case (uniform hash function) false positive probability.
     *
     * @param k                number of hashes
     * @param m                The size of the bloom filter in bits.
     * @param insertedElements number of elements inserted in the filter
     * @return The calculated false positive probability
     */
    public static double optimalP(long k, long m, double insertedElements) {
        return Math.pow((1 - Math.exp(-k * insertedElements / (double) m)), k);
    }
}
