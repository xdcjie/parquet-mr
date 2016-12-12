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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import org.apache.parquet.Log;

/**
 * Represents a Bloom filter and provides default methods for hashing.
 */
public abstract class BloomFilter<T> implements Cloneable, Serializable {
    private static final Log LOG = Log.getLog(BloomFilter.class);

    private long fileOffset;

    private long byteLength;

    /**
     * Adds the passed value to the filter.
     *
     * @param element value to add
     * @return {@code true} if the value did not previously exist in the filter. Note, that a false positive may occur,
     * thus the value may not have already been in the filter, but it hashed to a set of bits already in the filter.
     */
    public abstract boolean addRaw(byte[] element);

    /**
     * Adds the passed value to the filter.
     *
     * @param element value to add
     * @return {@code true} if the value did not previously exist in the filter. Note, that a false positive may occur,
     * thus the value may not have already been in the filter, but it hashed to a set of bits already in the filter.
     */
    public boolean add(T element) {
        return addRaw(toBytes(element));
    }

    /**
     * Performs a bulk add operation for a collection of elements.
     *
     * @param elements to add
     * @return a list of booleans indicating for each element, whether it was previously present in the filter
     */
    public List<Boolean> addAll(Collection<T> elements) {
        List<Boolean> rets = new ArrayList<Boolean>();
        for (T elem : elements) {
            rets.add(add(elem));
        }
        return rets;
    }

    /**
     * Removes all elements from the filter (i.e. resets all bits to zero).
     */
    public abstract void clear();

    /**
     * Tests whether an element is present in the filter (subject to the specified false positive rate).
     *
     * @param element to test
     * @return {@code true} if the element is contained
     */
    public abstract boolean contains(byte[] element);

    /**
     * Tests whether an element is present in the filter (subject to the specified false positive rate).
     *
     * @param element to test
     * @return {@code true} if the element is contained
     */
    public boolean contains(T element) {
        return contains(toBytes(element));
    }

    /**
     * Bulk-tests elements for existence in the filter.
     *
     * @param elements a collection of elements to test
     * @return a list of booleans indicating for each element, whether it is present in the filter
     */
    public List<Boolean> contains(Collection<T> elements) {
        List<Boolean> rets = new ArrayList<Boolean>();
        for (T elem : elements) {
            rets.add(contains(elem));
        }
        return rets;
    }

    /**
     * Bulk-tests elements for existence in the filter.
     *
     * @param elements a collection of elements to test
     * @return {@code true} if all elements are present in the filter
     */
    public boolean containsAll(Collection<T> elements) {
        Boolean ret = true;
        for (T elem : elements) {
            ret = ret && contains(elem);
        }
        return ret;
    }

    /**
     * Return the underyling bit vector of the Bloom filter.
     *
     * @return the underyling bit vector of the Bloom filter.
     */
    public abstract BitSet getBitSet();

    /**
     * Set the underyling bit verctor to the Bloom filter
     */
    public abstract void setBitSet(BitSet bitSet);

    /**
     * Returns the bytes of the Bloom filter
     */
    public abstract byte[] getBytes();

    /**
     * Returns the configuration/builder of the Bloom filter.
     *
     * @return the configuration/builder of the Bloom filter.
     */
    public abstract FilterBuilder config();

    /**
     * Constructs a deep copy of the Bloom filter
     *
     * @return a cloned Bloom filter
     */
    public abstract BloomFilter<T> clone();

    /**
     * Return the size of the Bloom filter, i.e. the number of positions in the underlyling bit vector (called m in the
     * literature).
     *
     * @return the bit vector size
     */
    public int getBitSize() {
        return config().getBits();
    }

    /**
     * Return the size of the Bloom filter, i.e. the number of positions in the underlyling bit vector (called m in the
     * literature).
     *
     * @return the byte size of bit vector
     */
    public abstract int getByteSize();

    /**
     * Returns the expected number of elements (called n in the literature)
     *
     * @return the expected number of elements
     */
    public int getExpectedElements() {
        return config().getExpectedElements();
    }

    /**
     * Returns the number of hash functions (called k in the literature)
     *
     * @return the number of hash functions
     */
    public int getHashes() {
        return config().getHashes();
    }

    /**
     * Converts an element to the byte array representation used for hashing.
     *
     * @param element the element to convert
     * @return the elements byte array representation
     */
    public byte[] toBytes(T element) {
        return element.toString().getBytes(FilterBuilder.defaultCharset());
    }

    /**
     * Checks if two Bloom filters are compatible, i.e. have compatible parameters (hash function, size, etc.)
     *
     * @param bloomFilter the bloomfilter
     * @param other the other bloomfilter
     * @return <code>true</code> if this bloomfilter is compatible with the other one
     *
     * @see #compatible(BloomFilter)
     */
    @Deprecated
    public boolean compatible(BloomFilter<T> bloomFilter, BloomFilter<T> other) {
        return bloomFilter.compatible(other);
    }

    /**
     * Checks if two Bloom filters are compatible, i.e. have compatible parameters (hash function, size, etc.)
     *
     * @param other the other bloomfilter
     * @return <code>true</code> if this bloomfilter is compatible with the other one
     */
    public boolean compatible(BloomFilter<T> other) {
        return config().isCompatibleTo(other.config());
    }

    /**
     * Destroys the Bloom filter by deleting its contents and metadata
     */
    public void remove() {
        clear();
    }

    /**
     * Returns the k hash values for an inputs element in byte array form
     *
     * @param bytes input element
     * @return hash values
     */
    public int[] hash(byte[] bytes) {
        return config().hashFunction().hash(bytes, config().getBits(), config().getHashes());
    }

    /**
     * Dispatches the hash function for a string value
     *
     * @param value the value to be hashed
     * @return array with <i>hashes</i> integer hash positions in the range <i>[0,size)</i>
     */
    public int[] hash(String value) {
        return hash(value.getBytes(FilterBuilder.defaultCharset()));
    }

    /**
     * Performs the union operation on two compatible bloom filters. This is achieved through a bitwise OR operation on
     * their bit vectors. This operations is lossless, i.e. no elements are lost and the bloom filter is the same that
     * would have resulted if all elements wer directly inserted in just one bloom filter.
     *
     * @param other the other bloom filter
     * @return <tt>true</tt> if this bloom filter could successfully be updated through the union with the provided
     * bloom filter
     */
    public abstract boolean union(BloomFilter<T> other);

    /**
     * Performs the intersection operation on two compatible bloom filters. This is achieved through a bitwise AND
     * operation on their bit vectors. The operations doesn't introduce any false negatives but it does raise the false
     * positive probability. The the false positive probability in the resulting Bloom filter is at most the
     * false-positive probability in one of the constituent bloom filters
     *
     * @param other the other bloom filter
     * @return <tt>true</tt> if this bloom filter could successfully be updated through the intersection with the
     * provided bloom filter
     */
    public abstract boolean intersect(BloomFilter<T> other);

    /**
     * Returns {@code true} if the Bloom filter does not contain any elements
     *
     * @return {@code true} if the Bloom filter does not contain any elements
     */
    public abstract boolean isEmpty();

    /**
     * Set byte length of bit vector
     */
    public void setByteLength(long length) {
        this.byteLength = length;
    }

    /**
     * Get byte length of bit vector
     */
    public long getByteLength() {
        return byteLength;
    }

    /**
     * Set the Bloom filter offset of data bytes in the file.
     */
    public void setFileOffset(long offset) {
        this.fileOffset = offset;
    }

    /**
     * @return the Bloom filter offset of data bytes in the file.
     */
    public long getFileOffset() {
        return fileOffset;
    }

    /**
     * Returns the expected false positive probability for the expected amounts of elements. This is independent of the
     * actual amount of elements in the filter. Use {@link #getFalsePositiveProbability(double)} for that purpose.
     *
     * @return the static expected false positive probability
     */
    public double getFalsePositiveProbability() {
        return config().getFalsePositiveProbability();
    }

    /**
     * Returns the probability of a false positive (approximated): <br> <code>(1 - e^(-hashes * insertedElements /
     * size)) ^ hashes</code>
     *
     * @param insertedElements The number of elements already inserted into the Bloomfilter
     * @return probability of a false positive after <i>expectedElements</i> {@link #addRaw(byte[])} operations
     */
    public double getFalsePositiveProbability(double insertedElements) {
        return FilterBuilder.optimalP(config().getHashes(), config().getBits(), insertedElements);
    }

    /**
     * Returns the probability of a false positive (approximated) using an estimation of how many elements are currently in the filter
     *
     * @return probability of a false positive
     */
    public double getEstimatedFalsePositiveProbability() {
        return getFalsePositiveProbability(getEstimatedPopulation());
    }


    /**
     * Calculates the numbers of Bits per element, based on the expected number of inserted elements
     * <i>expectedElements</i>.
     *
     * @param n The number of elements already inserted into the Bloomfilter
     * @return The numbers of bits per element
     */
    public double getBitsPerElement(int n) {
        return config().getBits() / (double) n;
    }

    /**
     * Returns the probability that a bit is zero.
     *
     * @param n The number of elements already inserted into the Bloomfilter
     * @return probability that a certain bit is zero after <i>expectedElements</i> {@link #addRaw(byte[])} operations
     */
    public double getBitZeroProbability(int n) {
        return Math.pow(1 - (double) 1 / config().getBits(), config().getHashes() * n);
    }

    /**
     * Estimates the current population of the Bloom filter (see: http://en.wikipedia.org/wiki/Bloom_filter#Approximating_the_number_of_items_in_a_Bloom_filter
     * )
     *
     * @return the estimated amount of elements in the filter
     */
    public Double getEstimatedPopulation() {
        return population(getBitSet(), config());
    }

    public static Double population(BitSet bitSet, FilterBuilder config) {
        int oneBits = bitSet.cardinality();
        return -config.getBits() / ((double) config.getHashes()) * Math.log(1 - oneBits / ((double) config.getBits()));
    }

    /**
     * Prints the Bloom filter: metadata and data
     *
     * @return String representation of the Bloom filter
     */
    public String asString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Bloom Filter Parameters: ");
        sb.append("bits = " + config().getBits() + ", ");
        sb.append("bytes = " + getByteSize() + ", ");
        sb.append("elements = " + config().getExpectedElements() + ", ");
        sb.append("hashes = " + config().getHashes() + ", ");
        sb.append("falsePositive = " + config().getFalsePositiveProbability());
        return sb.toString();
    }


}
