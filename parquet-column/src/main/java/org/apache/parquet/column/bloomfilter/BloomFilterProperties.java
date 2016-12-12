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
 * Created by baidu on 16/11/9.
 */
public class BloomFilterProperties {
    private final float uniqueRatioThreshold;
    private final int valueCountThreshold;
    private final float falsePositive;
    private final int strategy;

    public BloomFilterProperties(float uniqThred, int countThred, float fpp) {
        this(uniqThred, countThred, fpp, 0);
    }

    public BloomFilterProperties(float uniqThred, int countThred, float fpp, int stra) {
        falsePositive = fpp;
        uniqueRatioThreshold = uniqThred;
        valueCountThreshold = countThred;
        strategy = stra;
    }

    public int getValueCountThreshold() {
        return valueCountThreshold;
    }

    public float getUniqueRatioThreshold() {
        return uniqueRatioThreshold;
    }

    public float getFalsePositive() {
        return falsePositive;
    }

    public int getStrategy() {
        return strategy;
    }
}
