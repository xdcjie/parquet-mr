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
package org.apache.parquet.filter2.compat;

import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFetcher;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Given a {@link Filter} applies it to a list of BlockMetaData (row groups)
 * If the Filter is an {@link org.apache.parquet.filter.UnboundRecordFilter} or the no op filter,
 * no filtering will be performed.
 */
public class RowGroupFetcher implements Visitor<List<ColumnChunkMetaData>> {
    private final List<BlockMetaData> blocks;
  
    public static List<ColumnChunkMetaData> fetchRowGroups(Filter filter, List<BlockMetaData> blocks) {
      checkNotNull(filter, "filter");
      return filter.accept(new RowGroupFetcher(blocks));
    }
  
    private RowGroupFetcher(List<BlockMetaData> blocks) {
      this.blocks = checkNotNull(blocks, "blocks");
    }
  
    @Override
    public List<ColumnChunkMetaData> visit(FilterCompat.FilterPredicateCompat filterPredicateCompat) {
      FilterPredicate filterPredicate = filterPredicateCompat.getFilterPredicate();
      return StatisticsFetcher.needFetch(filterPredicate, blocks);
    }
  
    @Override
    public List<ColumnChunkMetaData> visit(FilterCompat.UnboundRecordFilterCompat unboundRecordFilterCompat) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public List<ColumnChunkMetaData> visit(NoOpFilter noOpFilter) {
      return new ArrayList<ColumnChunkMetaData>();
    }
}
