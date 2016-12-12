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
package org.apache.parquet.filter2.statisticslevel;

import org.apache.parquet.Log;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.*;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Preconditions.checkNotNull;

/**
 * Applies a {@link FilterPredicate} to statistics about a group of
 * records.
 *
 * Note: the supplied predicate must not contain any instances of the not() operator as this is not
 * supported by this filter.
 *
 * the supplied predicate should first be run through {@link org.apache.parquet.filter2.predicate.LogicalInverseRewriter} to rewrite it
 * in a form that doesn't make use of the not() operator.
 *
 * the supplied predicate should also have already been run through
 * {@link org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator}
 * to make sure it is compatible with the schema of this file.
 *
 * Returns true if all the records represented by the statistics in the provided column metadata can be dropped.
 *         false otherwise (including when it is not known, which is often the case).
 */
public class StatisticsFetcher implements FilterPredicate.Visitor<List<ColumnChunkMetaData>> {
    private static final Log LOG = Log.getLog(StatisticsFetcher.class);
  
    public static List<ColumnChunkMetaData> needFetch(FilterPredicate pred, List<BlockMetaData> blocks) {
      checkNotNull(pred, "pred");
      checkNotNull(blocks, "blocks");
      return pred.accept(new StatisticsFetcher(blocks));
    }
  
    private final Map<ColumnPath, List<ColumnChunkMetaData>> columns = new HashMap<ColumnPath, List<ColumnChunkMetaData>>();
  
    private StatisticsFetcher(List<BlockMetaData> blocksList) {
      for (BlockMetaData block : blocksList) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          if (!columns.containsKey(chunk.getPath())) {
            columns.put(chunk.getPath(), new ArrayList<ColumnChunkMetaData>());
          }
          columns.get(chunk.getPath()).add(chunk);
        }
      }
    }
  
    private List<ColumnChunkMetaData> getColumnChunkList(ColumnPath columnPath) {
      List<ColumnChunkMetaData> c = columns.get(columnPath);
      checkArgument(c != null, "Column " + columnPath.toDotString() + " not found in schema!");
      return c;
    }
  
    @Override
    public <T extends Comparable<T>> List<ColumnChunkMetaData> visit(Eq<T> eq) {
      Column<T> filterColumn = eq.getColumn();
      T value = eq.getValue();
      return getColumnChunkList(filterColumn.getColumnPath());
    }
  
    @Override
    public <T extends Comparable<T>> List<ColumnChunkMetaData> visit(NotEq<T> notEq) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public <T extends Comparable<T>> List<ColumnChunkMetaData> visit(Lt<T> lt) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public <T extends Comparable<T>> List<ColumnChunkMetaData> visit(LtEq<T> ltEq) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public <T extends Comparable<T>> List<ColumnChunkMetaData> visit(Gt<T> gt) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public <T extends Comparable<T>> List<ColumnChunkMetaData> visit(GtEq<T> gtEq) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public List<ColumnChunkMetaData> visit(And and) {
      List<ColumnChunkMetaData> merge = and.getLeft().accept(this);
      merge.addAll(and.getRight().accept(this));
      return merge;
    }
  
    @Override
    public List<ColumnChunkMetaData> visit(Or or) {
      List<ColumnChunkMetaData> merge = or.getLeft().accept(this);
      merge.addAll(or.getRight().accept(this));
      return merge;
    }
  
    @Override
    public List<ColumnChunkMetaData> visit(Not not) {
      throw new IllegalArgumentException(
          "This predicate contains a not!" + 
          "Did you forget to run this predicate through LogicalInverseRewriter? " + not);
    }
  
    private <T extends Comparable<T>, U extends UserDefinedPredicate<T>> 
    List<ColumnChunkMetaData> visit(UserDefined<T, U> ud, boolean inverted) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> 
    List<ColumnChunkMetaData> visit(UserDefined<T, U> ud) {
      return new ArrayList<ColumnChunkMetaData>();
    }
  
    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> 
    List<ColumnChunkMetaData> visit(LogicalNotUserDefined<T, U> lnud) {
      return new ArrayList<ColumnChunkMetaData>();
    }

}
