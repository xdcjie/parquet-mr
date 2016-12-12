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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Log;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.bloomfilter.BloomFilter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetOutputFormat.*;
import static org.apache.parquet.hadoop.TestUtils.enforceEmptyDir;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.*;

public class TestBloomFilter {
    private static final Log LOG = Log.getLog(TestBloomFilter.class);
  
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        Path root = new Path("target/tests/TestBloomFilter/");
        enforceEmptyDir(conf, root);
        MessageType schema = parseMessageType(
            "message test { "
            + "required binary binary_field; "
            + "required int32 int32_field; "
            + "required int64 int64_field; "
            + "required float float_field; "
            + "required double double_field; "
            + "required binary flba_field; "
            + "required binary int96_field; "
            + "} ");
        GroupWriteSupport.setSchema(schema, conf);
        conf.setBoolean(ENABLE_BLOOM_FILTER, true);
        conf.setFloat(BLOOM_FILTER_FALSE_POSITIVE, 0.01f);
        conf.setFloat(BLOOM_UNIQUE_RATIO_THRESHOLD, 0.2f);
        conf.setInt(BLOOM_VALUE_COUNT_THRESHOLD, 20);
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        Map<String, Encoding> expected = new HashMap<String, Encoding>();
        expected.put("10000-" + PARQUET_1_0, PLAIN);
        for (int modulo : asList(10000)) {
            Path file = new Path(root, "v1_" + modulo);
            ParquetWriter<Group> writer = new ParquetWriter<Group>(
                file,
                new GroupWriteSupport(),
                UNCOMPRESSED, 1024000, 10240, 512, true, false, WriterVersion.PARQUET_1_0, conf);
            for (int i = 0; i < 10000; i++) {
                String cc = "test" + (i % modulo);
                writer.write(
                    f.newGroup()
                    .append("binary_field", cc)
                    .append("int32_field", new Integer(i))
                    .append("int64_field", new Long((long) i))
                    .append("float_field", new Float(i * 1.0f))
                    .append("double_field", new Double(i * 1.0))
                    .append("flba_field", cc)
                    .append("int96_field", cc));
                    // .append("int96_field", Binary.fromByteArray(cc.getBytes())));
            }
            writer.close();
            ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
            for (int i = 0; i < 10000; i++) {
                Group group = reader.read();
                String cc = "test" + (i % modulo);
                assertEquals(cc, group.getBinary("binary_field", 0).toStringUsingUTF8());
                assertEquals(i, group.getInteger("int32_field", 0));
                assertEquals(i, group.getLong("int64_field", 0));
                assertEquals(i * 1.0f, group.getFloat("float_field", 0), 0.001);
                assertEquals(i * 1.0, group.getDouble("double_field", 0), 0.001);
                assertEquals(cc, group.getBinary("flba_field", 0).toStringUsingUTF8());
                assertEquals(Binary.fromByteArray(cc.getBytes()), group.getBinary("int96_field", 0));
            }
            reader.close();
            int failedContain = 0;
            ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
            List<BloomFilter> bloomFilters = new ArrayList<BloomFilter>();
            for (BlockMetaData blockMetaData : footer.getBlocks()) {
                for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
                    org.apache.parquet.column.statistics.Statistics statistics = column.getStatistics();
                    assertTrue(statistics.hasBloomFilter());
                    BloomFilter bloomFilter = statistics.getBloomFilter();
                    FSDataInputStream in = file.getFileSystem(conf).open(file);
                    LOG.info(String.format("bloom filter reader info : offset [%,d], length [%,d]",
                            bloomFilter.getFileOffset(), bloomFilter.getByteLength()));
                    byte[] bitSet = new byte[(int) bloomFilter.getByteLength()];
                    in.seek(bloomFilter.getFileOffset());
                    in.readFully(bitSet);
                    bloomFilter.setBitSet(BitSet.valueOf(bitSet));
                    bloomFilters.add(bloomFilter);
                }
            }
            for (long lmodulo : asList(10000L)) {
                for (int i = 0; i < 10000L; i++) {
                    Object item = null;
                    boolean contain = false;
                    for (int k = 0; k < 7; k++) {
                        switch (k) {
                            case 0:
                                item = "test" + (i % lmodulo);
                                contain = bloomFilters.get(0).contains(Binary.fromString(item.toString()));
                                break;
                            case 1:
                                item = new Integer(i);
                                contain = bloomFilters.get(1).contains(item);
                                break;
                            case 2:
                                item = new Long(i);
                                contain = bloomFilters.get(2).contains(item);
                                break;
                            case 3:
                                item = new Float(i * 1.0f);
                                contain = bloomFilters.get(3).contains(item);
                                break;
                            case 4:
                                item = new Double(i * 1.0);
                                contain = bloomFilters.get(4).contains(item);
                                break;
                            case 5:
                                item = "test" + (i % lmodulo);
                                contain = bloomFilters.get(5).contains(Binary.fromString(item.toString()));
                                break;
                            case 6:
                                item = "test" + (i % lmodulo);
                                contain = bloomFilters.get(6).contains(Binary.fromString(item.toString()));
                                break;
                            default :
                                break;
                        }
                        assertTrue(contain);
                    }
                }
            }
            LOG.info("failedContain is " + failedContain);
        }
    }
}
