/*
 * Copyright © 2015, 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.gcp.common;

import co.cask.cdap.api.dataset.lib.FileSetProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for configuring file sets during pipeline configuration.
 */
public final class FileSetUtil {
  private static final String AVRO_OUTPUT_CODEC = "avro.output.codec";
  private static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
  private static final String AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";
  private static final String CODEC_SNAPPY = "snappy";
  private static final String CODEC_DEFLATE = "deflate";
  private static final String CODEC_GZIP = "gzip";
  private static final String CODEC_LZO = "lzo";
  private static final String PARQUET_AVRO_SCHEMA = "parquet.avro.schema";
  private static final String PARQUET_COMPRESSION = "parquet.compression";

  /**
   * Sets the compression options for an Avro file set format. Also, sets the schema output key to the schema provided.
   * The map-reduce output compression is set to true, and the compression codec can be set to one of
   * the following:
   * <ul>
   *   <li>snappy</li>
   *   <li>deflate</li>
   * </ul>
   * @param compressionCodec compression code provided, can be either snappy or deflate
   * @param schema output schema to be set as the schema output key for the file set
   * @param isOutputProperty boolean value to identify if the compression options are used as output property for
   *                         FilesetProperties.Builder
   * @return map of string to be set as configuration or output properties in FileSetProperties.Builder
   */
  public static Map<String, String> getAvroCompressionConfiguration(String compressionCodec, String schema,
                                                                    Boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }
    conf.put(prefix + AVRO_SCHEMA_OUTPUT_KEY, schema);
    if (compressionCodec != null && !compressionCodec.equalsIgnoreCase("None")) {
      conf.put(prefix + MAPRED_OUTPUT_COMPRESS, "true");
      switch (compressionCodec.toLowerCase()) {
        case CODEC_SNAPPY:
          conf.put(prefix + AVRO_OUTPUT_CODEC, CODEC_SNAPPY);
          break;
        case CODEC_DEFLATE:
          conf.put(prefix + AVRO_OUTPUT_CODEC, CODEC_DEFLATE);
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + compressionCodec);
      }
    }
    return conf;
  }

  /**
   * Sets the compression options for an Avro file set format. Also, sets the schema output key to the schema provided.
   * The compression codec can be set to one of the following:
   * <ul>
   *   <li>SNAPPY</li>
   *   <li>GZIP</li>
   *   <li>LZO</li>
   * </ul>
   * @param compressionCodec compression code selected by user. Can be either snappy or deflate
   * @param schema output schema to be set as the schema output key for the file set
   * @param isOutputProperty boolean value to identify if the compression options are as output property for
   *                         FilesetProperties Builder
   * @return map of string to be set as configuration or output properties in FileSetProperties.Builder
   */
  public static Map<String, String> getParquetCompressionConfiguration(String compressionCodec, String schema,
                                                                       Boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }
    conf.put(prefix + PARQUET_AVRO_SCHEMA, schema);
    if (compressionCodec != null && !compressionCodec.equalsIgnoreCase("None")) {
      switch (compressionCodec.toLowerCase()) {
        case CODEC_SNAPPY:
          conf.put(prefix + PARQUET_COMPRESSION, CODEC_SNAPPY.toUpperCase());
          break;
        case CODEC_GZIP:
          conf.put(prefix + PARQUET_COMPRESSION, CODEC_GZIP.toUpperCase());
          break;
        case CODEC_LZO:
          //requires: sudo apt-get install lzop
          conf.put(prefix + PARQUET_COMPRESSION, CODEC_LZO.toUpperCase());
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + compressionCodec);
      }
    }
    return conf;
  }
}
