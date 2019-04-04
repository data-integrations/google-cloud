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

package io.cdap.plugin.gcp.common;

import io.cdap.cdap.api.dataset.lib.FileSetProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Utilities for configuring file sets during pipeline configuration.
 */
public final class FileSetUtil {
  private static final String AVRO_OUTPUT_CODEC = "avro.output.codec";
  private static final String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
  private static final String AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";
  private static final String CODEC_SNAPPY = "snappy";
  private static final String CODEC_DEFLATE = "deflate";
  public static final Set<String> AVRO_CODECS = new HashSet<>(Arrays.asList(CODEC_SNAPPY, CODEC_DEFLATE));
  private static final String CODEC_GZIP = "gzip";
  public static final Set<String> PARQUET_CODECS = new HashSet<>(Arrays.asList(CODEC_SNAPPY, CODEC_GZIP));
  private static final String PARQUET_AVRO_SCHEMA = "parquet.avro.schema";
  private static final String PARQUET_COMPRESSION = "parquet.compression";
  public static final String NONE = "None";

  /**
   * Sets the compression options for an Avro file set format. Also, sets the schema output key to the schema provided.
   * The map-reduce output compression is set to true, and the compression codec can be set to one of
   * the following:
   * <ul>
   * <li>snappy</li>
   * <li>deflate</li>
   * </ul>
   *
   * @param format The format param is required in order to provide better error msg
   * @param compressionCodec compression code provided, can be either snappy or deflate
   * @param schema           output schema to be set as the schema output key for the file set
   * @param isOutputProperty boolean value to identify if the compression options are used as output property for
   *                         FilesetProperties.Builder
   * @return map of string to be set as configuration or output properties in FileSetProperties.Builder
   */
  public static Map<String, String> getAvroCompressionConfiguration(String format, String compressionCodec,
                                                                    String schema, Boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }

    if (isCompressionRequired(format, compressionCodec, AVRO_CODECS)) {
      String codec = compressionCodec.toLowerCase();

      conf.put(prefix + MAPRED_OUTPUT_COMPRESS, "true");
      conf.put(prefix + AVRO_SCHEMA_OUTPUT_KEY, schema);
      conf.put(prefix + AVRO_OUTPUT_CODEC, codec);
    }

    return conf;
  }

  /**
   * Sets the compression options for an Parquet file set format.
   * Also, sets the schema output key to the schema provided.
   * The compression codec can be set to one of the following:
   * <ul>
   * <li>SNAPPY</li>
   * <li>GZIP</li>
   * </ul>
   *
   * @param format The format param is required in order to provide better error msg
   * @param compressionCodec compression code selected by user. Can be either snappy or gzip
   * @param schema           output schema to be set as the schema output key for the file set
   * @param isOutputProperty boolean value to identify if the compression options are as output property for
   *                         FilesetProperties Builder
   * @return map of string to be set as configuration or output properties in FileSetProperties.Builder
   */
  public static Map<String, String> getParquetCompressionConfiguration(String format, String compressionCodec,
                                                                       String schema, Boolean isOutputProperty) {
    Map<String, String> conf = new HashMap<>();
    String prefix = "";
    if (isOutputProperty) {
      prefix = FileSetProperties.OUTPUT_PROPERTIES_PREFIX;
    }
    conf.put(prefix + PARQUET_AVRO_SCHEMA, schema);
    if (isCompressionRequired(format, compressionCodec, PARQUET_CODECS)) {
      conf.put(prefix + PARQUET_COMPRESSION, compressionCodec.toUpperCase());
    }
    return conf;
  }

  /**
   * Checks if compression configuration is required and validates the if the codec is supported
   *
   * @param format The format to validate if the codec is supported
   * @param codec The codec to validate
   * @param supportedCodecs  The supported codecs
   * @return True if compression is required, False if compression is not required
   * @throws IllegalArgumentException in case the codec is not supported
   */
  public static boolean isCompressionRequired(String format, String codec, Set<String> supportedCodecs) {

    if (codec != null && !codec.equalsIgnoreCase(NONE)) {
      if (!supportedCodecs.contains(codec.toLowerCase())) {
        throw new IllegalArgumentException("Unsupported compression codec " + codec + " for format " + format);
      }
      return true;
    }
    return false;
  }
}
