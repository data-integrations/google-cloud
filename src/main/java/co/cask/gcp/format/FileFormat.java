/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.gcp.format;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.format.output.AvroOutputProvider;
import co.cask.gcp.format.output.DelimitedTextOutputProvider;
import co.cask.gcp.format.output.FileOutputFormatter;
import co.cask.gcp.format.output.FileOutputFormatterProvider;
import co.cask.gcp.format.output.JsonOutputProvider;
import co.cask.gcp.format.output.ParquetOutputProvider;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * FileFormat supported by the file based sources/sinks.
 */
public enum FileFormat {
  CSV(new DelimitedTextOutputProvider(",")),
  TSV(new DelimitedTextOutputProvider("\t")),
  DELIMITED(new DelimitedTextOutputProvider(null)),
  AVRO(new AvroOutputProvider()),
  PARQUET(new ParquetOutputProvider()),
  JSON(new JsonOutputProvider()),
  BLOB(null);
  private final FileOutputFormatterProvider outputProvider;

  FileFormat(FileOutputFormatterProvider outputProvider) {
    this.outputProvider = outputProvider;
  }

  @Nullable
  public <K, V> FileOutputFormatter<K, V> getFileOutputFormatter(Map<String, String> properties,
                                                                 @Nullable Schema schema) {
    //noinspection unchecked
    return (FileOutputFormatter<K, V>) outputProvider.create(properties, schema);
  }
}
