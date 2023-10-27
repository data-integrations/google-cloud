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

package io.cdap.plugin.gcp.spanner.sink;

import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfig;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReplicaInfo;
import com.google.cloud.spanner.Spanner;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferenceNames;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.spanner.SpannerConstants;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import io.cdap.plugin.gcp.spanner.connector.SpannerConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;


/**
 * This class extends {@link ReferenceBatchSink} to write to Google Cloud Spanner.
 *
 * Uses a {@link SpannerOutputFormat} and {@link SpannerOutputFormat.SpannerRecordWriter} to configure
 * and write to spanner. The <code>prepareRun</code> method configures the job by extracting
 * the user provided configuration and preparing it to be passed to {@link SpannerOutputFormat}.
 *
 */
@Plugin(type = "batchsink")
@Name(SpannerSink.NAME)
@Description("Batch sink to write to Cloud Spanner. Cloud Spanner is a fully managed, mission-critical, " +
  "relational database service that offers transactional consistency at global scale, schemas, " +
  "SQL (ANSI 2011 with extensions), and automatic, synchronous replication for high availability.")
@Metadata(properties = {@MetadataProperty(key = Connector.PLUGIN_TYPE, value = SpannerConnector.NAME)})
public final class SpannerSink extends BatchSink<StructuredRecord, NullWritable, Mutation> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerSink.class);
  public static final String NAME = "Spanner";
  private static final String TABLE_NAME = "tablename";
  private final SpannerSinkConfig config;
  private RecordToMutationTransformer transformer;

  public SpannerSink(SpannerSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    // TODO CDAP-15898 add validation to validate against input schema and underlying spanner table if it exists
    config.validate(collector);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector, context.getArguments().asMap());
    // throw a validation exception if any failures were added to the collector.
    collector.getOrThrowException();

    Schema configuredSchema = config.getSchema(collector);
    Schema schema = configuredSchema == null ? context.getInputSchema() : configuredSchema;
    CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(config.cmekKey, context.getArguments().asMap(),
                                                     context.getFailureCollector());
    Configuration configuration = new Configuration();
    configuration.setBoolean(SpannerConstants.IS_PREVIEW_ENABLED, context.isPreviewEnabled());
    String location = null;
    if (cmekKeyName != null) {
      configuration.set(SpannerConstants.CMEK_KEY, cmekKeyName.toString());
    }
    // initialize spanner
    String instanceConfigId;
    try (Spanner spanner = SpannerUtil.getSpannerService(config.connection.getServiceAccount(),
                                                         config.connection.isServiceAccountFilePath(),
                                                         config.connection.getProject())) {
      // get location for lineage data (location info resides in instance config for spanner)
      Instance spannerInstance = spanner.getInstanceAdminClient().getInstance(config.getInstance());
      instanceConfigId = spannerInstance.getInstanceConfigId().getInstanceConfig();
      InstanceConfig instanceConfig = spanner.getInstanceAdminClient()
          .getInstanceConfig(instanceConfigId);
      ReplicaInfo replica = instanceConfig.getReplicas().get(0);
      if (replica != null) {
        location = replica.getLocation();
      }
    }
    String fqn = SpannerUtil.getFQN(config.getConnection().getProject(), instanceConfigId,
        config.getInstance(), config.getDatabase(), config.getTable());
    String referenceName =
        Strings.isNullOrEmpty(config.getReferenceName()) ? ReferenceNames.normalizeFqn(fqn)
            : config.getReferenceName();
    Asset asset = Asset.builder(referenceName)
        .setFqn(fqn).setLocation(location)
        .build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);

    SpannerOutputFormat.configure(configuration, config, schema);
    context.addOutput(Output.of(referenceName,
                                new SinkOutputFormatProvider(SpannerOutputFormat.class, configuration)));

    List<Schema.Field> fields = schema.getFields();
    if (fields != null && !fields.isEmpty()) {
      // Record the field level WriteOperation
      lineageRecorder.recordWrite("Write", "Wrote to Spanner table.",
                                  fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();
    transformer = new RecordToMutationTransformer(config.getTable(), config.getSchema(collector));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Mutation>> emitter) {
    Mutation mutation = transformer.transform(input);
    emitter.emit(new KeyValue<>(null, mutation));
  }

  @Override
  public void destroy() {
    super.destroy();
  }
}
