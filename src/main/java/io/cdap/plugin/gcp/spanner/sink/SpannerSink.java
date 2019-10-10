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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Strings;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import io.cdap.cdap.api.annotation.Description;
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
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


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
    config.validate(collector);
    // throw a validation exception if any failures were added to the collector.
    collector.getOrThrowException();

    Schema schema = config.getSchema(collector);
    if (!context.isPreviewEnabled()) {
      try (Spanner spanner = SpannerUtil.getSpannerService(config.getServiceAccountFilePath(), config.getProject())) {
        DatabaseId db = DatabaseId.of(config.getProject(), config.getInstance(), config.getDatabase());
        DatabaseClient dbClient = spanner.getDatabaseClient(db);
        DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
        // create database
        Database database = getOrCreateDatabase(dbAdminClient);
        // create table
        createTableIfNotPresent(dbClient, database, schema);
      } catch (IOException e) {
        throw new RuntimeException("Exception while trying to get Spanner service. ", e);
      }
    }

    Configuration configuration = new Configuration();

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);

    SpannerOutputFormat.configure(configuration, config, schema);
    context.addOutput(Output.of(config.getReferenceName(),
                                new SinkOutputFormatProvider(SpannerOutputFormat.class, configuration)));

    List<Schema.Field> fields = schema.getFields();
    if (fields != null && !fields.isEmpty()) {
      // Record the field level WriteOperation
      lineageRecorder.recordWrite("Write", "Wrote to Spanner table.",
                                  fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  private void createTableIfNotPresent(DatabaseClient dbClient, Database database,
                                       Schema schema) throws ExecutionException, InterruptedException {
    boolean tableExists = isTablePresent(dbClient);
    if (!tableExists) {
      if (Strings.isNullOrEmpty(config.getKeys())) {
        throw new IllegalArgumentException(String.format("Spanner table %s does not exist. To create it from the " +
                                                           "pipeline, primary keys must be provided",
                                                         config.getTable()));
      }
      String createStmt = SpannerUtil.convertSchemaToCreateStatement(config.getTable(),
                                                                     config.getKeys(), schema);
      LOG.debug("Creating table with create statement: {} in database {} of instance {}", createStmt,
                config.getDatabase(), config.getInstance());
      // In Spanner table creation is an update ddl operation on the database.
      OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        database.updateDdl(Collections.singletonList(createStmt), null);
      // Table creation is an async operation. So wait until table is created.
      op.get();
    }
  }

  private boolean isTablePresent(DatabaseClient dbClient) {
    // Spanner does not have apis to get table or check if a given table exists. So select the table name from
    // information schema (metadata) of spanner database.
    Statement statement = Statement.newBuilder(String.format("SELECT\n" +
                                                               "    t.table_name\n" +
                                                               "FROM\n" +
                                                               "    information_schema.tables AS t\n" +
                                                               "WHERE\n" +
                                                               "    t.table_catalog = '' AND t.table_schema = '' AND\n"
                                                               + "    t.table_name = @%s", TABLE_NAME))
      .bind(TABLE_NAME).to(config.getTable()).build();

    ResultSet resultSet = dbClient.singleUse().executeQuery(statement);

    boolean tableExists = resultSet.next();
    // close result set to free up resources.
    resultSet.close();

    return tableExists;
  }

  private Database getOrCreateDatabase(
    DatabaseAdminClient dbAdminClient) throws ExecutionException, InterruptedException {
    Database database = getDatabaseIfPresent(dbAdminClient);

    if (database == null) {
      LOG.debug("Database not found. Creating database {} in instance {}.", config.getDatabase(), config.getInstance());
      // Create database
      OperationFuture<Database, CreateDatabaseMetadata> op =
        dbAdminClient.createDatabase(config.getInstance(), config.getDatabase(), Collections.emptyList());
      // database creation is an async operation. Wait until database creation operation is complete.
      database = op.get();
    }

    return database;
  }

  @Nullable
  private Database getDatabaseIfPresent(DatabaseAdminClient dbAdminClient) {
    Database database = null;
    try {
      database = dbAdminClient.getDatabase(config.getInstance(), config.getDatabase());
    } catch (SpannerException e) {
      if (e.getErrorCode() != ErrorCode.NOT_FOUND) {
        throw e;
      }
    }

    return database;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    Schema schema = config.getSchema(collector);
    collector.getOrThrowException();
    transformer = new RecordToMutationTransformer(config.getTable(), schema);
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
