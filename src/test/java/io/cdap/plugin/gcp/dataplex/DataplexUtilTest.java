package io.cdap.plugin.gcp.dataplex;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.Schema;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Schema.class, Entity.class, Storage.class, GCPUtils.class, GoogleCredentials.class,
  MetadataServiceClient.class})
public class DataplexUtilTest {

  @Mock
  com.google.cloud.dataplex.v1.Schema dataplexTableSchema;

  @Test
  public void testGetSchemaNull() {
    MockFailureCollector collector = new MockFailureCollector();
    com.google.cloud.dataplex.v1.Schema schema = PowerMockito.mock(Schema.class);
    Assert.assertNull(DataplexUtil.getTableSchema(schema, collector));
  }

  @Test
  public void testGetSchemaWRepeatMode() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)
      ));
    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.RECORD);
    Schema.SchemaField schemaField = fieldBuilder.build();
    Schema.PartitionField.Builder partitionFieldBuilder = Schema.PartitionField.newBuilder();
    partitionFieldBuilder.setName(avroField.getName());
    partitionFieldBuilder.setType(Schema.Type.STRING);
    com.google.cloud.dataplex.v1.Schema dataplexSchema =
      Schema.newBuilder().addFields(schemaField).addPartitionFields(partitionFieldBuilder.build()).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    List<Schema.PartitionField> partitionFieldList = dataplexSchema.getPartitionFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Mockito.when(dataplexTableSchema.getPartitionFieldsList()).thenReturn(partitionFieldList);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWnUllMode() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.DOUBLE);
    fieldBuilder.setMode(Schema.Mode.NULLABLE);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWRepeatedMode() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.DOUBLE);
    fieldBuilder.setMode(Schema.Mode.REPEATED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  // return null as mode is unspecified
  @Test
  public void testGetSchemaWModeUnspecified() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.DOUBLE);
    fieldBuilder.setMode(Schema.Mode.MODE_UNSPECIFIED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
  }

  @Test
  public void testGetSchemaWithTypeBoolean() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.BOOLEAN);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeInt32() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.INT32);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeInt64() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.INT64);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeString() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.STRING);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeBinary() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.BINARY);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeDate() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.DATE);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemWithTypeByte() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.BYTE);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeTime() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.TIME);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeTimestamp() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.TIMESTAMP);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetJobCompletion() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set(DataplexConstants.SERVICE_ACCOUNT_TYPE, GCPConnectorConfig.SERVICE_ACCOUNT_JSON);
    PowerMockito.mockStatic(GCPUtils.class);
    GoogleCredentials credentials = PowerMockito.mock(GoogleCredentials.class);
    String serviceAccount = "account";
    PowerMockito.when(GCPUtils.loadServiceAccountCredentials(serviceAccount, true)).thenReturn(credentials);
    Assert.assertThrows(RuntimeException.class, () -> {
      DataplexUtil.getJobCompletion(configuration);
    });
  }

  @Test
  public void testGetSchemaWithTypeDecimal() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.DECIMAL);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  @Test
  public void testGetSchemaWithTypeNull() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.NULL);
    fieldBuilder.setMode(Schema.Mode.REQUIRED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    Assert.assertNotNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Assert.assertThrows(IOException.class, () -> {
      DataplexUtil.getMetadataServiceClient(googleCredentials);
    });
    Assert.assertEquals("id", DataplexUtil.getTableSchema(dataplexTableSchema, collector).getField("id").getName());
  }

  // returns null as type is unspecified.
  @Test
  public void testGetSchemaWithTypeUnSpecified() {
    MockFailureCollector collector = new MockFailureCollector();
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    io.cdap.cdap.api.data.schema.Schema.Field avroField = schema.getField("id");
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName(avroField.getName());
    fieldBuilder.setType(Schema.Type.TYPE_UNSPECIFIED);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).build();
    List<Schema.SchemaField> fields = dataplexSchema.getFieldsList();
    com.google.cloud.dataplex.v1.Schema dataplexTableSchema = PowerMockito.mock(Schema.class);
    Mockito.when(dataplexTableSchema.getFieldsList()).thenReturn(fields);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Assert.assertThrows(RuntimeException.class, () -> {
      DataplexUtil.getDataplexServiceClient(googleCredentials);
    });
    Assert.assertNull(DataplexUtil.getTableSchema(dataplexTableSchema, collector));
  }

  @Test
  public void testGetStorageFormatForEntity() throws IOException {
    Assert.assertEquals("application/x-avro", DataplexUtil.getStorageFormatForEntity("avro"));
    Assert.assertEquals("text/csv", DataplexUtil.getStorageFormatForEntity("csv"));
    Assert.assertEquals("application/json", DataplexUtil.getStorageFormatForEntity("json"));
    Assert.assertEquals("application/x-orc", DataplexUtil.getStorageFormatForEntity("orc"));
    Assert.assertEquals("application/x-parquet", DataplexUtil.getStorageFormatForEntity("parquet"));
    Assert.assertEquals("undefined", DataplexUtil.getStorageFormatForEntity(""));
  }

  @Test
  public void testGetDataplexSchema() throws IOException {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("price",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.DOUBLE)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("stockSize",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.INT)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("updated",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.BOOLEAN)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("batchId",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.STRING)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("tax",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.FLOAT)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("image",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.BYTES)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timestampMillis",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIMESTAMP_MILLIS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timeMillis",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIME_MILLIS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timestamp",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIMESTAMP_MICROS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timeMicros",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIME_MICROS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("date",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.DATE))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("dateTime",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.DATETIME))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("updatedPrice",
        (io.cdap.cdap.api.data.schema.Schema.decimalOf(5, 2))));

    Assert.assertEquals(14, DataplexUtil.getDataplexSchema(schema).getFieldsCount());
    Assert.assertEquals("id", DataplexUtil.getDataplexSchema(schema).getFields(0).getName());
  }

  @Test
  public void testGetDataplexSchemaWTypeRecord() throws IOException {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("price",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.DOUBLE)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("stockSize",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.INT)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("updated",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.BOOLEAN)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("batchId",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.STRING)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("tax",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.FLOAT)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("image",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.BYTES)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timestampMillis",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIMESTAMP_MILLIS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timeMillis",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIME_MILLIS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timestamp",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIMESTAMP_MICROS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("timeMicros",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.TIME_MICROS))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("date",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.DATE))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("dateTime",
        io.cdap.cdap.api.data.schema.Schema.nullableOf(
          io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.DATETIME))),
      io.cdap.cdap.api.data.schema.Schema.Field.of("updatedPrice",
        (io.cdap.cdap.api.data.schema.Schema.decimalOf(5, 2))));

    Assert.assertEquals(14, DataplexUtil.getDataplexSchema(schema).getFieldsCount());
    Assert.assertEquals("id", DataplexUtil.getDataplexSchema(schema).getFields(0).getName());
  }

  @Test
  public void testGetDataplexSchemaWTypeNull() throws IOException {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("data",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.NULL)));
    Assert.assertEquals(1, DataplexUtil.getDataplexSchema(schema).getFieldsCount());
    Assert.assertEquals("data", DataplexUtil.getDataplexSchema(schema).getFields(0).getName());
  }
  
}
