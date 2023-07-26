package io.cdap.plugin.gcp.bigquery.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.avro.StructuredToAvroTransformer;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.function.Supplier;


public class AvroRecordWriterTest {

  @Test
  public void testSuccessfulWrites() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Supplier<OutputStream> outputStreamSupplier = () -> byteArrayOutputStream;

    AvroRecordWriter avroRecordWriter = new AvroRecordWriter(
      null,
      GenericData.get(),
      CodecFactory.nullCodec(),
      outputStreamSupplier,
      33);

    //Create a Generic record
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    StructuredRecord sourceRecord = StructuredRecord.builder(schema)
      .set("id", 1L)
      .set("name", "alice")
      .set("price", 1.2d)
      .setDate("dt", LocalDate.of(2018, 11, 11))
      .setTime("time", LocalTime.of(11, 11, 11))
      .set("timestamp", 1464181635000000L).build();
    GenericRecord sourceGenericRecord = new StructuredToAvroTransformer(schema).transform(sourceRecord);
    AvroKey<GenericRecord> record = new AvroKey(sourceGenericRecord);

    //Write using avroRecordWriter
    avroRecordWriter.write(record, Mockito.mock(NullWritable.class));
    avroRecordWriter.close(null);

    //Now read the avro record from the Output stream
    GenericDatumReader genericDatumReader = new GenericDatumReader<GenericRecord>(sourceGenericRecord.getSchema());
    SeekableByteArrayInput inputStream = new SeekableByteArrayInput(byteArrayOutputStream.toByteArray());
    DataFileReader dataFileReader = new DataFileReader<GenericRecord>(inputStream, genericDatumReader);

    //Check if it matches
    Assert.assertTrue(dataFileReader.iterator().hasNext());
    //Record read must match the sourceGenericRecord
    Assert.assertEquals(dataFileReader.iterator().next(), sourceGenericRecord);
  }


  /**
   * If there are no records, then the output stream supplier should not be called
   * If called then will throw exception
   *
   * @throws IOException
   */
  @Test
  public void testZeroWrites() throws IOException {
    Supplier<OutputStream> outputStreamSupplier = () -> {
      throw new IllegalStateException("This supplier should not be called");
    };

    AvroRecordWriter avroRecordWriter = new AvroRecordWriter(
      null,
      GenericData.get(),
      CodecFactory.nullCodec(),
      outputStreamSupplier,
      33);

    avroRecordWriter.close(null);
  }
}
