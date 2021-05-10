package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class BigQuerySinkUtilsTest {

    @Test
    public void testGenerateTableFieldSchema() {
        String fieldName = "arrayOfRecords";
        Schema schema = Schema.recordOf("record",
                Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                Schema.Field.of(fieldName,
                        Schema.arrayOf(
                                Schema.recordOf("innerRecord",
                                        Schema.Field.of("field1", Schema.of(Schema.Type.STRING))
                                )
                        )
                )
        );

        BigQueryTableFieldSchema bqTableFieldSchema =
                BigQuerySinkUtils.generateTableFieldSchema(schema.getField(fieldName));

        List<BigQueryTableFieldSchema> fields = bqTableFieldSchema.getFields();
        Assert.assertEquals(fields.size(), 1);

        BigQueryTableFieldSchema expectedSchema = new BigQueryTableFieldSchema();
        expectedSchema.setType("STRING");
        expectedSchema.setMode("REQUIRED");
        expectedSchema.setName("field1");

        Assert.assertEquals(fields.get(0), expectedSchema);
    }

    @Test
    public void testGenerateTableFieldSchemaNullable() {
        String fieldName = "arrayOfRecords";
        Schema schema = Schema.recordOf("record",
                Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                Schema.Field.of(fieldName,
                        Schema.nullableOf(
                                Schema.arrayOf(
                                        Schema.recordOf("innerRecord",
                                                Schema.Field.of("field1", Schema.of(Schema.Type.STRING))
                                        )
                                )
                        )
                )
        );

        BigQueryTableFieldSchema bqTableFieldSchema =
                BigQuerySinkUtils.generateTableFieldSchema(schema.getField(fieldName));

        List<BigQueryTableFieldSchema> fields = bqTableFieldSchema.getFields();
        Assert.assertEquals(fields.size(), 1);

        BigQueryTableFieldSchema expectedSchema = new BigQueryTableFieldSchema();
        expectedSchema.setType("STRING");
        expectedSchema.setMode("REQUIRED");
        expectedSchema.setName("field1");

        Assert.assertEquals(fields.get(0), expectedSchema);


    }

}
