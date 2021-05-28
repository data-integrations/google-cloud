package io.cdap.plugin.gcp.bigquery.util;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BigQueryUtilTest {

    @Test
    public void testValidateArraySchema() {
        String arrayField = "nullArray";
        Schema schema = Schema.recordOf("record",
                Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                Schema.Field.of(arrayField, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

        FailureCollector collector = Mockito.mock(FailureCollector.class);

        ValidationFailure failure  = BigQueryUtil.validateArraySchema(
                schema.getField(arrayField).getSchema(), arrayField, collector
        );

        Assert.assertNull(failure);
    }
}
