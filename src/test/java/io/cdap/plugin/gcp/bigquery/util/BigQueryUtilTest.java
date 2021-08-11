package io.cdap.plugin.gcp.bigquery.util;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import javax.validation.constraints.AssertTrue;

import static org.mockito.ArgumentMatchers.anyString;

public class BigQueryUtilTest {

    @Test
    public void testValidateArraySchema() {
        String arrayField = "nullArray";
        Schema schema = Schema.recordOf("record",
                Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                Schema.Field.of(arrayField, Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.STRING)))));

        FailureCollector collector = Mockito.mock(FailureCollector.class);
        Mockito.when(collector.addFailure(anyString(), anyString())).thenReturn(new ValidationFailure("errorMessage"));

        Field bigQueryField = Field.newBuilder(arrayField, StandardSQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();
        Schema.Field recordField = schema.getField(arrayField);

        BigQueryUtil.validateFieldModeMatches(bigQueryField,recordField, false, collector);

        Mockito.verify(collector,Mockito.times(0)).addFailure(anyString(), anyString());
    }
}
