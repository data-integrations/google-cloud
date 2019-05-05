package io.cdap.plugin.gcp.dlp;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(type = Transform.PLUGIN_TYPE)
@Name(Mask.NAME)
@Description(Mask.DESCRIPTION)
public class Mask extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(Mask.class);
  public static final String NAME = "Mask";
  public static final String DESCRIPTION = "Mask fields";

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {

  }
}
