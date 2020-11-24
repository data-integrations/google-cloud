package io.cdap.plugin.gcp.gcs.actions;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;

import java.io.IOException;

/**
 * An action plugin that creates a DONE file in case of a succeeded, failed or completed pipeline.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSDoneFileMarker.NAME)
@Description("Creates a \"done\" or \"success\" indication in the form of a file in GCS.")
public class GCSDoneFileMarker extends Action {
  public static final String NAME = "GCSDoneFileMarker";
  private GCSCopy.Config config;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
  }

  @Override
  public void run(ActionContext context) throws IOException {

  }

}
