package io.cdap.plugin.gcp.gcs.actions;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.common.batch.action.Condition;
import io.cdap.plugin.common.batch.action.ConditionConfig;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;

import java.io.IOException;


/**
 * An action plugin that creates a DONE file in case of a succeeded, failed or completed pipeline.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSDoneFileMarker.NAME)
@Description("Creates a \"done\" or \"success\" indication in the form of a file in GCS.")
public class GCSDoneFileMarker extends Action {
    public static final String NAME = "GCSDoneFileMarker";
    private Config config;

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    }

    @Override
    public void run(ActionContext context) throws IOException {
        config.validate(context.getFailureCollector());

        Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
        if (isServiceAccountFilePath == null) {
            context.getFailureCollector().addFailure("Service account type is undefined.",
                    "Must be `filePath` or `JSON`");
            context.getFailureCollector().getOrThrowException();
            return;
        }
        StorageClient storageClient = StorageClient.create(config.getProject(), config.getServiceAccount(),
                isServiceAccountFilePath);

        storageClient.createDoneFileMarker(config.getPath());
    }

    private abstract class Config extends GCPConfig {
        public static final String NAME_PATH = "path";
        public static final String RUN_CONDITION = "runCondition";

        @Name(NAME_PATH)
        @Description("Path where the done file will get written.")
        @Macro
        private String path;

        @Name(RUN_CONDITION)
        @Description("Creates a done file marker on pipeline completion/success/failure. By default it takes completion as an option.")
        @Macro
        private String runCondition;

        public Config() {
            this.runCondition = Condition.SUCCESS.name();
        }


        public GCSPath getPath() {
            return GCSPath.from(path);
        }


        void validate(FailureCollector collector) {
            if (!this.containsMacro("runCondition")) {
                try {
                    (new ConditionConfig(this.runCondition)).validate();
                } catch (IllegalArgumentException var3) {
                    collector.addFailure(var3.getMessage(), (String) null).withConfigProperty("runCondition");
                }
            }

        }

    }

}
