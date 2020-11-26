package io.cdap.plugin.gcp.gcs.actions;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.common.batch.action.Condition;
import io.cdap.plugin.common.batch.action.ConditionConfig;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.StorageClient;

import java.io.IOException;


/**
 * An post action plugin that creates a DONE file in case of a succeeded, failed or completed pipeline.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(GCSDoneFileMarker.NAME)
@Description("Creates a \"done\" or \"success\" file after the pipeline is finished.")
public class GCSDoneFileMarker extends PostAction {
    public static final String NAME = "GCSDoneFileMarker";
    public Config config;

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        config.validate(pipelineConfigurer.getStageConfigurer().getFailureCollector());
    }

    @Override
    public void run(BatchActionContext context) throws IOException {
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

    private static class Config extends GCPConfig {
        public static final String NAME_PATH = "path";
        public static final String RUN_CONDITION = "runCondition";

        @Name(NAME_PATH)
        @Description("Path where the done file will get written.")
        @Macro
        private String path;

        @Name(RUN_CONDITION)
        @Description("Creates a done file marker on pipeline completion/success/failure. By default it takes" +
                " completion as an option.")
        @Macro
        private String runCondition;

        Config() {
            super();
            this.runCondition = Condition.SUCCESS.name();
        }

        public GCSPath getPath() {
            return GCSPath.from(path);
        }

        void validate(FailureCollector collector) {
            if (!this.containsMacro(RUN_CONDITION)) {
                try {
                    (new ConditionConfig(this.runCondition)).validate();
                } catch (IllegalArgumentException var3) {
                    collector.addFailure(var3.getMessage(), (   String) null).withConfigProperty(RUN_CONDITION);
                }
            }

            if (!containsMacro(NAME_PATH)) {
                try {
                    getPath();
                } catch (IllegalArgumentException e) {
                    collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH);
                }
            }
        }
    }
}
