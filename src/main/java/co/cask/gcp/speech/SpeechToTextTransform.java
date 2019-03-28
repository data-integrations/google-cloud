/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcp.speech;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
import com.google.cloud.speech.v1.RecognitionAudio;
import com.google.cloud.speech.v1.RecognitionConfig;
import com.google.cloud.speech.v1.RecognizeResponse;
import com.google.cloud.speech.v1.SpeechClient;
import com.google.cloud.speech.v1.SpeechRecognitionAlternative;
import com.google.cloud.speech.v1.SpeechRecognitionResult;
import com.google.cloud.speech.v1.SpeechSettings;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Google Cloud Speech-To-Text Transform which transcripts audio
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(SpeechToTextTransform.NAME)
@Description(SpeechToTextTransform.DESCRIPTION)
public class SpeechToTextTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "SpeechToText";
  public static final String DESCRIPTION = "Converts audio files to text by applying powerful neural network models.";
  private SpeechTransformConfig config;
  private Schema outputSchema = null;
  private static final Schema SPEECH =
    Schema.recordOf("speech",
                    Schema.Field.of("transcript", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("confidence", Schema.nullableOf(Schema.of(Schema.Type.FLOAT)))
    );
  private RecognitionConfig recognitionConfig;
  private SpeechClient speech;

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    configurer.getStageConfigurer().setOutputSchema(getSchema(inputSchema));
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    config.validate(context.getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    outputSchema = context.getOutputSchema();
    setRecognitionConfig();
    speech = SpeechClient.create(getSettings());
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    ByteString audioBytes = ByteString.copyFrom((byte[]) input.get(config.audioField));
    RecognitionAudio audio = RecognitionAudio.newBuilder()
      .setContent(audioBytes)
      .build();

    RecognizeResponse response = speech.recognize(recognitionConfig, audio);
    List<SpeechRecognitionResult> results = response.getResultsList();

    // if an output schema is available then use it or else use the schema for the given input record
    Schema currentSchema;
    if (outputSchema != null) {
      currentSchema = outputSchema;
    } else {
      currentSchema = getSchema(input.getSchema());
    }

    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(currentSchema);

    List<StructuredRecord> transcriptParts = new ArrayList<>();
    StringBuilder completeTranscript = new StringBuilder();

    for (SpeechRecognitionResult result : results) {
      if (config.getPartsField() != null) {
        addTranscriptWithConfidence(transcriptParts, result.getAlternativesList());
      }
      if (config.getTextField() != null) {
        completeTranscript.append(result.getAlternativesList().get(0).getTranscript());
      }
    }

    if (config.getPartsField() != null) {
      outputBuilder.set(config.getPartsField(), transcriptParts);
    }
    if (config.getTextField() != null) {
      outputBuilder.set(config.getTextField(), completeTranscript.toString());
    }
    copyFields(input, outputBuilder);

    emitter.emit(outputBuilder.build());
  }

  @Override
  public void destroy() {
    super.destroy();
    try {
      speech.close();
    } catch (Exception e) {
      // no-op
    }
  }

  private Schema getSchema(Schema inputSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    if (inputSchema.getFields() != null) {
      fields.addAll(inputSchema.getFields());
    }
    boolean hasTranscriptField = false;
    // one of the output field in which the transcript will be specified must be specified
    if (config.transcriptionPartsField != null) {
      fields.add(Schema.Field.of(config.transcriptionPartsField, Schema.arrayOf(SPEECH)));
      hasTranscriptField = true;
    }

    if (config.transcriptionTextField != null) {
      fields.add(Schema.Field.of(config.transcriptionTextField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
      hasTranscriptField = true;
    }

    if (!hasTranscriptField) {
      throw new IllegalArgumentException("Either 'Transcript Parts Field' or 'Transcript Text Field' or both must be " +
                                           "specified.");
    }
    return Schema.recordOf("record", fields);
  }

  private SpeechSettings getSettings() throws IOException {
    SpeechSettings.Builder builder = SpeechSettings.newBuilder();
    if (config.getServiceAccountFilePath() != null) {
      builder.setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
    }
    return builder.build();
  }

  private void setRecognitionConfig() {
    recognitionConfig = RecognitionConfig.newBuilder()
      .setEncoding(getAudioEncoding())
      .setSampleRateHertz(config.getSampleRate())
      .setProfanityFilter(config.shouldFilterProfanity())
      .setLanguageCode(config.getLanguage())
      .build();
  }

  private RecognitionConfig.AudioEncoding getAudioEncoding() {
    RecognitionConfig.AudioEncoding encoding = RecognitionConfig.AudioEncoding.LINEAR16;
    if (config.encoding.equalsIgnoreCase("amr")) {
      encoding = RecognitionConfig.AudioEncoding.AMR;
    } else if (config.encoding.equalsIgnoreCase("amr_wb")) {
      encoding = RecognitionConfig.AudioEncoding.AMR_WB;
    } else if (config.encoding.equalsIgnoreCase("flac")) {
      encoding = RecognitionConfig.AudioEncoding.FLAC;
    } else if (config.encoding.equalsIgnoreCase("mulaw")) {
      encoding = RecognitionConfig.AudioEncoding.MULAW;
    } else if (config.encoding.equalsIgnoreCase("ogg_opus")) {
      encoding = RecognitionConfig.AudioEncoding.OGG_OPUS;
    }
    return encoding;
  }

  private void copyFields(StructuredRecord input, StructuredRecord.Builder outputBuilder) {
    // copy other schema field values
    List<Schema.Field> fields = input.getSchema().getFields();
    if (fields != null) {
      for (Schema.Field field : fields) {
        outputBuilder.set(field.getName(), input.get(field.getName()));
      }
    }
  }

  private void addTranscriptWithConfidence(List<StructuredRecord> speechArray,
                                           List<SpeechRecognitionAlternative> alternatives) {
    for (SpeechRecognitionAlternative alternative : alternatives) {
      float confidence = alternative.getConfidence();
      String transcript = alternative.getTranscript();
      StructuredRecord.Builder speech = StructuredRecord.builder(SPEECH);
      speech.set("confidence", confidence);
      speech.set("transcript", transcript);
      speechArray.add(speech.build());
    }
  }

  private static class SpeechTransformConfig extends PluginConfig {
    @Macro
    @Name("audiofield")
    @Description("Name of field containing binary audio file data")
    private String audioField;

    @Macro
    @Description("Audio encoding of the data sent in the audio message. All encodings support\n" +
      "only 1 channel (mono) audio. Only `FLAC` and `WAV` include a header that\n" +
      "describes the bytes of audio that follow the header. The other encodings\n" +
      "are raw audio bytes with no header.")
    private String encoding;

    @Macro
    @Name("samplerate")
    @Description("Sample rate in Hertz of the audio data sent in all `RecognitionAudio` messages. Valid values are: " +
      "8000-48000. 16000 is optimal. For best results, set the sampling rate of the audio source to 16000 Hz. If " +
      "that's not possible, use the native sample rate of the audio source (instead of re-sampling).")
    private String sampleRate;

    @Macro
    @Description("Whether to mask profanity, replacing all but the initial character in each masked word with " +
      "asterisks. For example, 'f***'.")
    private String profanity;

    @Macro
    @Description("The language of the supplied audio as a [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) " +
      "language tag. Example: \"en-US\". See [Language Support](https://cloud.google.com/speech/docs/languages) for a" +
      " list of the currently supported language codes.")
    private String language;

    @Macro
    @Nullable
    @Description("The name of the field to store all the different chunks of transcription with all the " +
      "different possibility and their confidence score. Defaults to 'parts'")
    private String transcriptionPartsField;

    @Macro
    @Nullable
    @Description("If a field name is specified then the transcription with highest confidence score will be stored as" +
      "text.")
    private String transcriptionTextField;

    @Name("serviceFilePath")
    @Description("Path on the local file system of the service account key used for authorization. Can be set to " +
      "'auto-detect' when running on a Dataproc cluster. When running on other clusters, the file must be present on " +
      "every node in the cluster.")
    @Macro
    private String serviceAccountFilePath;

    @Nullable
    public String getServiceAccountFilePath() {
      if (containsMacro("serviceFilePath") || serviceAccountFilePath == null ||
        serviceAccountFilePath.isEmpty() || GCPConfig.AUTO_DETECT.equals(serviceAccountFilePath)) {
        return null;
      }
      return serviceAccountFilePath;
    }

    @Nullable
    public String getPartsField() {
      if (containsMacro("transcriptionPartsField") || transcriptionPartsField == null ||
        transcriptionPartsField.isEmpty()) {
        return null;
      }
      return transcriptionPartsField;
    }

    @Nullable
    public String getTextField() {
      if (containsMacro("transcriptionTextField") || transcriptionTextField == null ||
        transcriptionTextField.isEmpty()) {
        return null;
      }
      return transcriptionTextField;
    }

    @Nullable
    public String getAudioField() {
      if (containsMacro("audiofield")) {
        return null;
      }
      return audioField;
    }

    public boolean shouldFilterProfanity() {
      return profanity.equalsIgnoreCase("true");
    }

    public String getLanguage() {
      return language == null ? "en-US" : language;
    }

    @Nullable
    public Integer getSampleRate() {
      if (containsMacro("sampleRate")) {
        return null;
      }
      return Integer.parseInt(sampleRate);
    }

    private void validate(@Nullable Schema inputSchema) {
      if (inputSchema != null) {
        // If a inputSchema and audioField is available then verify that the schema does contain the given audioField
        // and that the type is byte array. This will allow to fail fast i.e. during deployment time.
        String audioFieldName = getAudioField();
        if (audioFieldName != null && inputSchema.getField(audioFieldName) == null) {
          throw new IllegalArgumentException(String.format("Field %s does not exists in the input schema.",
                                                           audioFieldName));
        }
        Schema.Field field = inputSchema.getField(audioFieldName);
        Schema fieldSchema = field.getSchema();
        Schema.Type fieldType =
          fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (fieldType != Schema.Type.BYTES) {
          throw new IllegalArgumentException(String.format("Field '%s' must be of type 'bytes' but is of type " +
                                                             "'%s'.", field.getName(), fieldType));
        }
        if (getTextField() == null && getPartsField() == null) {
          throw new IllegalArgumentException("At least one of 'Transcript Parts Field' or 'Transcript Text Field' " +
                                               "must be specified.");
        }
        Set<String> fields = inputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toSet());
        if (getTextField() != null && fields.contains(getTextField())) {
          throw new IllegalArgumentException(String.format("Input schema contains given Transcription Text Field " +
                                                             "'%s'. Please provide a different name.", getTextField()));
        }
        if (getPartsField() != null && fields.contains(getPartsField())) {
          throw new IllegalArgumentException(String.format("Input schema contains given Transcription Parts Field " +
                                                             "'%s'. Please provide a different name.",
                                                           getPartsField()));
        }
      }

      if (getSampleRate() != null && (getSampleRate() < 8000 || getSampleRate() > 48000)) {
        throw new IllegalArgumentException("Sample Rate value must be between 8000 to 48000");
      }
    }
  }
}
