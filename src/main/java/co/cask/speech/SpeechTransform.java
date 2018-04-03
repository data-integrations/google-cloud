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

package co.cask.speech;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.speech.v1.RecognitionAudio;
import com.google.cloud.speech.v1.RecognitionConfig;
import com.google.cloud.speech.v1.RecognizeResponse;
import com.google.cloud.speech.v1.SpeechClient;
import com.google.cloud.speech.v1.SpeechRecognitionAlternative;
import com.google.cloud.speech.v1.SpeechRecognitionResult;
import com.google.cloud.speech.v1.SpeechSettings;
import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.Path;

/**
 * Class description here.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(SpeechTransform.NAME)
@Description(SpeechTransform.DESCRIPTION)
public class SpeechTransform extends Transform<StructuredRecord, StructuredRecord> {
  public static final String NAME = "SpeechTranslator";
  public static final String DESCRIPTION = "Converts audio files to text by applying powerful neural network models.";
  private SpeechTransformConfig config;
  private Schema schema;
  private RecognitionConfig recognitionConfig;
  private SpeechClient speech;

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    try {
      schema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
      );
    }

    if (!config.containsMacro("field")) {
      Schema.Field field = schema.getField("field");
      if (field != null && field.getSchema().getType() == Schema.Type.BYTES) {
        throw new IllegalArgumentException(
          String.format(
            "Field '%s' should be a byte array.", field.getName()
          )
        );
      }
    }
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

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    try {
      schema = Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
      );
    }

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

    recognitionConfig = RecognitionConfig.newBuilder()
      .setEncoding(encoding)
      .setSampleRateHertz(Integer.parseInt(config.sampleRate))
      .setProfanityFilter(config.profanity.equals("true") ? true : false)
      .setLanguageCode(config.language == null ? "en-US" : config.language)
      .build();

    SpeechSettings settings = SpeechSettings.newBuilder()
      .setCredentialsProvider(
        new CredentialsProvider() {
          @Override
          public Credentials getCredentials() throws IOException {
            try {
              return loadServiceAccountCredentials(config.serviceAccountFilePath);
            } catch (Exception e) {
              throw new IOException(e.getMessage());
            }
          }
        }
      ).build();
    speech = SpeechClient.create(settings);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    ByteString audioBytes = ByteString.copyFrom((byte[])input.get(config.audio));
    RecognitionAudio audio = RecognitionAudio.newBuilder()
      .setContent(audioBytes)
      .build();

    RecognizeResponse response = speech.recognize(recognitionConfig, audio);
    List<SpeechRecognitionResult> results = response.getResultsList();

    Schema recordSchema = getSchema(schema);
    List<StructuredRecord> speechArray = new ArrayList<>();
    StructuredRecord.Builder builder = StructuredRecord.builder(recordSchema);
    for (SpeechRecognitionResult result: results) {
      List<SpeechRecognitionAlternative> alternatives = result.getAlternativesList();
      for (SpeechRecognitionAlternative alternative: alternatives) {
        float confidence = alternative.getConfidence();
        String transcript = alternative.getTranscript();
        StructuredRecord.Builder speech =
          StructuredRecord.builder(schema.getField("speeches").getSchema().getNonNullable().getComponentSchema());
        speech.set("confidence", confidence);
        speech.set("transcript", transcript);
        speechArray.add(speech.build());
      }
    }
    builder.set("speeches", speechArray);
    List<Schema.Field> fields = input.getSchema().getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (recordSchema.getField(name) != null) {
        builder.set(name, input.get(name));
      }
    }
    emitter.emit(builder.build());
  }

  public static ServiceAccountCredentials loadServiceAccountCredentials(String path) throws Exception {
    File credentialsPath = new File(path);
    if (!credentialsPath.exists()) {
      throw new FileNotFoundException("Service account file " + credentialsPath.getName() + " does not exist.");
    }
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      return ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (FileNotFoundException e) {
      throw new Exception(
        String.format("Unable to find service account file '%s'.", path)
      );
    } catch (IOException e) {
      throw new Exception(
        String.format(
          "Issue reading service account file '%s', please check permission of the file", path
        )
      );
    }
  }

  public static final class Request {
    private Map<String, Schema> inputSchemas;
  }

  @Path("getSchema")
  public Schema getSchema(Request request) {
    Set<Map.Entry<String, Schema>> entries = request.inputSchemas.entrySet();
    return getSchema(entries.iterator().next().getValue());
  }

  private Schema getSchema(Schema outSchema) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("transcript", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    fields.add(Schema.Field.of("confidence", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))));
    Schema.Field field = Schema.Field.of("speeches",
                                         Schema.nullableOf(
                      Schema.arrayOf(
                        Schema.recordOf("speech", fields)
                      )
                    )
    );

    List<Schema.Field> topLevel = new ArrayList<>();
    topLevel.add(field);
    if (outSchema != null) {
      topLevel.addAll(outSchema.getFields());
    }
    return Schema.recordOf("record", topLevel);
  }

  private static final class SpeechTransformConfig extends PluginConfig {
    @Name("schema")
    @Macro
    private String schema;

    @Name("audio")
    @Description("Field containing binary audio file data")
    @Macro
    private String audio;

    @Name("encoding")
    @Description("Audio encoding of the data sent in the audio message. All encodings support\n" +
      "only 1 channel (mono) audio. Only `FLAC` and `WAV` include a header that\n" +
      "describes the bytes of audio that follow the header. The other encodings\n" +
      "are raw audio bytes with no header.")
    @Macro
    private String encoding;

    @Name("samplerate")
    @Macro
    private String sampleRate;

    @Name("profanity")
    @Macro
    private String profanity;

    @Name("language")
    @Macro
    private String language;

    @Name("serviceFilePath")
    @Description("Service Account File Path")
    @Macro
    public String serviceAccountFilePath;
  }
}
