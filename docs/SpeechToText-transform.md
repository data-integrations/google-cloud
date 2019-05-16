
#### **Description**

Speech-to-Text coverts audio files to text using Google Speech-to-Text.

#### **Properties**

Following are properties used to configure this plugin

* **Audio Field**

  Name of the input field which contains the raw audio data in bytes.

* **Audio Encoding**

  Audio encoding of the data sent in the audio message. All encodings support only 1 channel (mono)
audio. Only 'FLAC' and 'WAV' include a header that describes the bytes of audio that follow the header.
The other encodings are raw audio bytes with no header.

* **Sampling Rate**:

  Sample rate in Hertz of the audio data sent in all 'RecognitionAudio' messages.
Valid values are: 8000-48000. 16000 is optimal. For best results, set the sampling rate of the audio source to
16000 Hz. If that's not possible, use the native sample rate of the audio source (instead of re-sampling).

* **Profanity**:

  Whether to attempt filtering profanities, replacing all but the initial character in each filtered
word with asterisks, e.g. "f***". If set to `false`, profanities won't be filtered out.

* **Language**:

  The language of the supplied audio as a [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt)
language tag. Example: "en-US". See [Language Support](https://cloud.google.com/speech/docs/languages) for a list of
the currently supported language codes.

* **Transcription Parts Field**:

  The field to store the transcription parts. It will be an array of records. Each record
in the array represents one part of the full audio data and will contain the transcription and confidence for that part.

* **Transcription Text Field**:

  The field to store the transcription of the full audio data. It is generated using the
transcription for each part with the highest confidence.

#### **Credentials**

If the plugin is run in GCP environment, the service account file path does not need to be
specified and can be set to 'auto-detect'. Credentials will be automatically read from the GCP environment.
A path to a service account key must be provided when not running in GCP. The service account
key can be found on the Dashboard in the Cloud Platform Console. Ensure that the account key has permission
to access resource.

* **Service Account File Path**

  Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running in GCP. When running on outside GCP,
the file must be present on every node were pipeline runs.

