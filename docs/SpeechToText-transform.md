# Google Cloud Speech-to-Text Transform

Description
-----------
This plugin converts audio files to text by using Google Cloud Speech-to-Text.

Google Cloud Speech-to-Text enables developers to convert audio to text by applying powerful neural network models.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Spanner.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Audio Field:** Name of the input field which contains the raw audio data in bytes.

**Audio Encoding**: Audio encoding of the data sent in the audio message. All encodings support only 1 channel (mono)
audio. Only 'FLAC' and 'WAV' include a header that describes the bytes of audio that follow the header.
The other encodings are raw audio bytes with no header.

**Sampling Rate**: Sample rate in Hertz of the audio data sent in all 'RecognitionAudio' messages.
Valid values are: 8000-48000. 16000 is optimal. For best results, set the sampling rate of the audio source to
16000 Hz. If that's not possible, use the native sample rate of the audio source (instead of re-sampling).

**Profanity**: Whether to attempt filtering profanities, replacing all but the initial character in each filtered
word with asterisks, e.g. "f***". If set to `false`, profanities won't be filtered out.

**Language**: The language of the supplied audio as a [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt)
language tag. Example: "en-US". See [Language Support](https://cloud.google.com/speech/docs/languages) for a list of
the currently supported language codes.

**Transcription Parts Field**: The field to store the transcription parts. It will be an array of records. Each record
in the array represents one part of the full audio data and will contain the transcription and confidence for that part.

**Transcription Text Field**: The field to store the transcription of the full audio data. It is generated using the
transcription for each part with the highest confidence.
