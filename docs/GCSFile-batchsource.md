# Google Cloud Storage File Reader

Description
-----------
This plugin reads objects from a path in a Google Cloud Storage bucket.

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.
You can use Cloud Storage for a range of scenarios including serving website content,
storing data for archival and disaster recovery,
or distributing large data objects to users via direct download.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Use Connection:** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection:** Name of the connection to use. Project and service account information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Project ID:** Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account Type:** Service account type, file path where the service account is located or the JSON content of 
the service account.

**Service Account File Path:** Path on the local file system of the service account key. Can be set to 'auto-detect'.

**Service Account JSON:** Contents of the service account JSON file.

**Path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.
For example, `gs://<bucket>/path/to/directory/`.
An asterisk ("\*") can be used as a wildcard to match a filename pattern.
If no files are found or matched, the pipeline will fail.

**Format:** Format of the data to read.
The format must be one of 'avro', 'blob', 'csv', 'delimited', 'json', 'parquet', 'text', 'tsv', 'xls', or the
name of any format plugin that you have deployed to your environment.
If the format is a macro, only the pre-packaged formats can be used.
If the format is 'blob', every input file will be read into a separate record.
The 'blob' format also requires a schema that contains a field named 'body' of type 'bytes'.
If the format is 'text', the schema must contain a field named 'body' of type 'string'.

**Sample Size:** The maximum number of rows that will get investigated for automatic data type detection. 
The default value is 1000.

**Override:** A list of columns with the corresponding data types for whom the automatic data type detection gets
skipped. 

**Terminate If Empty Row:** Specify whether to stop reading after encountering the first empty row. Defaults to false.

**Select Sheet Using:** Select the sheet by name or number. Default is 'Sheet Number'.

**Sheet Value:** The name/number of the sheet to read from. If not specified, the first sheet will be read.
Sheet Number are 0 based, ie first sheet is 0.

**Delimiter:** Delimiter to use when the format is 'delimited'. This will be ignored for other formats.

**Use First Row as Header:** Whether to use first row as header. Supported formats are 'text', 'csv', 'tsv', 'delimited', 'xls'.

**Enable Quoted Values:** Whether to treat content between quotes as a value. This value will only be used if the format
is 'csv', 'tsv' or 'delimited'. For example, if this is set to true, a line that looks like `1, "a, b, c"` will output two fields.
The first field will have `1` as its value and the second will have `a, b, c` as its value. The quote characters will be trimmed.
The newline delimiter cannot be within quotes.

It also assumes the quotes are well enclosed. The left quote will match the first following quote right before the delimiter. If there is an
unenclosed quote, an error will occur.

**Service Account:**  - service account key used for authorization

* **File Path:** Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON:** Contents of the service account JSON file.

**Maximum Split Size:** Maximum size in bytes for each input partition.
Smaller partitions will increase the level of parallelism, but will require more resources and overhead.
The default value is 128MB.

**Minimum Split Size:** Minimum size in bytes for each input partition.

**Regex Path Filter:** Regular expression that file paths must match in order to be included in the input.
The full file path is compared, not just the file name.
If no value is given, no file filtering will be done.
For example, a regex of .+\.csv will read only files that end in '.csv'.

See https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html for more information about 
the regular expression syntax

**Path Field:** Output field to place the path of the file that the record was read from.
If not specified, the file path will not be included in output records.
If specified, the field must exist in the output schema as a string.

**Path Filename Only:** Whether to only use the filename instead of the URI of the file path when a path field is given.
The default value is false.

**Read Files Recursively:** Whether files are to be read recursively from the path. The default value is false.

**Allow Empty Input:** Whether to allow an input that does not exist. When false, the source will fail the run if the 
input does not exist. When true, the run will not fail and the source will not generate any output.
The default value is false.

**File System Properties:** Additional properties to use with the InputFormat when reading the data.

**File Encoding:** The character encoding for the file(s) to be read. The default encoding is UTF-8.

**Schema:** Output schema. If a Path Field is set, it must be present in the schema as a string.

**Data File Encrypted:** Whether files are encrypted. The default value is false. 
If it is set to true, files will be decrypted using the Streaming AEAD provided by the 
[Google Tink library](https://github.com/google/tink). Each data file needs to be accompanied with a metadata file
that contains the cipher information. For example, an encrypted data file at 
`gs://<bucket>/path/to/directory/file1.csv.enc` should have a metadata file at
`gs://<bucket>/path/to/directory/file1.csv.enc.metadata`.
 
The metadata file contains a JSON object with the following properties:

| Property | Description |
|----------|-------------| 
| kms      | The Cloud KMS URI that was used to encrypt the Data Encryption Key |
| aad      | The Base64 encoded Additional Authenticated Data used in the encryption |
| keyset   | A JSON object representing the serialized keyset information from the Tink library |

For example:
```json
{
    "kms": "gcp-kms://projects/my-key-project/locations/us-west1/keyRings/my-key-ring/cryptoKeys/mykey",
    "aad": "73iT4SUJBM24umXecCCf3A==",
    "keyset": {
        "keysetInfo": {
            "primaryKeyId": 602257784,
            "keyInfo": [{
                "typeUrl": "type.googleapis.com/google.crypto.tink.AesGcmHkdfStreamingKey",
                "outputPrefixType": "RAW",
                "keyId": 602257784,
                "status": "ENABLED"
            }]
        },
        "encryptedKeyset": "CiQAz5HH+nUA0Zuqnz4LCnBEVTHS72s/zwjpcnAMIPGpW6kxLggSrAEAcJKHmXeg8kfJ3GD4GuFeWDZzgGn3tfolk6Yf5d7rxKxDEChIMWJWGhWlDHbBW5B9HqWfKx2nQWSC+zjM8FLefVtPYrdJ8n6Eg8ksAnSyXmhN5LoIj6az3XBugtXvCCotQHrBuyoDY+j5ZH9J4tm/bzrLEjCdWAc+oAlhsUAV77jZhowJr6EBiyVuRVfcwLwiscWkQ9J7jjHc7ih9HKfnqAZmQ6iWP36OMrEn"
    }
}
```

**Encryption Metadata File Suffix:** The file name suffix for the encryption metadata file. The default value is `.metadata`.