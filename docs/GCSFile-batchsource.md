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

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.
For example, `gs://<bucket>/path/to/directory/`.

**Format:** Format of the data to read.
The format must be one of 'avro', 'blob', 'csv', 'delimited', 'json', 'parquet', 'text', or 'tsv'.
If the format is 'blob', every input file will be read into a separate record.
The 'blob' format also requires a schema that contains a field named 'body' of type 'bytes'.
If the format is 'text', the schema must contain a field named 'body' of type 'string'.

**Delimiter:** Delimiter to use when the format is 'delimited'. This will be ignored for other formats.

**Skip Header** Whether to skip the first line of each file. Supported formats are 'text', 'csv', 'tsv', 'delimited'.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Maximum Split Size:** Maximum size in bytes for each input partition.
Smaller partitions will increase the level of parallelism, but will require more resources and overhead.
The default value is 128MB.

**Path Field:** Output field to place the path of the file that the record was read from.
If not specified, the file path will not be included in output records.
If specified, the field must exist in the output schema as a string.

**Path Filename Only:** Whether to only use the filename instead of the URI of the file path when a path field is given.
The default value is false.

**Read Files Recursively:** Whether files are to be read recursively from the path. The default value is false.

**File System Properties:** Additional properties to use with the InputFormat when reading the data.

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