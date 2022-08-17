# Migration steps to new structure

1. Checkout branch 
2. Apply all pending changes from develop, copying files over to the right destination
3. Run `mvn clean install -DskipTests` to ensure project builds.
    1. Known Issues
       1. [GCPUtils.java](google-cloud-common/src/main/java/io/cdap/plugin/gcp/common/GCPUtils.java) complaints about GSON dependency. [Fix here](https://github.com/data-integrations/google-cloud/commit/595e03c2355a24db9c132cbdf325308aebf0b00e#diff-ecbd6e9431d61eee2143b04d8b68d35c792ea40fded5b5d6fed1941f6d29ef27)
       2. [BigQuerySinkTest.java](google-cloud-bigquery/src/test/java/io/cdap/plugin/gcp/bigquery/sink/BigQuerySinkTest.java) complains about a Spark class not available. [Fix here](https://github.com/data-integrations/google-cloud/commit/595e03c2355a24db9c132cbdf325308aebf0b00e#diff-8b26805b83830ec280c4bd9ecb084afc76e03e1b420aeee18c774adc2c8daaef)
       3. [PubSubOutputFormat](google-cloud-pubsub/src/main/java/io/cdap/plugin/gcp/publisher/PubSubOutputFormat.java) complaints about imports. [Fix here](https://github.com/data-integrations/google-cloud/commit/595e03c2355a24db9c132cbdf325308aebf0b00e#diff-ddf71d4aca12ae1f28862ad3d2a10e6c59bb149536c8d924436943f4dc426d1e)
    2. Use [this PR](https://github.com/data-integrations/google-cloud/commit/595e03c2355a24db9c132cbdf325308aebf0b00e) or [this patch](https://github.com/data-integrations/google-cloud/commit/595e03c2355a24db9c132cbdf325308aebf0b00e.patch) to figure out solutions to compilation issues you will find.
4. Run `mvn clean install` to ensure tests pass.

# Resources

Original Migration commit can be [found here](https://github.com/data-integrations/google-cloud/commit/595e03c2355a24db9c132cbdf325308aebf0b00e)

# Issues

Most of the issues we had were related to Classloading for the plugins that depend on the GCS Hadoop client 
(Storage, Bigquery, Dataplex). 

If this dependency is exported by multiple modules, and 2 of the modules are present in the same pipeline, the pipeline
will fail because one of the 2 modules will complain that the class is not what it's supposed to be. 

If this dependency is not exported by any of the modules, the Spark driver/worker won't be able to find and initialize
the Hadoop GCS implementation. 