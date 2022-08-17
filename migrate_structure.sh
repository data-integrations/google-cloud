#/bin/bash

# Migrate BigQuery
rsync -a ./src/main/java/io/cdap/plugin/gcp/bigquery ./google-cloud-bigquery/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/bigquery ./google-cloud-bigquery/src/test/java/io/cdap/plugin/gcp/
find ./docs/ -name "BigQuery*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "BigQuery*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "BigQuery*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Bigtable
rsync -a ./src/main/java/io/cdap/plugin/gcp/bigtable ./google-cloud-bigtable/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/bigtable ./google-cloud-bigtable/src/test/java/io/cdap/plugin/gcp/
find ./docs/ -name "Bigtable*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "Bigtable*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "Bigtable*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Common
rsync -a ./src/main/java/io/cdap/plugin/gcp/common ./google-cloud-common/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/common ./google-cloud-common/src/test/java/io/cdap/plugin/gcp/
rsync -a ./src/main/java/io/cdap/plugin/gcp/crypto ./google-cloud-common/src/main/java/io/cdap/plugin/gcp/
mv src/main/java/io/cdap/plugin/gcp/gcs/GCSPath.java ./google-cloud-common/src/main/java/io/cdap/plugin/gcp/gcs/
mv src/main/java/io/cdap/plugin/gcp/gcs/ServiceAccountAccessTokenProvider.java ./google-cloud-common/src/main/java/io/cdap/plugin/gcp/gcs/

# Migrate Dataplex
rsync -a ./src/main/java/io/cdap/plugin/gcp/dataplex ./google-cloud-dataplex/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/dataplex ./google-cloud-dataplex/src/test/java/io/cdap/plugin/gcp/
find ./docs/ -name "Dataplex*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "Dataplex*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "Dataplex*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Datastore
rsync -a ./src/main/java/io/cdap/plugin/gcp/datastore ./google-cloud-datastore/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/datastore ./google-cloud-datastore/src/test/java/io/cdap/plugin/gcp/
find ./docs/ -name "Datastore*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "Datastore*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "Datastore*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Pubsub
rsync -a ./src/main/java/io/cdap/plugin/gcp/publisher ./google-cloud-pubsub/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/publisher ./google-cloud-pubsub/src/test/java/io/cdap/plugin/gcp/
rsync -a ./src/main/java/org/apache/spark/streaming/pubsub ./google-cloud-pubsub/src/main/java/org/apache/spark/streaming/pubsub
find ./docs/ -name "Google*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "Google*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "Google*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Spanner
rsync -a ./src/main/java/io/cdap/plugin/gcp/spanner ./google-cloud-spanner/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/spanner ./google-cloud-spanner/src/test/java/io/cdap/plugin/gcp/
find ./docs/ -name "Spanner*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "Spanner*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "Spanner*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Speech
rsync -a ./src/main/java/io/cdap/plugin/gcp/speech ./google-cloud-speech/src/main/java/io/cdap/plugin/gcp/
find ./docs/ -name "Speech*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "Speech*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "Speech*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate Storage
rsync -a ./src/main/java/io/cdap/plugin/gcp/gcs ./google-cloud-storage/src/main/java/io/cdap/plugin/gcp/
rsync -a ./src/test/java/io/cdap/plugin/gcp/gcs ./google-cloud-storage/src/test/java/io/cdap/plugin/gcp/
find ./docs/ -name "GCS*" | xargs -I{} cp {} ./google-cloud-bigquery/docs/
find ./icons/ -name "GCS*" | xargs -I{} cp {} ./google-cloud-bigquery/icons/
find ./widgets/ -name "GCS*" | xargs -I{} cp {} ./google-cloud-bigquery/widgets/

# Migrate E2E Tests
rsync -a ./src/e2e-test ./google-cloud-e2e-tests/src/

# delete base directories
rm -r ./src
rm -r ./docs
rm -r ./icons
rm -r ./widgets

# Delete .gitkeep files
find . -name ".gitkeep" | xargs -I{} rm {}