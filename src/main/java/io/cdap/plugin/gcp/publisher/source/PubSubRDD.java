/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.gcp.publisher.source;

import com.google.auth.Credentials;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.util.Collections;

/**
 * PubSubRDD to be used with streaming source
 */
public class PubSubRDD extends RDD<PubSubMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubRDD.class);
  private static final int DEFAULT_PARTITIONS = 1;

  private long readDuration;

  private PubSubSubscriberConfig config;
  private final boolean autoAcknowledge;
  private final Credentials credentials;

  PubSubRDD(SparkContext sparkContext, long readDuration,
            PubSubSubscriberConfig config, boolean autoAcknowledge, Credentials credentials) {
    super(sparkContext, scala.collection.JavaConverters.asScalaBuffer(Collections.emptyList()),
          scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));

    this.readDuration = readDuration;
    this.config = config;
    this.autoAcknowledge = autoAcknowledge;
    this.credentials = credentials;
  }

  @Override
  public Iterator<PubSubMessage> compute(Partition split, TaskContext context) {
    LOG.info("In compute for PubSubRDD, creating a new PubSubRDDIterator");
    return new PubSubRDDIterator(config, context, readDuration, autoAcknowledge, credentials);
  }

  @Override
  public Partition[] getPartitions() {
    int partitionCount = config.getNumberOfReaders() * DEFAULT_PARTITIONS;
    Partition[] partitions = new Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      final int index = i;
      partitions[i] = () -> index;
    }
    return partitions;
  }
}
