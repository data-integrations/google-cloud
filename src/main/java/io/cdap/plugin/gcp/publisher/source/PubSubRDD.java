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

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.util.Collections;

/**
 * PubSubRDD that returns PubSubMessage for each partition.
 * Partition count is same as the number of readers.
 */
public class PubSubRDD extends RDD<PubSubMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubRDD.class);

  private final Time batchTime;
  private final long readDuration;
  private final PubSubSubscriberConfig config;
  private final boolean autoAcknowledge;

  PubSubRDD(SparkContext sparkContext, Time batchTime, long readDuration, PubSubSubscriberConfig config,
            boolean autoAcknowledge) {
    super(sparkContext, scala.collection.JavaConverters.asScalaBuffer(Collections.emptyList()),
          scala.reflect.ClassTag$.MODULE$.apply(PubSubMessage.class));
    this.batchTime = batchTime;
    this.readDuration = readDuration;
    this.config = config;
    this.autoAcknowledge = autoAcknowledge;
  }

  @Override
  public Iterator<PubSubMessage> compute(Partition split, TaskContext context) {
    LOG.debug("Computing for partition {} .", split.index());
    return new PubSubRDDIterator(config, context, batchTime, readDuration, autoAcknowledge);
  }

  @Override
  public Partition[] getPartitions() {
    int partitionCount = config.getNumberOfReaders();
    Partition[] partitions = new Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      final int index = i;
      partitions[i] = () -> index;
    }
    return partitions;
  }
}
