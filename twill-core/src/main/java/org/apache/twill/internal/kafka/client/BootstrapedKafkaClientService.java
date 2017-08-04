/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.kafka.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of {@link KafkaClientService} with bootstrap servers string
 * Created by SFilippov on 11.07.2017.
 */
public class BootstrapedKafkaClientService extends AbstractIdleService implements KafkaClientService {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BootstrapedKafkaClientService.class);

  private final String bootstrapServers;
  private final List<BetterKafkaPublisher> publishers;
  private BetterKafkaConsumer consumer;

  public BootstrapedKafkaClientService(String bootstrapServers) {
    Preconditions.checkNotNull(bootstrapServers, "Bootstrap server's list cannot be null");
    Preconditions.checkState(!bootstrapServers.isEmpty(), "Bootstrap server's list cannot be empty");
    this.bootstrapServers = bootstrapServers;
    this.publishers = Lists.newArrayList();
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Kafka consumer");
    consumer.stop();
    for (BetterKafkaPublisher publisher : publishers) {
      publisher.stop();
    }
    LOG.info("Kafka Consumer stopped");
  }

  @Override
  public KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression) {
    BetterKafkaPublisher publisher = new BetterKafkaPublisher(bootstrapServers, ack, compression);
    publishers.add(publisher);
    return publisher;
  }

  @Override
  public org.apache.twill.kafka.client.KafkaConsumer getConsumer() {
    if (consumer == null) {
      consumer = new BetterKafkaConsumer(bootstrapServers);
    }
    return consumer;
  }
}
