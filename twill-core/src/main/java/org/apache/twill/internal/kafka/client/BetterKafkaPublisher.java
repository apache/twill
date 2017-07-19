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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Created by SFilippov on 18.07.2017.
 */
public class BetterKafkaPublisher implements KafkaPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(BetterKafkaPublisher.class);

  private final KafkaProducer<Integer, ByteBuffer> kafkaProducer;

  public BetterKafkaPublisher(String bootstrapServers,
                              KafkaPublisher.Ack ack,
                              Compression compression) {
    final Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, Integer.toString(ack.getAck()));
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

    kafkaProducer = new KafkaProducer<>(props,
                                        new IntegerSerializer(),
                                        new ByteBufferSerializer());
    LOG.debug("Starting kafkaProducer {}", props);
  }

  @Override
  public Preparer prepare(String topic) {
    return new BetterPreparer(topic);
  }

  public void stop() {
    kafkaProducer.close();
  }


  private final class BetterPreparer implements Preparer {
    private final String topic;
    private final List<ProducerRecord<Integer, ByteBuffer>> messages;

    private BetterPreparer(String topic) {
      this.topic = topic;
      this.messages = Lists.newLinkedList();
    }

    @Override
    public Preparer add(ByteBuffer message, Object partitionKey) {
      messages.add(new ProducerRecord<Integer, ByteBuffer>(topic, Math.abs(partitionKey.hashCode()), message));
      return BetterPreparer.this;
    }

    @Override
    public ListenableFuture<Integer> send() {
      try {
        if (kafkaProducer == null) {
          return Futures.immediateFailedFuture(new IllegalStateException("No kafka producer available."));
        }

        List<ListenableFuture<RecordMetadata>> futures = Lists.newArrayList();
        for (ProducerRecord pr : messages) {
          futures.add(JdkFutureAdapters.listenInPoolThread(kafkaProducer.send(pr),
                                                           Executors.newSingleThreadExecutor()));
        }
        ListenableFuture<List<RecordMetadata>> listListenableFuture = Futures.allAsList(futures);
        return Futures.transform(listListenableFuture, new AsyncFunction<List<RecordMetadata>, Integer>() {
          @Override
          public ListenableFuture<Integer> apply(List<RecordMetadata> input) throws Exception {
            return Futures.immediateFuture(input.size());
          }
        });
      } catch (Exception e) {
        return Futures.immediateFailedFuture(e);
      } finally {
        messages.clear();
      }
    }
  }
}
