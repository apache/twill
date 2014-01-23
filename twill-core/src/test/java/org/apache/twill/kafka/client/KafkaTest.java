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
package org.apache.twill.kafka.client;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Services;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaTest {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTest.class);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static ZKClientService zkClientService;
  private static KafkaClientService kafkaClient;

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    // Extract the kafka.tgz and start the kafka server
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr()));
    kafkaServer.startAndWait();

    zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();

    kafkaClient = new ZKKafkaClientService(zkClientService);
    Services.chainStart(zkClientService, kafkaClient).get();
  }

  @AfterClass
  public static void finish() throws Exception {
    Services.chainStop(kafkaClient, zkClientService).get();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void testKafkaClient() throws Exception {
    String topic = "testClient";

    Thread t1 = createPublishThread(kafkaClient, topic, Compression.GZIP, "GZIP Testing message", 10);
    Thread t2 = createPublishThread(kafkaClient, topic, Compression.NONE, "Testing message", 10);

    t1.start();
    t2.start();

    Thread t3 = createPublishThread(kafkaClient, topic, Compression.SNAPPY, "Snappy Testing message", 10);
    t2.join();
    t3.start();

    final CountDownLatch latch = new CountDownLatch(30);
    final CountDownLatch stopLatch = new CountDownLatch(1);
    Cancellable cancel = kafkaClient.getConsumer().prepare().add(topic, 0, 0).consume(new KafkaConsumer
      .MessageCallback() {
      @Override
      public void onReceived(Iterator<FetchedMessage> messages) {
        while (messages.hasNext()) {
          LOG.info(Charsets.UTF_8.decode(messages.next().getPayload()).toString());
          latch.countDown();
        }
      }

      @Override
      public void finished() {
        stopLatch.countDown();
      }
    });

    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    cancel.cancel();
    Assert.assertTrue(stopLatch.await(1, TimeUnit.SECONDS));
  }

  private Thread createPublishThread(final KafkaClient kafkaClient, final String topic,
                                     final Compression compression, final String message, final int count) {
    return createPublishThread(kafkaClient, topic, compression, message, count, 0);
  }

  private Thread createPublishThread(final KafkaClient kafkaClient, final String topic, final Compression compression,
                                     final String message, final int count, final int base) {
    return new Thread() {
      public void run() {
        KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, compression);
        KafkaPublisher.Preparer preparer = publisher.prepare(topic);
        for (int i = 0; i < count; i++) {
          preparer.add(Charsets.UTF_8.encode((base + i) + " " + message), 0);
        }
        Futures.getUnchecked(preparer.send());
      }
    };
  }

  private long secondsPassed(long startTime, TimeUnit startUnit) {
    return TimeUnit.SECONDS.convert(System.nanoTime() - TimeUnit.NANOSECONDS.convert(startTime, startUnit),
                                    TimeUnit.NANOSECONDS);
  }


  private static Properties generateKafkaConfig(String zkConnectStr) throws IOException {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("log.dir", TMP_FOLDER.newFolder().getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");

    // Use a really small file size to force some flush to happen
    prop.setProperty("log.file.size", "1024");
    prop.setProperty("log.default.flush.interval.ms", "1000");
    return prop;
  }
}
