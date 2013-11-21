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

import org.apache.twill.common.Services;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.Compression;
import org.apache.twill.internal.kafka.client.SimpleKafkaClient;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
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
  private static KafkaClient kafkaClient;

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    // Extract the kafka.tgz and start the kafka server
    kafkaServer = new EmbeddedKafkaServer(extractKafka(), generateKafkaConfig(zkServer.getConnectionStr()));
    kafkaServer.startAndWait();

    zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();

    kafkaClient = new SimpleKafkaClient(zkClientService);
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

    Iterator<FetchedMessage> consumer = kafkaClient.consume(topic, 0, 0, 1048576);
    int count = 0;
    long startTime = System.nanoTime();
    while (count < 30 && consumer.hasNext() && secondsPassed(startTime, TimeUnit.NANOSECONDS) < 5) {
      LOG.info(Charsets.UTF_8.decode(consumer.next().getBuffer()).toString());
      count++;
    }

    Assert.assertEquals(30, count);
  }

  @Test (timeout = 10000)
  public void testOffset() throws Exception {
    String topic = "testOffset";

    // Initial earliest offset should be 0.
    long[] offsets = kafkaClient.getOffset(topic, 0, -2, 10).get();
    Assert.assertArrayEquals(new long[]{0L}, offsets);

    // Publish some messages
    Thread publishThread = createPublishThread(kafkaClient, topic, Compression.NONE, "Testing", 2000);
    publishThread.start();
    publishThread.join();

    // Fetch earliest offset, should still be 0.
    offsets = kafkaClient.getOffset(topic, 0, -2, 10).get();
    Assert.assertArrayEquals(new long[]{0L}, offsets);

    // Fetch latest offset
    offsets = kafkaClient.getOffset(topic, 0, -1, 10).get();
    Iterator<FetchedMessage> consumer = kafkaClient.consume(topic, 0, offsets[0], 1048576);

    // Publish one more message, the consumer should see the new message being published.
    publishThread = createPublishThread(kafkaClient, topic, Compression.NONE, "Testing", 1, 3000);
    publishThread.start();
    publishThread.join();

    // Should see the last message being published.
    Assert.assertTrue(consumer.hasNext());
    Assert.assertEquals("3000 Testing", Charsets.UTF_8.decode(consumer.next().getBuffer()).toString());
  }

  private Thread createPublishThread(final KafkaClient kafkaClient, final String topic,
                                     final Compression compression, final String message, final int count) {
    return createPublishThread(kafkaClient, topic, compression, message, count, 0);
  }

  private Thread createPublishThread(final KafkaClient kafkaClient, final String topic, final Compression compression,
                                     final String message, final int count, final int base) {
    return new Thread() {
      public void run() {
        PreparePublish preparePublish = kafkaClient.preparePublish(topic, compression);
        for (int i = 0; i < count; i++) {
          preparePublish.add(((base + i) + " " + message).getBytes(Charsets.UTF_8), 0);
        }
        Futures.getUnchecked(preparePublish.publish());
      }
    };
  }

  private long secondsPassed(long startTime, TimeUnit startUnit) {
    return TimeUnit.SECONDS.convert(System.nanoTime() - TimeUnit.NANOSECONDS.convert(startTime, startUnit),
                                    TimeUnit.NANOSECONDS);
  }

  private static File extractKafka() throws IOException, ArchiveException, CompressorException {
    File kafkaExtract = TMP_FOLDER.newFolder();
    InputStream kakfaResource = KafkaTest.class.getClassLoader().getResourceAsStream("kafka-0.7.2.tgz");
    ArchiveInputStream archiveInput = new ArchiveStreamFactory()
      .createArchiveInputStream(ArchiveStreamFactory.TAR,
                                new CompressorStreamFactory()
                                  .createCompressorInputStream(CompressorStreamFactory.GZIP, kakfaResource));

    try {
      ArchiveEntry entry = archiveInput.getNextEntry();
      while (entry != null) {
        File file = new File(kafkaExtract, entry.getName());
        if (entry.isDirectory()) {
          file.mkdirs();
        } else {
          ByteStreams.copy(archiveInput, Files.newOutputStreamSupplier(file));
        }
        entry = archiveInput.getNextEntry();
      }
    } finally {
      archiveInput.close();
    }
    return kafkaExtract;
  }

  private static Properties generateKafkaConfig(String zkConnectStr) throws IOException {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("log.dir", TMP_FOLDER.newFolder().getAbsolutePath());
    prop.setProperty("zk.connect", zkConnectStr);
    prop.setProperty("num.threads", "8");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("log.flush.interval", "1000");
    prop.setProperty("max.socket.request.bytes", "104857600");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("log.default.flush.scheduler.interval.ms", "1000");
    prop.setProperty("zk.connectiontimeout.ms", "1000000");
    prop.setProperty("socket.receive.buffer", "1048576");
    prop.setProperty("enable.zookeeper", "true");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("brokerid", "0");
    prop.setProperty("socket.send.buffer", "1048576");
    prop.setProperty("num.partitions", "1");
    // Use a really small file size to force some flush to happen
    prop.setProperty("log.file.size", "1024");
    prop.setProperty("log.default.flush.interval.ms", "1000");
    return prop;
  }
}
