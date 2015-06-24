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
package org.apache.twill.internal.kafka;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A {@link com.google.common.util.concurrent.Service} implementation for running an instance of Kafka server in
 * the same process.
 */
public final class EmbeddedKafkaServer extends AbstractIdleService {

  public static final String START_TIMEOUT_RETRIES = "twill.kafka.start.timeout.retries";

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedKafkaServer.class);
  private static final String DEFAULT_START_TIMEOUT_RETRIES = "5";

  private final int startTimeoutRetries;
  private final KafkaConfig kafkaConfig;
  private KafkaServer server;

  public EmbeddedKafkaServer(Properties properties) {
    this.startTimeoutRetries = Integer.parseInt(properties.getProperty(START_TIMEOUT_RETRIES,
                                                                       DEFAULT_START_TIMEOUT_RETRIES));
    this.kafkaConfig = new KafkaConfig(properties);
  }

  @Override
  protected void startUp() throws Exception {
    int tries = 0;
    do {
      KafkaServer kafkaServer = createKafkaServer(kafkaConfig);
      try {
        kafkaServer.startup();
        server = kafkaServer;
      } catch (Exception e) {
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();

        Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof ZkTimeoutException) {
          // Potentially caused by race condition bug described in TWILL-139.
          LOG.warn("Timeout when connecting to ZooKeeper from KafkaServer. Attempt number {}.", tries, rootCause);
        } else {
          throw e;
        }
      }
    } while (server == null && ++tries < startTimeoutRetries);

    if (server == null) {
      throw new IllegalStateException("Failed to start Kafka server after " + tries + " attempts.");
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (server != null) {
      server.shutdown();
      server.awaitShutdown();
    }
  }

  private KafkaServer createKafkaServer(KafkaConfig kafkaConfig) {
    return new KafkaServer(kafkaConfig, new Time() {

      @Override
      public long milliseconds() {
        return System.currentTimeMillis();
      }

      @Override
      public long nanoseconds() {
        return System.nanoTime();
      }

      @Override
      public void sleep(long ms) {
        try {
          Thread.sleep(ms);
        } catch (InterruptedException e) {
          Thread.interrupted();
        }
      }
    });
  }
}
