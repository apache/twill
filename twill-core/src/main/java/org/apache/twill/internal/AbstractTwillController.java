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
package org.apache.twill.internal;

import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.json.StackTraceElementCodec;
import org.apache.twill.internal.kafka.client.SimpleKafkaClient;
import org.apache.twill.internal.logging.LogEntryDecoder;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * A abstract base class for {@link org.apache.twill.api.TwillController} implementation that uses Zookeeper to controller a
 * running twill application.
 */
public abstract class AbstractTwillController extends AbstractZKServiceController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillController.class);
  private static final int MAX_KAFKA_FETCH_SIZE = 1048576;
  private static final long SHUTDOWN_TIMEOUT_MS = 2000;
  private static final long LOG_FETCH_TIMEOUT_MS = 5000;

  private final Queue<LogHandler> logHandlers;
  private final KafkaClient kafkaClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final LogPollerThread logPoller;

  public AbstractTwillController(RunId runId, ZKClient zkClient, Iterable<LogHandler> logHandlers) {
    super(runId, zkClient);
    this.logHandlers = new ConcurrentLinkedQueue<LogHandler>();
    this.kafkaClient = new SimpleKafkaClient(ZKClients.namespace(zkClient, "/" + runId.getId() + "/kafka"));
    this.discoveryServiceClient = new ZKDiscoveryService(zkClient);
    Iterables.addAll(this.logHandlers, logHandlers);
    this.logPoller = new LogPollerThread(runId, kafkaClient, logHandlers);
  }

  @Override
  protected void doStartUp() {
    if (!logHandlers.isEmpty()) {
      logPoller.start();
    }
  }

  @Override
  protected void doShutDown() {
    logPoller.terminate();
    try {
      // Wait for the poller thread to stop.
      logPoller.join(SHUTDOWN_TIMEOUT_MS);
    } catch (InterruptedException e) {
      LOG.warn("Joining of log poller thread interrupted.", e);
    }
  }

  @Override
  public final synchronized void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    if (!logPoller.isAlive()) {
      logPoller.start();
    }
  }

  @Override
  public final ServiceDiscovered discoverService(String serviceName) {
    return discoveryServiceClient.discover(serviceName);
  }

  @Override
  public final ListenableFuture<Integer> changeInstances(String runnable, int newCount) {
    return sendMessage(SystemMessages.setInstances(runnable, newCount), newCount);
  }

  private static final class LogPollerThread extends Thread {

    private final KafkaClient kafkaClient;
    private final Iterable<LogHandler> logHandlers;
    private volatile boolean running = true;

    LogPollerThread(RunId runId, KafkaClient kafkaClient, Iterable<LogHandler> logHandlers) {
      super("twill-log-poller-" + runId.getId());
      setDaemon(true);
      this.kafkaClient = kafkaClient;
      this.logHandlers = logHandlers;
    }

    @Override
    public void run() {
      LOG.info("Twill log poller thread '{}' started.", getName());
      kafkaClient.startAndWait();
      Gson gson = new GsonBuilder().registerTypeAdapter(LogEntry.class, new LogEntryDecoder())
        .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
        .create();

      while (running && !isInterrupted()) {
        long offset;
        try {
          // Get the earliest offset
          long[] offsets = kafkaClient.getOffset(Constants.LOG_TOPIC, 0, -2, 1).get(LOG_FETCH_TIMEOUT_MS,
                                                                                    TimeUnit.MILLISECONDS);
          // Should have one entry
          offset = offsets[0];
        } catch (Throwable t) {
          // Keep retrying
          LOG.warn("Failed to fetch offsets from Kafka. Retrying.", t);
          continue;
        }

        // Now fetch log messages from Kafka
        Iterator<FetchedMessage> messageIterator = kafkaClient.consume(Constants.LOG_TOPIC, 0,
                                                                       offset, MAX_KAFKA_FETCH_SIZE);
        try {
          while (messageIterator.hasNext()) {
            String json = Charsets.UTF_8.decode(messageIterator.next().getBuffer()).toString();
            try {
              LogEntry entry = gson.fromJson(json, LogEntry.class);
              if (entry != null) {
                invokeHandlers(entry);
              }
            } catch (Exception e) {
              LOG.error("Failed to decode log entry {}", json, e);
            }
          }
        } catch (Throwable t) {
          LOG.warn("Exception while fetching log message from Kafka. Retrying.", t);
          continue;
        }
      }

      kafkaClient.stopAndWait();
      LOG.info("Twill log poller thread stopped.");
    }

    void terminate() {
      running = false;
      interrupt();
    }

    private void invokeHandlers(LogEntry entry) {
      for (LogHandler handler : logHandlers) {
        handler.onLog(entry);
      }
    }
  }
}
