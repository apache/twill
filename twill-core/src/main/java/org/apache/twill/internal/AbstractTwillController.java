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

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.json.StackTraceElementCodec;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.logging.LogEntryDecoder;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A abstract base class for {@link org.apache.twill.api.TwillController} implementation that uses Zookeeper to controller a
 * running twill application.
 */
public abstract class AbstractTwillController extends AbstractZKServiceController implements TwillController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillController.class);

  private final Queue<LogHandler> logHandlers;
  private final KafkaClientService kafkaClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  private volatile Cancellable logCancellable;

  public AbstractTwillController(RunId runId, ZKClient zkClient, Iterable<LogHandler> logHandlers) {
    super(runId, zkClient);
    this.logHandlers = new ConcurrentLinkedQueue<LogHandler>();
    this.kafkaClient = new ZKKafkaClientService(ZKClients.namespace(zkClient, "/" + runId.getId() + "/kafka"));
    this.discoveryServiceClient = new ZKDiscoveryService(zkClient);
    Iterables.addAll(this.logHandlers, logHandlers);
  }

  @Override
  protected synchronized void doStartUp() {
    if (!logHandlers.isEmpty()) {
      kafkaClient.startAndWait();
      logCancellable = kafkaClient.getConsumer().prepare()
                                  .addFromBeginning(Constants.LOG_TOPIC, 0)
                                  .consume(new LogMessageCallback(logHandlers));
    }
  }

  @Override
  protected void doShutDown() {
    if (logCancellable != null) {
      logCancellable.cancel();
    }
    // Safe to call stop no matter when state the KafkaClientService is in.
    kafkaClient.stopAndWait();
  }

  @Override
  public final synchronized void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    if (logHandlers.size() == 1) {
      kafkaClient.startAndWait();
      logCancellable = kafkaClient.getConsumer().prepare()
        .addFromBeginning(Constants.LOG_TOPIC, 0)
        .consume(new LogMessageCallback(logHandlers));
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

  private static final class LogMessageCallback implements KafkaConsumer.MessageCallback {

    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(LogEntry.class, new LogEntryDecoder())
      .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
      .create();

    private final Iterable<LogHandler> logHandlers;

    private LogMessageCallback(Iterable<LogHandler> logHandlers) {
      this.logHandlers = logHandlers;
    }

    @Override
    public void onReceived(Iterator<FetchedMessage> messages) {
      while (messages.hasNext()) {
        String json = Charsets.UTF_8.decode(messages.next().getPayload()).toString();
        try {
          LogEntry entry = GSON.fromJson(json, LogEntry.class);
          if (entry != null) {
            invokeHandlers(entry);
          }
        } catch (Exception e) {
          LOG.error("Failed to decode log entry {}", json, e);
        }
      }
    }

    @Override
    public void finished() {
      // No-op
    }

    private void invokeHandlers(LogEntry entry) {
      for (LogHandler handler : logHandlers) {
        try {
          handler.onLog(entry);
        } catch (Throwable t) {
          LOG.warn("Exception while calling LogHandler {}", handler, t);
        }
      }
    }
  }
}
