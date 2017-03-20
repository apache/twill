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
package org.apache.twill.internal.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.api.logging.LogThrowable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.Services;
import org.apache.twill.internal.json.ILoggingEventSerializer;
import org.apache.twill.internal.json.LogThrowableCodec;
import org.apache.twill.internal.json.StackTraceElementCodec;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A logback {@link Appender} for writing log events to Kafka.
 */
public final class KafkaAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

  private static final String PUBLISH_THREAD_NAME = "kafka-logger";

  private final AtomicReference<KafkaPublisher.Preparer> publisher;
  private final Runnable flushTask;
  /**
   * Rough count of how many entries are being buffered. It's just approximate, not exact.
   */
  private final AtomicInteger bufferedSize;

  private LogEventConverter eventConverter;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClient;
  private String zkConnectStr;
  private String hostname;
  private String runnableName;
  private String topic;
  private Queue<String> buffer;
  private int flushLimit = 20;
  private int flushPeriod = 100;
  private ScheduledExecutorService scheduler;

  public KafkaAppender() {
    publisher = new AtomicReference<>();
    flushTask = createFlushTask();
    bufferedSize = new AtomicInteger();
    buffer = new ConcurrentLinkedQueue<>();
  }

  /**
   * Sets the zookeeper connection string. Called by slf4j.
   */
  @SuppressWarnings("unused")
  public void setZookeeper(String zkConnectStr) {
    this.zkConnectStr = zkConnectStr;
  }

  /**
   * Sets the hostname. Called by slf4j.
   */
  @SuppressWarnings("unused")
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * Sets the runnableName.
   */
  @SuppressWarnings("unused")
  public void setRunnableName(String runnableName) {
    this.runnableName = runnableName;
  }

  /**
   * Sets the topic name for publishing logs. Called by slf4j.
   */
  @SuppressWarnings("unused")
  public void setTopic(String topic) {
    this.topic = topic;
  }

  /**
   * Sets the maximum number of cached log entries before performing an force flush. Called by slf4j.
   */
  @SuppressWarnings("unused")
  public void setFlushLimit(int flushLimit) {
    this.flushLimit = flushLimit;
  }

  /**
   * Sets the periodic flush time in milliseconds. Called by slf4j.
   */
  @SuppressWarnings("unused")
  public void setFlushPeriod(int flushPeriod) {
    this.flushPeriod = flushPeriod;
  }

  @Override
  public void start() {
    Preconditions.checkNotNull(zkConnectStr);

    eventConverter = new LogEventConverter(hostname, runnableName);
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory(PUBLISH_THREAD_NAME));

    zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnectStr).build(),
                                 RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));

    kafkaClient = new ZKKafkaClientService(zkClientService);
    Futures.addCallback(Services.chainStart(zkClientService, kafkaClient),
                        new FutureCallback<List<ListenableFuture<Service.State>>>() {
      @Override
      public void onSuccess(List<ListenableFuture<Service.State>> result) {
        for (ListenableFuture<Service.State> future : result) {
          Preconditions.checkState(Futures.getUnchecked(future) == Service.State.RUNNING,
                                   "Service is not running.");
        }
        addInfo("Kafka client started: " + zkConnectStr);
        scheduler.scheduleWithFixedDelay(flushTask, 0, flushPeriod, TimeUnit.MILLISECONDS);
      }

      @Override
      public void onFailure(Throwable t) {
        // Fail to talk to kafka. Other than logging, what can be done?
        addError("Failed to start kafka appender.", t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    scheduler.shutdownNow();
    Futures.getUnchecked(Services.chainStop(kafkaClient, zkClientService));
  }

  public void forceFlush() {
    try {
      scheduler.submit(flushTask).get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      addError("Failed to force log flush in 2 seconds.", e);
    }
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    buffer.offer(eventConverter.convert(eventObject));
    if (bufferedSize.incrementAndGet() >= flushLimit && publisher.get() != null) {
      // Try to do a extra flush
      scheduler.submit(flushTask);
    }
  }

  /**
   * Publishes buffered logs to Kafka, within the given timeout.
   *
   * @return Number of logs published.
   * @throws TimeoutException If timeout reached before publish completed.
   */
  private int publishLogs(long timeout, TimeUnit timeoutUnit) throws TimeoutException {
    List<ByteBuffer> logs = Lists.newArrayListWithExpectedSize(bufferedSize.get());

    for (String json : Iterables.consumingIterable(buffer)) {
      logs.add(Charsets.UTF_8.encode(json));
    }

    long backOffTime = timeoutUnit.toNanos(timeout) / 10;
    if (backOffTime <= 0) {
      backOffTime = 1;
    }

    try {
      Stopwatch stopwatch = new Stopwatch();
      stopwatch.start();
      long publishTimeout = timeout;

      do {
        try {
          int published = doPublishLogs(logs).get(publishTimeout, timeoutUnit);
          bufferedSize.addAndGet(-published);
          return published;
        } catch (ExecutionException e) {
          addError("Failed to publish logs to Kafka.", e);
          TimeUnit.NANOSECONDS.sleep(backOffTime);
          publishTimeout -= stopwatch.elapsedTime(timeoutUnit);
          stopwatch.reset();
          stopwatch.start();
        }
      } while (publishTimeout > 0);
    } catch (InterruptedException e) {
      addWarn("Logs publish to Kafka interrupted.", e);
    }
    return 0;
  }

  private ListenableFuture<Integer> doPublishLogs(Collection <ByteBuffer> logs) {
    // Nothing to publish, simply returns a completed future.
    if (logs.isEmpty()) {
      return Futures.immediateFuture(0);
    }

    // If the publisher is not available, tries to create one.
    KafkaPublisher.Preparer publisher = KafkaAppender.this.publisher.get();
    if (publisher == null) {
      try {
        KafkaPublisher.Preparer preparer = kafkaClient.getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED,
                                                                    Compression.SNAPPY).prepare(topic);
        KafkaAppender.this.publisher.compareAndSet(null, preparer);
        publisher = KafkaAppender.this.publisher.get();
      } catch (Exception e) {
        return Futures.immediateFailedFuture(e);
      }
    }

    for (ByteBuffer buffer : logs) {
      publisher.add(buffer, 0);
    }

    return publisher.send();
  }

  /**
   * Creates a {@link Runnable} that writes all logs in the buffer into kafka.
   * @return The Runnable task
   */
  private Runnable createFlushTask() {
    return new Runnable() {
      @Override
      public void run() {
        try {
          publishLogs(2L, TimeUnit.SECONDS);
        } catch (Exception e) {
          addError("Failed to push logs to Kafka. Log entries dropped.", e);
        }
      }
    };
  }

  /**
   * Helper class to convert {@link ILoggingEvent} into json string.
   */
  private static final class LogEventConverter {

    private final Gson gson;

    private LogEventConverter(String hostname, String runnableName) {
      gson = new GsonBuilder()
        .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
        .registerTypeAdapter(LogThrowable.class, new LogThrowableCodec())
        .registerTypeAdapter(ILoggingEvent.class, new ILoggingEventSerializer(hostname, runnableName))
        .create();
    }

    private String convert(ILoggingEvent event) {
      return gson.toJson(event, ILoggingEvent.class);
    }
  }
}
