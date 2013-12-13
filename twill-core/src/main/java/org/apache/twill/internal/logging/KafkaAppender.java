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

import org.apache.twill.common.Services;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.kafka.client.Compression;
import org.apache.twill.internal.kafka.client.SimpleKafkaClient;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.PreparePublish;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import ch.qos.logback.classic.pattern.ClassOfCallerConverter;
import ch.qos.logback.classic.pattern.FileOfCallerConverter;
import ch.qos.logback.classic.pattern.LineOfCallerConverter;
import ch.qos.logback.classic.pattern.MethodOfCallerConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.AppenderBase;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.stream.JsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public final class KafkaAppender extends AppenderBase<ILoggingEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAppender.class);

  private final LogEventConverter eventConverter;
  private final AtomicReference<PreparePublish> publisher;
  private final Runnable flushTask;
  /**
   * Rough count of how many entries are being buffered. It's just approximate, not exact.
   */
  private final AtomicInteger bufferedSize;

  private ZKClientService zkClientService;
  private KafkaClient kafkaClient;
  private String zkConnectStr;
  private String hostname;
  private String topic;
  private Queue<String> buffer;
  private int flushLimit = 20;
  private int flushPeriod = 100;
  private ScheduledExecutorService scheduler;

  public KafkaAppender() {
    eventConverter = new LogEventConverter();
    publisher = new AtomicReference<PreparePublish>();
    flushTask = createFlushTask();
    bufferedSize = new AtomicInteger();
    buffer = new ConcurrentLinkedQueue<String>();
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

    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("kafka-logger"));

    zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnectStr).build(),
                                 RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));

    kafkaClient = new SimpleKafkaClient(zkClientService);
    Futures.addCallback(Services.chainStart(zkClientService, kafkaClient), new FutureCallback<Object>() {
      @Override
      public void onSuccess(Object result) {
        LOG.info("Kafka client started: " + zkConnectStr);
        publisher.set(kafkaClient.preparePublish(topic, Compression.SNAPPY));
        scheduler.scheduleWithFixedDelay(flushTask, 0, flushPeriod, TimeUnit.MILLISECONDS);
      }

      @Override
      public void onFailure(Throwable t) {
        // Fail to talk to kafka. Other than logging, what can be done?
        LOG.error("Failed to start kafka client.", t);
      }
    });

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
      publishLogs().get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Failed to publish last batch of log.", e);
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

  private ListenableFuture<Integer> publishLogs() {
    // If the publisher is not available, simply returns a completed future.
    PreparePublish publisher = KafkaAppender.this.publisher.get();
    if (publisher == null) {
      return Futures.immediateFuture(0);
    }

    int count = 0;
    for (String json : Iterables.consumingIterable(buffer)) {
      publisher.add(Charsets.UTF_8.encode(json), 0);
      count++;
    }
    // Nothing to publish, simply returns a completed future.
    if (count == 0) {
      return Futures.immediateFuture(0);
    }

    bufferedSize.set(0);
    final int finalCount = count;
    return Futures.transform(publisher.publish(), new Function<Object, Integer>() {
      @Override
      public Integer apply(Object input) {
        return finalCount;
      }
    });
  }

  /**
   * Creates a {@link Runnable} that writes all logs in the buffer into kafka.
   * @return The Runnable task
   */
  private Runnable createFlushTask() {
    return new Runnable() {
      @Override
      public void run() {
        Futures.addCallback(publishLogs(), new FutureCallback<Integer>() {
          @Override
          public void onSuccess(Integer result) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Log entries published, size=" + result);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.error("Failed to push logs to kafka. Log entries dropped.", t);
          }
        });
      }
    };
  }

  /**
   * Helper class to convert {@link ILoggingEvent} into json string.
   */
  private final class LogEventConverter {

    private final ClassOfCallerConverter classNameConverter = new ClassOfCallerConverter();
    private final MethodOfCallerConverter methodConverter = new MethodOfCallerConverter();
    private final FileOfCallerConverter fileConverter = new FileOfCallerConverter();
    private final LineOfCallerConverter lineConverter = new LineOfCallerConverter();

    private String convert(ILoggingEvent event) {
      StringWriter result = new StringWriter();
      JsonWriter writer = new JsonWriter(result);

      try {
        try {
          writer.beginObject();
          writer.name("name").value(event.getLoggerName());
          writer.name("host").value(hostname);
          writer.name("timestamp").value(Long.toString(event.getTimeStamp()));
          writer.name("level").value(event.getLevel().toString());
          writer.name("className").value(classNameConverter.convert(event));
          writer.name("method").value(methodConverter.convert(event));
          writer.name("file").value(fileConverter.convert(event));
          writer.name("line").value(lineConverter.convert(event));
          writer.name("thread").value(event.getThreadName());
          writer.name("message").value(event.getFormattedMessage());
          writer.name("stackTraces");
          encodeStackTraces(event.getThrowableProxy(), writer);

          writer.endObject();
        } finally {
          writer.close();
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      return result.toString();
    }

    private void encodeStackTraces(IThrowableProxy throwable, JsonWriter writer) throws IOException {
      writer.beginArray();
      try {
        if (throwable == null) {
          return;
        }

        for (StackTraceElementProxy stackTrace : throwable.getStackTraceElementProxyArray()) {
          writer.beginObject();

          StackTraceElement element = stackTrace.getStackTraceElement();
          writer.name("className").value(element.getClassName());
          writer.name("method").value(element.getMethodName());
          writer.name("file").value(element.getFileName());
          writer.name("line").value(element.getLineNumber());

          writer.endObject();
        }
      } finally {
        writer.endArray();
      }
    }
  }
}
