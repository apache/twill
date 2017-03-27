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
package org.apache.twill.internal.container;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.Command;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.BasicTwillContext;
import org.apache.twill.internal.ContainerInfo;
import org.apache.twill.internal.ContainerLiveNodeData;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.internal.utils.Instances;
import org.apache.twill.internal.yarn.AbstractYarnTwillService;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * This class act as a yarn container and run a {@link org.apache.twill.api.TwillRunnable}.
 */
public final class TwillContainerService extends AbstractYarnTwillService {

  private static final Logger LOG = LoggerFactory.getLogger(TwillContainerService.class);

  private final TwillRunnableSpecification specification;
  private final ClassLoader classLoader;
  private final BasicTwillContext context;
  private final ContainerLiveNodeData containerLiveNodeData;
  private final Map<String, String> oldLogLevels;
  private final Map<String, String> defaultLogLevels;
  private ExecutorService commandExecutor;
  private TwillRunnable runnable;

  TwillContainerService(BasicTwillContext context, ContainerInfo containerInfo, ZKClient zkClient,
                        RunId runId, TwillRunnableSpecification specification, ClassLoader classLoader,
                        Configuration config, Location applicationLocation,
                        Map<String, String> defaultLogLevels, Map<String, String> logLevels) {
    super(zkClient, runId, config, applicationLocation);

    this.specification = specification;
    this.classLoader = classLoader;
    this.defaultLogLevels = ImmutableMap.copyOf(defaultLogLevels);
    this.oldLogLevels = new HashMap<>(defaultLogLevels);
    this.containerLiveNodeData = createLiveNodeData(
      containerInfo, isLoggerContext() ? logLevels : Collections.<String, String>emptyMap());
    this.context = context;
  }

  private ContainerLiveNodeData createLiveNodeData(ContainerInfo containerInfo,
                                                   Map<String, String> logLevels) {
    // if debugging is enabled, log the port and register it in service discovery.
    String debugPort = System.getProperty("twill.debug.port");
    if (debugPort != null) {
      LOG.info("JVM is listening for debugger on port {}", debugPort);
    }
    return new ContainerLiveNodeData(containerInfo.getId(),
                                     containerInfo.getHost().getCanonicalHostName(), debugPort, logLevels);
  }

  @Override
  protected Object getLiveNodeData() {
    return containerLiveNodeData;
  }

  @Override
  public ListenableFuture<String> onReceived(final String messageId, final Message message) {
    LOG.debug("Message received: {} {}.", messageId, message);

    if (handleSecureStoreUpdate(message)) {
      return Futures.immediateFuture(messageId);
    }

    final SettableFuture<String> result = SettableFuture.create();
    Command command = message.getCommand();
    if (message.getType() == Message.Type.SYSTEM
      && "instances".equals(command.getCommand()) && command.getOptions().containsKey("count")) {
      context.setInstanceCount(Integer.parseInt(command.getOptions().get("count")));
    }

    String commandStr = command.getCommand();
    if (message.getType() == Message.Type.SYSTEM) {
      boolean handled = false;
      if (SystemMessages.SET_LOG_LEVEL.equals(commandStr)) {
        // The options is a map from logger name to log level.
        setLogLevels(command.getOptions());
        handled = true;
      } else if (SystemMessages.RESET_LOG_LEVEL.equals(commandStr)) {
        // The options is a set of loggers to reset in the form of loggerName -> loggerName map.
        resetLogLevels(command.getOptions().keySet());
        handled = true;
      }

      if (handled) {
        updateLiveNode();
        return Futures.immediateFuture(messageId);
      }
    }

    commandExecutor.execute(new Runnable() {

      @Override
      public void run() {
        try {
          runnable.handleCommand(message.getCommand());
          result.set(messageId);
        } catch (Exception e) {
          result.setException(e);
        }
      }
    });
    return result;
  }

  /**
   * Sets the log levels for the given set of loggers.
   *
   * @param logLevels a map from logger name to log level to be set
   */
  private void setLogLevels(Map<String, String> logLevels) {
    for (Map.Entry<String, String> entry : logLevels.entrySet()) {
      String loggerName = entry.getKey();
      String logLevel = entry.getValue();

      // Setting the log level in logging system as well as in the container live node
      String oldLogLevel = setLogLevel(loggerName, logLevel);
      containerLiveNodeData.setLogLevel(loggerName, logLevel);

      if (!oldLogLevels.containsKey(loggerName)) {
        String defaultLogLevel = defaultLogLevels.get(loggerName);
        oldLogLevels.put(loggerName, defaultLogLevel == null ? oldLogLevel : defaultLogLevel);
      }
    }
  }

  /**
   * Resets the log levels for the given set of loggers. If the set is empty, reset all loggers that have been set
   * before.
   */
  private void resetLogLevels(Set<String> loggerNames) {
    Iterator<Map.Entry<String, String>> entryIterator = oldLogLevels.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Map.Entry<String, String> entry = entryIterator.next();

      String loggerName = entry.getKey();
      // logger name is empty if we are resetting all loggers.
      if (loggerNames.isEmpty() || loggerNames.contains(loggerName)) {
        String oldLogLevel = entry.getValue();

        setLogLevel(loggerName, oldLogLevel);
        if (oldLogLevel == null || !defaultLogLevels.containsKey(loggerName)) {
          containerLiveNodeData.removeLogLevel(loggerName);
          entryIterator.remove();
        } else {
          containerLiveNodeData.setLogLevel(loggerName, oldLogLevel);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doStart() throws Exception {
    for (Map.Entry<String, String> entry : containerLiveNodeData.getLogLevels().entrySet()) {
      String loggerName = entry.getKey();
      String oldLogLevel = setLogLevel(loggerName, entry.getValue());
      if (!defaultLogLevels.containsKey(loggerName)) {
        oldLogLevels.put(loggerName, oldLogLevel);
      }
    }

    commandExecutor = Executors.newSingleThreadExecutor(
      Threads.createDaemonThreadFactory("runnable-command-executor"));

    Class<?> runnableClass = classLoader.loadClass(specification.getClassName());
    Preconditions.checkArgument(TwillRunnable.class.isAssignableFrom(runnableClass),
                                "Class %s is not instance of TwillRunnable.", specification.getClassName());

    runnable = Instances.newInstance((Class<TwillRunnable>) runnableClass);
    runnable.initialize(context);
  }

  @Override
  protected void doRun() throws Exception {
    runnable.run();
  }

  @Override
  protected void doStop() throws Exception {
    commandExecutor.shutdownNow();
    try {
      runnable.destroy();
    } catch (Throwable t) {
      // Just catch the exception, not propagate it since it's already in shutdown sequence and
      // we want all twill services properly shutdown.
      LOG.warn("Exception when calling runnable.destroy.", t);
    } finally {
      context.stop();
    }
  }

  @Override
  protected void triggerShutdown() {
    try {
      runnable.stop();
    } catch (Throwable t) {
      LOG.error("Exception when stopping runnable.", t);
    }
  }

  private boolean isLoggerContext() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    return loggerFactory instanceof LoggerContext;
  }

  /**
   * Set the log level for the requested logger name.
   *
   * @param loggerName name of the logger
   * @param logLevel the log level to set to.
   * @return the current log level of the given logger. If there is no log level configured for the given logger or
   *         if the logging implementation is not logback, {@code null} will be returned
   */
  @Nullable
  private String setLogLevel(String loggerName, @Nullable String logLevel) {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      LOG.error("LoggerFactory is not a logback LoggerContext, cannot make the log level change");
      return null;
    }
    LoggerContext loggerContext = (LoggerContext) loggerFactory;

    ch.qos.logback.classic.Logger logger = loggerContext.getLogger(loggerName);
    LogEntry.Level oldLogLevel = logger.getLevel() == null ? null :
      LogEntry.Level.valueOf(logger.getLevel().toString());
    LOG.debug("Log level of {} changed from {} to {}", loggerName, oldLogLevel, logLevel);
    logger.setLevel(logLevel == null ? null : Level.toLevel(logLevel, Level.ERROR));

    return oldLogLevel == null ? null : oldLogLevel.name();
  }
}
