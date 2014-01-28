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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.common.Services;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.logging.KafkaAppender;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.util.concurrent.ExecutionException;

/**
 * Class for main method that starts a service.
 */
public abstract class ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMain.class);

  static {
    // This is to work around detection of HADOOP_HOME (HADOOP-9422)
    if (!System.getenv().containsKey("HADOOP_HOME") && System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", new File("").getAbsolutePath());
    }
  }

  protected final void doMain(final ZKClientService zkClientService,
                              final Service service) throws ExecutionException, InterruptedException {
    configureLogger();

    final String serviceName = service.toString();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        Services.chainStop(service, zkClientService);
      }
    });

    // Listener for state changes of the service
    ListenableFuture<Service.State> completion = Services.getCompletionFuture(service);

    // Starts the service
    LOG.info("Starting service {}.", serviceName);
    Futures.getUnchecked(Services.chainStart(zkClientService, service));
    LOG.info("Service {} started.", serviceName);
    try {
      completion.get();
      LOG.info("Service {} completed.", serviceName);
    } catch (Throwable t) {
      LOG.warn("Exception thrown from service {}.", serviceName, t);
      throw Throwables.propagate(t);
    } finally {
      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
      if (loggerFactory instanceof LoggerContext) {
        ((LoggerContext) loggerFactory).stop();
      }
    }
  }

  protected abstract String getHostname();

  protected abstract String getKafkaZKConnect();

  /**
   * Returns the {@link Location} for the application based on the env {@link EnvKeys#TWILL_APP_DIR}.
   */
  protected static Location createAppLocation(Configuration conf) {
    // Note: It's a little bit hacky based on the uri schema to create the LocationFactory, refactor it later.
    URI appDir = URI.create(System.getenv(EnvKeys.TWILL_APP_DIR));

    try {
      if ("file".equals(appDir.getScheme())) {
        return new LocalLocationFactory().create(appDir);
      }

      if ("hdfs".equals(appDir.getScheme())) {
        if (UserGroupInformation.isSecurityEnabled()) {
          return new HDFSLocationFactory(FileSystem.get(conf)).create(appDir);
        }

        String fsUser = System.getenv(EnvKeys.TWILL_FS_USER);
        if (fsUser == null) {
          throw new IllegalStateException("Missing environment variable " + EnvKeys.TWILL_FS_USER);
        }
        return new HDFSLocationFactory(FileSystem.get(FileSystem.getDefaultUri(conf), conf, fsUser)).create(appDir);
      }

      LOG.warn("Unsupported location type {}.", appDir);
      throw new IllegalArgumentException("Unsupported location type " + appDir);

    } catch (Exception e) {
      LOG.error("Failed to create application location for {}.", appDir);
      throw Throwables.propagate(e);
    }
  }

  private void configureLogger() {
    // Check if SLF4J is bound to logback in the current environment
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext context = (LoggerContext) loggerFactory;
    context.reset();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);

    try {
      File twillLogback = new File(Constants.Files.LOGBACK_TEMPLATE);
      if (twillLogback.exists()) {
        configurator.doConfigure(twillLogback);
      }
      new ContextInitializer(context).autoConfig();
    } catch (JoranException e) {
      throw Throwables.propagate(e);
    }
    doConfigure(configurator, getLogConfig(getLoggerLevel(context.getLogger(Logger.ROOT_LOGGER_NAME))));
  }

  private void doConfigure(JoranConfigurator configurator, String config) {
    try {
      configurator.doConfigure(new InputSource(new StringReader(config)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogConfig(String rootLevel) {
    return
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<configuration>\n" +
      "    <appender name=\"KAFKA\" class=\"" + KafkaAppender.class.getName() + "\">\n" +
      "        <topic>" + Constants.LOG_TOPIC + "</topic>\n" +
      "        <hostname>" + getHostname() + "</hostname>\n" +
      "        <zookeeper>" + getKafkaZKConnect() + "</zookeeper>\n" +
      "    </appender>\n" +
      "    <logger name=\"org.apache.twill.internal.logging\" additivity=\"false\" />\n" +
      "    <root level=\"" + rootLevel + "\">\n" +
      "        <appender-ref ref=\"KAFKA\"/>\n" +
      "    </root>\n" +
      "</configuration>";
  }

  private String getLoggerLevel(Logger logger) {
    if (logger instanceof ch.qos.logback.classic.Logger) {
      return ((ch.qos.logback.classic.Logger) logger).getLevel().toString();
    }

    if (logger.isTraceEnabled()) {
      return "TRACE";
    }
    if (logger.isDebugEnabled()) {
      return "DEBUG";
    }
    if (logger.isInfoEnabled()) {
      return "INFO";
    }
    if (logger.isWarnEnabled()) {
      return "WARN";
    }
    if (logger.isErrorEnabled()) {
      return "ERROR";
    }
    return "OFF";
  }
}
