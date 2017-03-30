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
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.logging.KafkaAppender;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

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

  protected final void doMain(final Service mainService,
                              Service...prerequisites) throws Exception {
    // Only configure the log collection if it is enabled.
    if (getTwillRuntimeSpecification().isLogCollectionEnabled()) {
      configureLogger();
    }

    Service requiredServices = new CompositeService(prerequisites);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        mainService.stopAndWait();
      }
    });

    // Listener for state changes of the service
    ListenableFuture<Service.State> completion = Services.getCompletionFuture(mainService);
    Throwable initFailure = null;

    try {
      try {
        // Starts the service
        LOG.info("Starting service {}.", mainService);
        Futures.allAsList(Services.chainStart(requiredServices, mainService).get()).get();
        LOG.info("Service {} started.", mainService);
      } catch (Throwable t) {
        LOG.error("Exception when starting service {}.", mainService, t);
        initFailure = t;
      }

      try {
        if (initFailure == null) {
          completion.get();
          LOG.info("Service {} completed.", mainService);
        }
      } catch (Throwable t) {
        LOG.error("Exception thrown from service {}.", mainService, t);
        throw Throwables.propagate(t);
      }
    } finally {
      requiredServices.stopAndWait();

      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
      if (loggerFactory instanceof LoggerContext) {
        ((LoggerContext) loggerFactory).stop();
      }

      if (initFailure != null) {
        // Exit with the init fail exit code.
        System.exit(ContainerExitCodes.INIT_FAILED);
      }
    }
  }

  protected abstract String getHostname();

  /**
   * Returns the {@link TwillRuntimeSpecification} for this application.
   */
  protected abstract TwillRuntimeSpecification getTwillRuntimeSpecification();

  /**
   * Returns the name of the runnable that this running inside this process.
   */
  @Nullable
  protected abstract String getRunnableName();

  /**
   * Returns the {@link Location} for the application based on the app directory.
   */
  protected final Location createAppLocation(final Configuration conf, String fsUser, final URI appDir) {
    // Note: It's a little bit hacky based on the uri schema to create the LocationFactory, refactor it later.

    try {
      if ("file".equals(appDir.getScheme())) {
        return new LocalLocationFactory().create(appDir);
      }

      // If not file, assuming it is a FileSystem, hence construct with FileContextLocationFactory
      UserGroupInformation ugi;
      if (UserGroupInformation.isSecurityEnabled()) {
        ugi = UserGroupInformation.getCurrentUser();
      } else {
        ugi = UserGroupInformation.createRemoteUser(fsUser);
      }
      return ugi.doAs(new PrivilegedExceptionAction<Location>() {
        @Override
        public Location run() throws Exception {
          Configuration hConf = new Configuration(conf);
          final URI defaultURI;
          if (appDir.getAuthority() == null || appDir.getAuthority().isEmpty()) {
            // some FileSystems do not have authority. E.g. maprfs or s3 (similar to file:///)
            // need to handle URI differently for those
            defaultURI = new URI(appDir.getScheme(), "", "/", null, null);
          } else {
            defaultURI = new URI(appDir.getScheme(), appDir.getAuthority(), null, null, null);
          }
          hConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultURI.toString());
          return new FileContextLocationFactory(hConf).create(appDir);
        }
      });
    } catch (Exception e) {
      LOG.error("Failed to create application location for {}.", appDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a {@link ZKClientService}.
   */
  protected final ZKClientService createZKClient() {
    TwillRuntimeSpecification twillRuntimeSpec = getTwillRuntimeSpecification();

    return ZKClientServices.delegate(
      ZKClients.namespace(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(twillRuntimeSpec.getZkConnectStr()).build(),
            RetryStrategies.fixDelay(1, TimeUnit.SECONDS)
          )
        ), "/" + twillRuntimeSpec.getTwillAppName()
      )
    );
  }

  private void configureLogger() throws MalformedURLException, JoranException {
    // Check if SLF4J is bound to logback in the current environment
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext context = (LoggerContext) loggerFactory;

    ContextInitializer contextInitializer = new ContextInitializer(context);
    URL url = contextInitializer.findURLOfDefaultConfigurationFile(false);
    if (url == null) {
      // The logger context was not initialized using configuration file, initialize it with the logback template.
      File twillLogback = new File(Constants.Files.RUNTIME_CONFIG_JAR, Constants.Files.LOGBACK_TEMPLATE);
      if (twillLogback.exists()) {
        contextInitializer.configureByResource(twillLogback.toURI().toURL());
      }
    }

    // Attach the KafkaAppender to the root logger
    KafkaAppender kafkaAppender = new KafkaAppender();
    kafkaAppender.setName("KAFKA");
    kafkaAppender.setTopic(Constants.LOG_TOPIC);
    kafkaAppender.setHostname(getHostname());
    // The Kafka ZK Connection shouldn't be null as this method only get called if log collection is enabled
    kafkaAppender.setZookeeper(getTwillRuntimeSpecification().getKafkaZKConnect());
    String runnableName = getRunnableName();
    if (runnableName != null) {
      kafkaAppender.setRunnableName(runnableName);
    }

    kafkaAppender.setContext(context);
    kafkaAppender.start();

    context.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).addAppender(kafkaAppender);
  }

  /**
   * A simple service for creating/remove ZK paths needed for {@link AbstractTwillService}.
   */
  protected static class TwillZKPathService extends AbstractIdleService {

    protected static final long TIMEOUT_SECONDS = 5L;

    private static final Logger LOG = LoggerFactory.getLogger(TwillZKPathService.class);

    private final ZKClient zkClient;
    private final String path;

    public TwillZKPathService(ZKClient zkClient, RunId runId) {
      this.zkClient = zkClient;
      this.path = "/" + runId.getId();
    }

    @Override
    protected void startUp() throws Exception {
      LOG.info("Creating container ZK path: {}{}", zkClient.getConnectString(), path);
      ZKOperations.ignoreError(zkClient.create(path, null, CreateMode.PERSISTENT),
                               KeeperException.NodeExistsException.class, null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("Removing container ZK path: {}{}", zkClient.getConnectString(), path);
      ZKOperations.recursiveDelete(zkClient, path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }
}
