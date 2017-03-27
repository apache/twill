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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.BasicTwillContext;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ContainerInfo;
import org.apache.twill.internal.EnvContainerInfo;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.ServiceMain;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.logging.Loggings;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

/**
 * The main class for launching a {@link TwillContainerService}.
 */
public final class TwillContainerMain extends ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(TwillContainerMain.class);

  private final TwillRuntimeSpecification twillRuntimeSpec;

  /**
   * Main method for launching a {@link TwillContainerService} which runs
   * a {@link org.apache.twill.api.TwillRunnable}.
   */
  public static void main(String[] args) throws Exception {
    File twillSpecFile = new File(Constants.Files.RUNTIME_CONFIG_JAR, Constants.Files.TWILL_SPEC);
    TwillRuntimeSpecification twillRuntimeSpec = TwillRuntimeSpecificationAdapter.create().fromJson(twillSpecFile);

    new TwillContainerMain(twillRuntimeSpec).doMain();
  }

  private TwillContainerMain(TwillRuntimeSpecification twillRuntimeSpec) {
    this.twillRuntimeSpec = twillRuntimeSpec;
  }

  private void doMain() throws Exception {
    // Try to load the secure store from localized file, which AM requested RM to localize it for this container.
    loadSecureStore();
    RunId appRunId = twillRuntimeSpec.getTwillAppRunId();
    RunId runId = RunIds.fromString(System.getenv(EnvKeys.TWILL_RUN_ID));
    String runnableName = System.getenv(EnvKeys.TWILL_RUNNABLE_NAME);
    int instanceId = Integer.valueOf(System.getenv(EnvKeys.TWILL_INSTANCE_ID));
    int instanceCount = Integer.valueOf(System.getenv(EnvKeys.TWILL_INSTANCE_COUNT));
    Map<String, String> defaultLogLevels = twillRuntimeSpec.getLogLevels().get(runnableName);
    Map<String, String> dynamicLogLevels = loadLogLevels().get(runnableName);

    Map<String, String> logLevels = new HashMap<>();
    if (defaultLogLevels != null) {
      logLevels.putAll(defaultLogLevels);
    } else {
      defaultLogLevels = ImmutableMap.of();
    }
    if (dynamicLogLevels != null) {
      logLevels.putAll(dynamicLogLevels);
    }

    ZKClientService zkClientService = createZKClient();
    ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClientService);

    ZKClient appRunZkClient = getAppRunZKClient(zkClientService, appRunId);

    TwillRunnableSpecification runnableSpec =
      twillRuntimeSpec.getTwillSpecification().getRunnables().get(runnableName).getRunnableSpecification();
    ContainerInfo containerInfo = new EnvContainerInfo();
    Arguments arguments = decodeArgs();
    BasicTwillContext context = new BasicTwillContext(
      runId, appRunId, containerInfo.getHost(),
      arguments.getRunnableArguments().get(runnableName).toArray(new String[0]),
      arguments.getArguments().toArray(new String[0]),
      runnableSpec, instanceId, discoveryService, discoveryService, appRunZkClient,
      instanceCount, containerInfo.getMemoryMB(), containerInfo.getVirtualCores()
    );

    ZKClient containerZKClient = getContainerZKClient(zkClientService, appRunId, runnableName);
    Configuration conf = new YarnConfiguration(new HdfsConfiguration(new Configuration()));
    TwillContainerService service = new TwillContainerService(context, containerInfo, containerZKClient,
                                                              runId, runnableSpec, getClassLoader(), conf,
                                                              createAppLocation(conf, twillRuntimeSpec.getFsUser(),
                                                                                twillRuntimeSpec.getTwillAppDir()),
                                                              defaultLogLevels, logLevels);
    doMain(
      service,
      zkClientService,
      new LogFlushService(),
      new TwillZKPathService(containerZKClient, runId),
      new CloseableServiceWrapper(discoveryService)
    );
  }

  private static void loadSecureStore() throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    File file = new File(Constants.Files.CREDENTIALS);
    if (file.exists()) {
      Credentials credentials = new Credentials();
      try (DataInputStream input = new DataInputStream(new FileInputStream(file))) {
        credentials.readTokenStorageStream(input);
      }

      UserGroupInformation.getCurrentUser().addCredentials(credentials);
      LOG.info("Secure store updated from {}", file);
    }
  }

  /**
   * Returns a {@link ZKClient} that namespaced under the given run id.
   */
  private static ZKClient getAppRunZKClient(ZKClient zkClient, RunId appRunId) {
    return ZKClients.namespace(zkClient, String.format("/%s", appRunId));
  }

  private static ZKClient getContainerZKClient(ZKClient zkClient, RunId appRunId, String runnableName) {
    return ZKClients.namespace(zkClient, String.format("/%s/runnables/%s", appRunId, runnableName));
  }

  /**
   * Returns the ClassLoader for the runnable.
   */
  private static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      return ClassLoader.getSystemClassLoader();
    }
    return classLoader;
  }

  private static Map<String, Map<String, String>> loadLogLevels() throws IOException {
    File file = new File(Constants.Files.LOG_LEVELS);
    if (file.exists()) {
      try (Reader reader = Files.newReader(file, Charsets.UTF_8)) {
        Gson gson = new GsonBuilder().serializeNulls().create();
        return gson.fromJson(reader, new TypeToken<Map<String, Map<String, String>>>() { }.getType());
      }
    }
    return new HashMap<>();
  }

  private static Arguments decodeArgs() throws IOException {
    return ArgumentsCodec.decode(
      Files.newReaderSupplier(new File(Constants.Files.RUNTIME_CONFIG_JAR, Constants.Files.ARGUMENTS), Charsets.UTF_8));
  }

  @Override
  protected String getHostname() {
    return System.getenv(EnvKeys.YARN_CONTAINER_HOST);
  }

  @Override
  protected TwillRuntimeSpecification getTwillRuntimeSpecification() {
    return twillRuntimeSpec;
  }

  @Override
  protected String getRunnableName() {
    return System.getenv(EnvKeys.TWILL_RUNNABLE_NAME);
  }

  /**
   * Simple service that force flushing logs on stop.
   */
  private static final class LogFlushService extends AbstractService {

    @Override
    protected void doStart() {
      // No-op
      notifyStarted();
    }

    @Override
    protected void doStop() {
      Loggings.forceFlush();
      notifyStopped();
    }
  }

  /**
   * A wrapper to adapt a {@link AutoCloseable} to a {@link ServiceMain}. The service has no-op during start up
   * and will call the {@link AutoCloseable#close()} on shutdown.
   */
  private static final class CloseableServiceWrapper extends AbstractIdleService {

    private final AutoCloseable closeable;

    private CloseableServiceWrapper(AutoCloseable closeable) {
      this.closeable = closeable;
    }

    @Override
    protected void startUp() throws Exception {
      // no-op
    }

    @Override
    protected void shutDown() throws Exception {
      closeable.close();
    }
  }
}
