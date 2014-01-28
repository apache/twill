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
package org.apache.twill.internal.appmaster;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.twill.api.Command;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.AbstractTwillService;
import org.apache.twill.internal.Configs;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultTwillRunResources;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.TwillContainerLauncher;
import org.apache.twill.internal.ZKServiceDecorator;
import org.apache.twill.internal.json.LocalFileCodec;
import org.apache.twill.internal.json.TwillSpecificationAdapter;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.logging.Loggings;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.MessageCallback;
import org.apache.twill.internal.utils.Instances;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.yarn.YarnAMClient;
import org.apache.twill.internal.yarn.YarnAMClientFactory;
import org.apache.twill.internal.yarn.YarnContainerInfo;
import org.apache.twill.internal.yarn.YarnContainerStatus;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService extends AbstractTwillService {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);

  // Copied from org.apache.hadoop.yarn.security.AMRMTokenIdentifier.KIND_NAME since it's missing in Hadoop-2.0
  private static final Text AMRM_TOKEN_KIND_NAME = new Text("YARN_AM_RM_TOKEN");

  private final RunId runId;
  private final ZKClient zkClient;
  private final TwillSpecification twillSpec;
  private final ApplicationMasterLiveNodeData amLiveNode;
  private final ZKServiceDecorator serviceDelegate;
  private final RunningContainers runningContainers;
  private final ExpectedContainers expectedContainers;
  private final TrackerService trackerService;
  private final YarnAMClient amClient;
  private final String jvmOpts;
  private final int reservedMemory;
  private final EventHandler eventHandler;
  private final Location applicationLocation;

  private EmbeddedKafkaServer kafkaServer;
  private Queue<RunnableContainerRequest> runnableContainerRequests;
  private ExecutorService instanceChangeExecutor;

  public ApplicationMasterService(RunId runId, ZKClient zkClient, File twillSpecFile,
                                  YarnAMClientFactory amClientFactory, Location applicationLocation) throws Exception {
    super(applicationLocation);

    this.runId = runId;
    this.twillSpec = TwillSpecificationAdapter.create().fromJson(twillSpecFile);
    this.zkClient = zkClient;
    this.applicationLocation = applicationLocation;
    this.amClient = amClientFactory.create();
    this.credentials = createCredentials();
    this.jvmOpts = loadJvmOptions();
    this.reservedMemory = getReservedMemory();

    amLiveNode = new ApplicationMasterLiveNodeData(Integer.parseInt(System.getenv(EnvKeys.YARN_APP_ID)),
                                                   Long.parseLong(System.getenv(EnvKeys.YARN_APP_ID_CLUSTER_TIME)),
                                                   amClient.getContainerId().toString());

    serviceDelegate = new ZKServiceDecorator(zkClient, runId, createLiveNodeDataSupplier(),
                                             new ServiceDelegate(), new Runnable() {
      @Override
      public void run() {
        amClient.stopAndWait();
      }
    });
    expectedContainers = initExpectedContainers(twillSpec);
    runningContainers = initRunningContainers(amClient.getContainerId(), amClient.getHost());
    trackerService = new TrackerService(runningContainers.getResourceReport(), amClient.getHost());
    eventHandler = createEventHandler(twillSpec);
  }

  private String loadJvmOptions() throws IOException {
    final File jvmOptsFile = new File(Constants.Files.JVM_OPTIONS);
    if (!jvmOptsFile.exists()) {
      return "";
    }

    return CharStreams.toString(new InputSupplier<Reader>() {
      @Override
      public Reader getInput() throws IOException {
        return new FileReader(jvmOptsFile);
      }
    });
  }

  private int getReservedMemory() {
    String value = System.getenv(EnvKeys.TWILL_RESERVED_MEMORY_MB);
    if (value == null) {
      return Configs.Defaults.JAVA_RESERVED_MEMORY_MB;
    }
    try {
      return Integer.parseInt(value);
    } catch (Exception e) {
      return Configs.Defaults.JAVA_RESERVED_MEMORY_MB;
    }
  }

  private EventHandler createEventHandler(TwillSpecification twillSpec) {
    try {
      // Should be able to load by this class ClassLoader, as they packaged in the same jar.
      EventHandlerSpecification handlerSpec = twillSpec.getEventHandler();

      Class<?> handlerClass = getClass().getClassLoader().loadClass(handlerSpec.getClassName());
      Preconditions.checkArgument(EventHandler.class.isAssignableFrom(handlerClass),
                                  "Class {} does not implements {}",
                                  handlerClass, EventHandler.class.getName());
      return Instances.newInstance((Class<? extends EventHandler>) handlerClass);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Supplier<? extends JsonElement> createLiveNodeDataSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        return new Gson().toJsonTree(amLiveNode);
      }
    };
  }

  private RunningContainers initRunningContainers(ContainerId appMasterContainerId,
                                                  String appMasterHost) throws Exception {
    TwillRunResources appMasterResources = new DefaultTwillRunResources(
      0,
      appMasterContainerId.toString(),
      Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES)),
      Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_MEMORY_MB)),
      appMasterHost);
    String appId = appMasterContainerId.getApplicationAttemptId().getApplicationId().toString();
    return new RunningContainers(appId, appMasterResources);
  }

  private ExpectedContainers initExpectedContainers(TwillSpecification twillSpec) {
    Map<String, Integer> expectedCounts = Maps.newHashMap();
    for (RuntimeSpecification runtimeSpec : twillSpec.getRunnables().values()) {
      expectedCounts.put(runtimeSpec.getName(), runtimeSpec.getResourceSpecification().getInstances());
    }
    return new ExpectedContainers(expectedCounts);
  }

  private void doStart() throws Exception {
    LOG.info("Start application master with spec: " + TwillSpecificationAdapter.create().toJson(twillSpec));

    // initialize the event handler, if it fails, it will fail the application.
    eventHandler.initialize(new BasicEventHandlerContext(twillSpec.getEventHandler()));

    instanceChangeExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("instanceChanger"));

    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig());

    // Must start tracker before start AMClient
    LOG.info("Starting application master tracker server");
    trackerService.startAndWait();
    URL trackerUrl = trackerService.getUrl();
    LOG.info("Started application master tracker server on " + trackerUrl);

    amClient.setTracker(trackerService.getBindAddress(), trackerUrl);
    amClient.startAndWait();

    // Creates ZK path for runnable and kafka logging service
    Futures.allAsList(ImmutableList.of(
      zkClient.create("/" + runId.getId() + "/runnables", null, CreateMode.PERSISTENT),
      zkClient.create("/" + runId.getId() + "/kafka", null, CreateMode.PERSISTENT))
    ).get();

    // Starts kafka server
    LOG.info("Starting kafka server");

    kafkaServer.startAndWait();
    LOG.info("Kafka server started");

    runnableContainerRequests = initContainerRequests();
  }

  private void doStop() throws Exception {
    Thread.interrupted();     // This is just to clear the interrupt flag

    LOG.info("Stop application master with spec: {}", TwillSpecificationAdapter.create().toJson(twillSpec));

    try {
      // call event handler destroy. If there is error, only log and not affected stop sequence.
      eventHandler.destroy();
    } catch (Throwable t) {
      LOG.warn("Exception when calling {}.destroy()", twillSpec.getEventHandler().getClassName(), t);
    }

    instanceChangeExecutor.shutdownNow();

    // For checking if all containers are stopped.
    final Set<String> ids = Sets.newHashSet(runningContainers.getContainerIds());
    YarnAMClient.AllocateHandler handler = new YarnAMClient.AllocateHandler() {
      @Override
      public void acquired(List<ProcessLauncher<YarnContainerInfo>> launchers) {
        // no-op
      }

      @Override
      public void completed(List<YarnContainerStatus> completed) {
        for (YarnContainerStatus status : completed) {
          ids.remove(status.getContainerId());
        }
      }
    };

    runningContainers.stopAll();

    // Poll for 5 seconds to wait for containers to stop.
    int count = 0;
    while (!ids.isEmpty() && count++ < 5) {
      amClient.allocate(0.0f, handler);
      TimeUnit.SECONDS.sleep(1);
    }

    LOG.info("Stopping application master tracker server");
    try {
      trackerService.stopAndWait();
      LOG.info("Stopped application master tracker server");
    } catch (Exception e) {
      LOG.error("Failed to stop tracker service.", e);
    } finally {
      try {
        // App location cleanup
        cleanupDir(URI.create(System.getenv(EnvKeys.TWILL_APP_DIR)));
        Loggings.forceFlush();
        // Sleep a short while to let kafka clients to have chance to fetch the log
        TimeUnit.SECONDS.sleep(1);
      } finally {
        kafkaServer.stopAndWait();
        LOG.info("Kafka server stopped");
      }
    }
  }

  private void cleanupDir(URI appDir) {
    try {
      if (applicationLocation.delete(true)) {
        LOG.info("Application directory deleted: {}", appDir);
      } else {
        LOG.warn("Failed to cleanup directory {}.", appDir);
      }
    } catch (Exception e) {
      LOG.warn("Exception while cleanup directory {}.", appDir, e);
    }
  }


  private void doRun() throws Exception {
    // The main loop
    Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> currentRequest = null;
    final Queue<ProvisionRequest> provisioning = Lists.newLinkedList();

    YarnAMClient.AllocateHandler allocateHandler = new YarnAMClient.AllocateHandler() {
      @Override
      public void acquired(List<ProcessLauncher<YarnContainerInfo>> launchers) {
        launchRunnable(launchers, provisioning);
      }

      @Override
      public void completed(List<YarnContainerStatus> completed) {
        handleCompleted(completed);
      }
    };

    long nextTimeoutCheck = System.currentTimeMillis() + Constants.PROVISION_TIMEOUT;
    while (isRunning()) {
      // Call allocate. It has to be made at first in order to be able to get cluster resource availability.
      amClient.allocate(0.0f, allocateHandler);

      // Looks for containers requests.
      if (provisioning.isEmpty() && runnableContainerRequests.isEmpty() && runningContainers.isEmpty()) {
        LOG.info("All containers completed. Shutting down application master.");
        break;
      }

      // If nothing is in provisioning, and no pending request, move to next one
      while (provisioning.isEmpty() && currentRequest == null && !runnableContainerRequests.isEmpty()) {
        currentRequest = runnableContainerRequests.peek().takeRequest();
        if (currentRequest == null) {
          // All different types of resource request from current order is done, move to next one
          // TODO: Need to handle order type as well
          runnableContainerRequests.poll();
        }
      }
      // Nothing in provision, makes the next batch of provision request
      if (provisioning.isEmpty() && currentRequest != null) {
        addContainerRequests(currentRequest.getKey(), currentRequest.getValue(), provisioning);
        currentRequest = null;
      }

      nextTimeoutCheck = checkProvisionTimeout(nextTimeoutCheck);

      if (isRunning()) {
        TimeUnit.SECONDS.sleep(1);
      }
    }
  }

  /**
   * Handling containers that are completed.
   */
  private void handleCompleted(List<YarnContainerStatus> completedContainersStatuses) {
    Multiset<String> restartRunnables = HashMultiset.create();
    for (YarnContainerStatus status : completedContainersStatuses) {
      LOG.info("Container {} completed with {}:{}.",
               status.getContainerId(), status.getState(), status.getDiagnostics());
      runningContainers.handleCompleted(status, restartRunnables);
    }

    for (Multiset.Entry<String> entry : restartRunnables.entrySet()) {
      LOG.info("Re-request container for {} with {} instances.", entry.getElement(), entry.getCount());
      for (int i = 0; i < entry.getCount(); i++) {
        runnableContainerRequests.add(createRunnableContainerRequest(entry.getElement()));
      }
    }

    // For all runnables that needs to re-request for containers, update the expected count timestamp
    // so that the EventHandler would triggered with the right expiration timestamp.
    expectedContainers.updateRequestTime(restartRunnables.elementSet());
  }

  /**
   * Check for containers provision timeout and invoke eventHandler if necessary.
   *
   * @return the timestamp for the next time this method needs to be called.
   */
  private long checkProvisionTimeout(long nextTimeoutCheck) {
    if (System.currentTimeMillis() < nextTimeoutCheck) {
      return nextTimeoutCheck;
    }

    // Invoke event handler for provision request timeout
    Map<String, ExpectedContainers.ExpectedCount> expiredRequests = expectedContainers.getAll();
    Map<String, Integer> runningCounts = runningContainers.countAll();

    List<EventHandler.TimeoutEvent> timeoutEvents = Lists.newArrayList();
    for (Map.Entry<String, ExpectedContainers.ExpectedCount> entry : expiredRequests.entrySet()) {
      String runnableName = entry.getKey();
      ExpectedContainers.ExpectedCount expectedCount = entry.getValue();
      int runningCount = runningCounts.containsKey(runnableName) ? runningCounts.get(runnableName) : 0;
      if (expectedCount.getCount() != runningCount) {
        timeoutEvents.add(new EventHandler.TimeoutEvent(runnableName, expectedCount.getCount(),
                                                                   runningCount, expectedCount.getTimestamp()));
      }
    }

    if (!timeoutEvents.isEmpty()) {
      try {
        EventHandler.TimeoutAction action = eventHandler.launchTimeout(timeoutEvents);
        if (action.getTimeout() < 0) {
          // Abort application
          stop();
        } else {
          return nextTimeoutCheck + action.getTimeout();
        }
      } catch (Throwable t) {
        LOG.warn("Exception when calling EventHandler {}. Ignore the result.", t);
      }
    }
    return nextTimeoutCheck + Constants.PROVISION_TIMEOUT;
  }

  private Credentials createCredentials() {
    Credentials credentials = new Credentials();
    if (!UserGroupInformation.isSecurityEnabled()) {
      return credentials;
    }

    try {
      credentials.addAll(UserGroupInformation.getCurrentUser().getCredentials());

      // Remove the AM->RM tokens
      Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        Token<?> token = iter.next();
        if (token.getKind().equals(AMRM_TOKEN_KIND_NAME)) {
          iter.remove();
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to get current user. No credentials will be provided to containers.", e);
    }

    return credentials;
  }

  private Queue<RunnableContainerRequest> initContainerRequests() {
    // Orderly stores container requests.
    Queue<RunnableContainerRequest> requests = Lists.newLinkedList();
    // For each order in the twillSpec, create container request for each runnable.
    for (TwillSpecification.Order order : twillSpec.getOrders()) {
      // Group container requests based on resource requirement.
      ImmutableMultimap.Builder<Resource, RuntimeSpecification> builder = ImmutableMultimap.builder();
      for (String runnableName : order.getNames()) {
        RuntimeSpecification runtimeSpec = twillSpec.getRunnables().get(runnableName);
        Resource capability = createCapability(runtimeSpec.getResourceSpecification());
        builder.put(capability, runtimeSpec);
      }
      requests.add(new RunnableContainerRequest(order.getType(), builder.build()));
    }
    return requests;
  }

  /**
   * Adds container requests with the given resource capability for each runtime.
   */
  private void addContainerRequests(Resource capability,
                                    Collection<RuntimeSpecification> runtimeSpecs,
                                    Queue<ProvisionRequest> provisioning) {
    for (RuntimeSpecification runtimeSpec : runtimeSpecs) {
      String name = runtimeSpec.getName();
      int newContainers = expectedContainers.getExpected(name) - runningContainers.count(name);
      if (newContainers > 0) {
        // TODO: Allow user to set priority?
        LOG.info("Request {} container with capability {}", newContainers, capability);
        String requestId = amClient.addContainerRequest(capability, newContainers).setPriority(0).apply();
        provisioning.add(new ProvisionRequest(runtimeSpec, requestId, newContainers));
      }
    }
  }

  /**
   * Launches runnables in the provisioned containers.
   */
  private void launchRunnable(List<ProcessLauncher<YarnContainerInfo>> launchers,
                              Queue<ProvisionRequest> provisioning) {
    for (ProcessLauncher<YarnContainerInfo> processLauncher : launchers) {
      LOG.info("Got container {}", processLauncher.getContainerInfo().getId());
      ProvisionRequest provisionRequest = provisioning.peek();
      if (provisionRequest == null) {
        continue;
      }

      String runnableName = provisionRequest.getRuntimeSpec().getName();
      LOG.info("Starting runnable {} with {}", runnableName, processLauncher);

      int containerCount = expectedContainers.getExpected(runnableName);

      ProcessLauncher.PrepareLaunchContext launchContext = processLauncher.prepareLaunch(
        ImmutableMap.<String, String>builder()
          .put(EnvKeys.TWILL_APP_DIR, System.getenv(EnvKeys.TWILL_APP_DIR))
          .put(EnvKeys.TWILL_FS_USER, System.getenv(EnvKeys.TWILL_FS_USER))
          .put(EnvKeys.TWILL_APP_RUN_ID, runId.getId())
          .put(EnvKeys.TWILL_APP_NAME, twillSpec.getName())
          .put(EnvKeys.TWILL_ZK_CONNECT, zkClient.getConnectString())
          .put(EnvKeys.TWILL_LOG_KAFKA_ZK, getKafkaZKConnect())
          .build()
        , getLocalizeFiles(), credentials
      );

      TwillContainerLauncher launcher = new TwillContainerLauncher(
        twillSpec.getRunnables().get(runnableName), launchContext,
        ZKClients.namespace(zkClient, getZKNamespace(runnableName)),
        containerCount, jvmOpts, reservedMemory, getSecureStoreLocation());

      runningContainers.start(runnableName, processLauncher.getContainerInfo(), launcher);

      // Need to call complete to workaround bug in YARN AMRMClient
      if (provisionRequest.containerAcquired()) {
        amClient.completeContainerRequest(provisionRequest.getRequestId());
      }

      if (expectedContainers.getExpected(runnableName) == runningContainers.count(runnableName)) {
        LOG.info("Runnable " + runnableName + " fully provisioned with " + containerCount + " instances.");
        provisioning.poll();
      }
    }
  }

  private List<LocalFile> getLocalizeFiles() {
    try {
      Reader reader = Files.newReader(new File(Constants.Files.LOCALIZE_FILES), Charsets.UTF_8);
      try {
        return new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
                                .create().fromJson(reader, new TypeToken<List<LocalFile>>() { }.getType());
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String getZKNamespace(String runnableName) {
    return String.format("/%s/runnables/%s", runId.getId(), runnableName);
  }

  private String getKafkaZKConnect() {
    return String.format("%s/%s/kafka", zkClient.getConnectString(), runId.getId());
  }

  private Properties generateKafkaConfig() {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("log.dir", new File("kafka-logs").getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zookeeper.connect", getKafkaZKConnect());
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    return prop;
  }

  private ListenableFuture<String> processMessage(final String messageId, Message message) {
    LOG.debug("Message received: {} {}.", messageId, message);

    SettableFuture<String> result = SettableFuture.create();
    Runnable completion = getMessageCompletion(messageId, result);

    if (handleSecureStoreUpdate(message)) {
      runningContainers.sendToAll(message, completion);
      return result;
    }

    if (handleSetInstances(message, completion)) {
      return result;
    }

    // Replicate messages to all runnables
    if (message.getScope() == Message.Scope.ALL_RUNNABLE) {
      runningContainers.sendToAll(message, completion);
      return result;
    }

    // Replicate message to a particular runnable.
    if (message.getScope() == Message.Scope.RUNNABLE) {
      runningContainers.sendToRunnable(message.getRunnableName(), message, completion);
      return result;
    }

    LOG.info("Message ignored. {}", message);
    return Futures.immediateFuture(messageId);
  }

  /**
   * Attempts to change the number of running instances.
   * @return {@code true} if the message does requests for changes in number of running instances of a runnable,
   *         {@code false} otherwise.
   */
  private boolean handleSetInstances(final Message message, final Runnable completion) {
    if (message.getType() != Message.Type.SYSTEM || message.getScope() != Message.Scope.RUNNABLE) {
      return false;
    }

    Command command = message.getCommand();
    Map<String, String> options = command.getOptions();
    if (!"instances".equals(command.getCommand()) || !options.containsKey("count")) {
      return false;
    }

    final String runnableName = message.getRunnableName();
    if (runnableName == null || runnableName.isEmpty() || !twillSpec.getRunnables().containsKey(runnableName)) {
      LOG.info("Unknown runnable {}", runnableName);
      return false;
    }

    final int newCount = Integer.parseInt(options.get("count"));
    final int oldCount = expectedContainers.getExpected(runnableName);

    LOG.info("Received change instances request for {}, from {} to {}.", runnableName, oldCount, newCount);

    if (newCount == oldCount) {   // Nothing to do, simply complete the request.
      completion.run();
      return true;
    }

    instanceChangeExecutor.execute(createSetInstanceRunnable(message, completion, oldCount, newCount));
    return true;
  }

  /**
   * Creates a Runnable for execution of change instance request.
   */
  private Runnable createSetInstanceRunnable(final Message message, final Runnable completion,
                                             final int oldCount, final int newCount) {
    return new Runnable() {
      @Override
      public void run() {
        final String runnableName = message.getRunnableName();

        LOG.info("Processing change instance request for {}, from {} to {}.", runnableName, oldCount, newCount);
        try {
          // Wait until running container count is the same as old count
          runningContainers.waitForCount(runnableName, oldCount);
          LOG.info("Confirmed {} containers running for {}.", oldCount, runnableName);

          expectedContainers.setExpected(runnableName, newCount);

          try {
            if (newCount < oldCount) {
              // Shutdown some running containers
              for (int i = 0; i < oldCount - newCount; i++) {
                runningContainers.removeLast(runnableName);
              }
            } else {
              // Increase the number of instances
              runnableContainerRequests.add(createRunnableContainerRequest(runnableName));
            }
          } finally {
            runningContainers.sendToRunnable(runnableName, message, completion);
            LOG.info("Change instances request completed. From {} to {}.", oldCount, newCount);
          }
        } catch (InterruptedException e) {
          // If the wait is being interrupted, discard the message.
          completion.run();
        }
      }
    };
  }

  private RunnableContainerRequest createRunnableContainerRequest(final String runnableName) {
    // Find the current order of the given runnable in order to create a RunnableContainerRequest.
    TwillSpecification.Order order = Iterables.find(twillSpec.getOrders(), new Predicate<TwillSpecification.Order>() {
      @Override
      public boolean apply(TwillSpecification.Order input) {
        return (input.getNames().contains(runnableName));
      }
    });

    RuntimeSpecification runtimeSpec = twillSpec.getRunnables().get(runnableName);
    Resource capability = createCapability(runtimeSpec.getResourceSpecification());
    return new RunnableContainerRequest(order.getType(), ImmutableMultimap.of(capability, runtimeSpec));
  }

  private Runnable getMessageCompletion(final String messageId, final SettableFuture<String> future) {
    return new Runnable() {
      @Override
      public void run() {
        future.set(messageId);
      }
    };
  }

  private Resource createCapability(ResourceSpecification resourceSpec) {
    Resource capability = Records.newRecord(Resource.class);

    if (!YarnUtils.setVirtualCores(capability, resourceSpec.getVirtualCores())) {
      LOG.debug("Virtual cores limit not supported.");
    }

    capability.setMemory(resourceSpec.getMemorySize());
    return capability;
  }

  @Override
  protected Service getServiceDelegate() {
    return serviceDelegate;
  }

  /**
   * A private class for service lifecycle. It's done this way so that we can have {@link ZKServiceDecorator} to
   * wrap around this to reflect status in ZK.
   */
  private final class ServiceDelegate extends AbstractExecutionThreadService implements MessageCallback {

    private volatile Thread runThread;

    @Override
    protected void run() throws Exception {
      runThread = Thread.currentThread();
      try {
        doRun();
      } catch (InterruptedException e) {
        // It's ok to get interrupted exception, as it's a signal to stop
        Thread.currentThread().interrupt();
      }
    }

    @Override
    protected void startUp() throws Exception {
      doStart();
    }

    @Override
    protected void shutDown() throws Exception {
      doStop();
    }

    @Override
    protected void triggerShutdown() {
      Thread runThread = this.runThread;
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    public ListenableFuture<String> onReceived(String messageId, Message message) {
      return processMessage(messageId, message);
    }
  }
}
