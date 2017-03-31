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
package org.apache.twill.yarn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.Configs;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.api.security.SecureStoreWriter;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.appmaster.ApplicationMasterLiveNodeData;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.io.NoCachingLocationCache;
import org.apache.twill.internal.yarn.VersionDetectYarnAppClientFactory;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnApplicationReport;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of {@link org.apache.twill.api.TwillRunnerService} that runs application on a YARN cluster.
 */
public final class YarnTwillRunnerService implements TwillRunnerService {

  private static final Logger LOG = LoggerFactory.getLogger(YarnTwillRunnerService.class);
  private static final int ZK_TIMEOUT = 10000;
  private static final Function<String, RunId> STRING_TO_RUN_ID = new Function<String, RunId>() {
    @Override
    public RunId apply(String input) {
      return RunIds.fromString(input);
    }
  };
  private static final Function<YarnTwillController, TwillController> CAST_CONTROLLER =
    new Function<YarnTwillController, TwillController>() {
    @Override
    public TwillController apply(YarnTwillController controller) {
      return controller;
    }
  };

  private final YarnConfiguration yarnConfig;
  private final ZKClientService zkClientService;
  private final LocationFactory locationFactory;
  private final Table<String, RunId, YarnTwillController> controllers;
  // A Guava service to help the state transition.
  private final Service serviceDelegate;
  private LocationCache locationCache;
  private LocationCacheCleaner locationCacheCleaner;
  private ScheduledExecutorService secureStoreScheduler;

  private Iterable<LiveInfo> liveInfos;
  private Cancellable watchCancellable;

  private volatile String jvmOptions = null;

  /**
   * Creates an instance with a {@link FileContextLocationFactory} created base on the given configuration with the
   * user home directory as the location factory namespace.
   *
   * @param config Configuration of the yarn cluster
   * @param zkConnect ZooKeeper connection string
   */
  public YarnTwillRunnerService(YarnConfiguration config, String zkConnect) {
    this(config, zkConnect, createDefaultLocationFactory(config));
  }

  /**
   * Creates an instance.
   *
   * @param config Configuration of the yarn cluster
   * @param zkConnect ZooKeeper connection string
   * @param locationFactory Factory to create {@link Location} instances that are readable and writable by this service
   */
  public YarnTwillRunnerService(YarnConfiguration config, String zkConnect, LocationFactory locationFactory) {
    this.yarnConfig = config;
    this.locationFactory = locationFactory;
    this.zkClientService = getZKClientService(zkConnect);
    this.controllers = HashBasedTable.create();
    this.serviceDelegate = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        YarnTwillRunnerService.this.startUp();
      }

      @Override
      protected void shutDown() throws Exception {
        YarnTwillRunnerService.this.shutDown();
      }
    };
  }

  @Override
  public void start() {
    serviceDelegate.startAndWait();
  }

  @Override
  public void stop() {
    serviceDelegate.stopAndWait();
  }

  /**
   * This methods sets the extra JVM options that will be passed to the java command line for every application
   * started through this {@link YarnTwillRunnerService} instance. It only affects applications that are started
   * after options is set.
   *
   * This is intended for advance usage. All options will be passed unchanged to the java command line. Invalid
   * options could cause application not able to start.
   *
   * @param options extra JVM options.
   */
  public void setJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.jvmOptions = options;
  }

  /**
   * Returns any extra JVM options that have been set.
   * @see #setJVMOptions(String)
   */
  public String getJVMOptions() {
    return jvmOptions;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(final SecureStoreUpdater updater,
                                               long initialDelay, long delay, TimeUnit unit) {
    synchronized (this) {
      if (secureStoreScheduler == null) {
        secureStoreScheduler = Executors.newSingleThreadScheduledExecutor(
          Threads.createDaemonThreadFactory("secure-store-renewer"));
      }
    }

    final ScheduledFuture<?> future = secureStoreScheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        // Collects all live applications
        Table<String, RunId, YarnTwillController> liveApps;
        synchronized (this) {
          liveApps = HashBasedTable.create(controllers);
        }

        // Update the secure store with merging = true
        renewSecureStore(liveApps, new SecureStoreRenewer() {
          @Override
          public void renew(String application, RunId runId, SecureStoreWriter secureStoreWriter) throws IOException {
            secureStoreWriter.write(updater.update(application, runId));
          }
        }, true);
      }
    }, initialDelay, delay, unit);

    return new Cancellable() {
      @Override
      public void cancel() {
        future.cancel(false);
      }
    };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
                                           long delay, long retryDelay, TimeUnit unit) {
    synchronized (this) {
      if (secureStoreScheduler != null) {
        // Shutdown and block until the schedule is stopped
        stopScheduler(secureStoreScheduler);
      }
      secureStoreScheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("secure-store-renewer"));
    }

    final ScheduledExecutorService currentScheduler = secureStoreScheduler;
    secureStoreScheduler.scheduleWithFixedDelay(
      createSecureStoreUpdateRunnable(currentScheduler, renewer,
                                      ImmutableMultimap.<String, RunId>of(), retryDelay, unit),
      initialDelay, delay, unit);
    return new Cancellable() {
      @Override
      public void cancel() {
        synchronized (YarnTwillRunnerService.this) {
          // Only cancel if the active scheduler is the same as the schedule bind to this cancellable
          if (currentScheduler == secureStoreScheduler) {
            secureStoreScheduler.shutdown();
            secureStoreScheduler = null;
          }
        }
      }
    };
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    Preconditions.checkState(serviceDelegate.isRunning(), "Service not start. Please call start() first.");
    final TwillSpecification twillSpec = application.configure();
    final String appName = twillSpec.getName();
    RunId runId = RunIds.generate();
    Location appLocation = locationFactory.create(String.format("/%s/%s", twillSpec.getName(), runId.getId()));
    LocationCache locationCache = this.locationCache;
    if (locationCache == null) {
      locationCache = new NoCachingLocationCache(appLocation);
    }

    Configuration config = new Configuration(yarnConfig);
    return new YarnTwillPreparer(config, twillSpec, runId, zkClientService.getConnectString(),
                                 appLocation, jvmOptions, locationCache, new YarnTwillControllerFactory() {
      @Override
      public YarnTwillController create(RunId runId, boolean logCollectionEnabled, Iterable<LogHandler> logHandlers,
                                        Callable<ProcessController<YarnApplicationReport>> startUp,
                                        long startTimeout, TimeUnit startTimeoutUnit) {
        ZKClient zkClient = ZKClients.namespace(zkClientService, "/" + appName);
        YarnTwillController controller = listenController(new YarnTwillController(appName, runId, zkClient,
                                                                                  logCollectionEnabled,
                                                                                  logHandlers, startUp,
                                                                                  startTimeout, startTimeoutUnit));
        synchronized (YarnTwillRunnerService.this) {
          Preconditions.checkArgument(!controllers.contains(appName, runId),
                                      "Application %s with runId %s is already running.", appName, runId);
          controllers.put(appName, runId, controller);
        }
        return controller;
      }
    });
  }

  @Override
  public synchronized TwillController lookup(String applicationName, final RunId runId) {
    return controllers.get(applicationName, runId);
  }

  @Override
  public Iterable<TwillController> lookup(final String applicationName) {
    return new Iterable<TwillController>() {
      @Override
      public Iterator<TwillController> iterator() {
        synchronized (YarnTwillRunnerService.this) {
          return Iterators.transform(ImmutableList.copyOf(controllers.row(applicationName).values()).iterator(),
                                     CAST_CONTROLLER);
        }
      }
    };
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return liveInfos;
  }

  private void startUp() throws Exception {
    zkClientService.startAndWait();

    // Create the root node, so that the namespace root would get created if it is missing
    // If the exception is caused by node exists, then it's ok. Otherwise propagate the exception.
    ZKOperations.ignoreError(zkClientService.create("/", null, CreateMode.PERSISTENT),
                             KeeperException.NodeExistsException.class, null).get();

    watchCancellable = watchLiveApps();
    liveInfos = createLiveInfos();

    boolean enableSecureStoreUpdate = yarnConfig.getBoolean(Configs.Keys.SECURE_STORE_UPDATE_LOCATION_ENABLED, true);
    // Schedule an updater for updating HDFS delegation tokens
    if (UserGroupInformation.isSecurityEnabled() && enableSecureStoreUpdate) {
      long renewalInterval = yarnConfig.getLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                                                DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      // Schedule it five minutes before it expires.
      long delay = renewalInterval - TimeUnit.MINUTES.toMillis(5);
      // Just to safeguard. In practice, the value shouldn't be that small, otherwise nothing could work.
      if (delay <= 0) {
        delay = (renewalInterval <= 2) ? 1 : renewalInterval / 2;
      }

      setSecureStoreRenewer(new LocationSecureStoreRenewer(yarnConfig, locationFactory),
                            delay, delay, 10000L, TimeUnit.MILLISECONDS);
    }

    // Optionally create a LocationCache
    String cacheDir = yarnConfig.get(Configs.Keys.LOCATION_CACHE_DIR);
    if (cacheDir != null) {
      String sessionId = Long.toString(System.currentTimeMillis());
      try {
        Location cacheBase = locationFactory.create(cacheDir);
        cacheBase.mkdirs();
        cacheBase.setPermissions("775");

        // Use a unique cache directory for each instance of this class
        Location cacheLocation = cacheBase.append(sessionId);
        cacheLocation.mkdirs();
        cacheLocation.setPermissions("775");

        locationCache = new BasicLocationCache(cacheLocation);
        locationCacheCleaner = startLocationCacheCleaner(cacheBase, sessionId);
      } catch (IOException e) {
        LOG.warn("Failed to create location cache directory. Location cache cannot be enabled.", e);
      }
    }
  }

  /**
   * Forces a cleanup of location cache based on the given time.
   */
  @VisibleForTesting
  void forceLocationCacheCleanup(long currentTime) {
    locationCacheCleaner.forceCleanup(currentTime);
  }

  private LocationCacheCleaner startLocationCacheCleaner(final Location cacheBase, final String sessionId) {
    LocationCacheCleaner cleaner = new LocationCacheCleaner(
      yarnConfig, cacheBase, sessionId, new Predicate<Location>() {
        @Override
        public boolean apply(Location location) {
          // Collects all the locations that is being used by any live applications
          Set<Location> activeLocations = new HashSet<>();
          synchronized (YarnTwillRunnerService.this) {
            for (YarnTwillController controller : controllers.values()) {
              ApplicationMasterLiveNodeData amLiveNodeData = controller.getApplicationMasterLiveNodeData();
              if (amLiveNodeData != null) {
                for (LocalFile localFile : amLiveNodeData.getLocalFiles()) {
                  activeLocations.add(locationFactory.create(localFile.getURI()));
                }
              }
            }
          }

          try {
            // Always keep the launcher.jar and twill.jar from the current session as they should never change,
            // hence never expires
            activeLocations.add(cacheBase.append(sessionId).append(Constants.Files.LAUNCHER_JAR));
            activeLocations.add(cacheBase.append(sessionId).append(Constants.Files.TWILL_JAR));
          } catch (IOException e) {
            // This should not happen
            LOG.warn("Failed to construct cache location", e);
          }

          return !activeLocations.contains(location);
        }
      });
    cleaner.startAndWait();
    return cleaner;
  }

  private void shutDown() throws Exception {
    // Shutdown shouldn't stop any controllers, as stopping this client service should let the remote containers
    // running. However, this assumes that this TwillRunnerService is a long running service and you only stop it
    // when the JVM process is about to exit. Hence it is important that threads created in the controllers are
    // daemon threads.
    synchronized (this) {
      if (locationCacheCleaner != null) {
        locationCacheCleaner.stopAndWait();
      }
      if (secureStoreScheduler != null) {
        secureStoreScheduler.shutdownNow();
      }
    }
    watchCancellable.cancel();
    zkClientService.stopAndWait();
  }

  private Cancellable watchLiveApps() {
    final Map<String, Cancellable> watched = Maps.newConcurrentMap();

    final AtomicBoolean cancelled = new AtomicBoolean(false);
    // Watch child changes in the root, which gives all application names.
    final Cancellable cancellable = ZKOperations.watchChildren(zkClientService, "/",
                                                               new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        if (cancelled.get()) {
          return;
        }

        Set<String> apps = ImmutableSet.copyOf(nodeChildren.getChildren());

        // For each for the application name, watch for ephemeral nodes under /instances.
        for (final String appName : apps) {
          if (watched.containsKey(appName)) {
            continue;
          }

          final String instancePath = String.format("/%s/instances", appName);
          watched.put(appName,
                      ZKOperations.watchChildren(zkClientService, instancePath, new ZKOperations.ChildrenCallback() {
            @Override
            public void updated(NodeChildren nodeChildren) {
              if (cancelled.get()) {
                return;
              }
              if (nodeChildren.getChildren().isEmpty()) {     // No more child, means no live instances
                Cancellable removed = watched.remove(appName);
                if (removed != null) {
                  removed.cancel();
                }
                return;
              }
              synchronized (YarnTwillRunnerService.this) {
                // For each of the children, which the node name is the runId,
                // fetch the application Id and construct TwillController.
                for (final RunId runId : Iterables.transform(nodeChildren.getChildren(), STRING_TO_RUN_ID)) {
                  if (controllers.contains(appName, runId)) {
                    continue;
                  }
                  updateController(appName, runId, cancelled);
                }
              }
            }
          }));
        }

        // Remove app watches for apps that are gone. Removal of controller from controllers table is done
        // in the state listener attached to the twill controller.
        for (String removeApp : Sets.difference(watched.keySet(), apps)) {
          watched.remove(removeApp).cancel();
        }
      }
    });
    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
        cancellable.cancel();
        for (Cancellable c : watched.values()) {
          c.cancel();
        }
      }
    };
  }

  private YarnTwillController listenController(final YarnTwillController controller) {
    controller.onTerminated(new Runnable() {
      @Override
      public void run() {
        synchronized (YarnTwillRunnerService.this) {
          Iterables.removeIf(controllers.values(), new Predicate<YarnTwillController>() {
            @Override
            public boolean apply(YarnTwillController input) {
              return input == controller;
            }
          });
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return controller;
  }

  private ZKClientService getZKClientService(String zkConnect) {
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                   .setSessionTimeout(ZK_TIMEOUT)
                                   .build(), RetryStrategies.exponentialDelay(100, 2000, TimeUnit.MILLISECONDS))));
  }

  private Iterable<LiveInfo> createLiveInfos() {
    return new Iterable<LiveInfo>() {

      @Override
      public Iterator<LiveInfo> iterator() {
        Map<String, Map<RunId, YarnTwillController>> controllerMap;
        synchronized (YarnTwillRunnerService.this) {
          controllerMap = ImmutableTable.copyOf(controllers).rowMap();
        }
        return Iterators.transform(controllerMap.entrySet().iterator(),
                                   new Function<Map.Entry<String, Map<RunId, YarnTwillController>>, LiveInfo>() {
          @Override
          public LiveInfo apply(final Map.Entry<String, Map<RunId, YarnTwillController>> entry) {
            return new LiveInfo() {
              @Override
              public String getApplicationName() {
                return entry.getKey();
              }

              @Override
              public Iterable<TwillController> getControllers() {
                return Iterables.transform(entry.getValue().values(), CAST_CONTROLLER);
              }
            };
          }
        });
      }
    };
  }

  private void updateController(final String appName, final RunId runId, final AtomicBoolean cancelled) {
    String instancePath = String.format("/%s/instances/%s", appName, runId.getId());

    // Fetch the content node.
    Futures.addCallback(zkClientService.getData(instancePath), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        if (cancelled.get()) {
          return;
        }

        ApplicationMasterLiveNodeData amLiveNodeData = ApplicationMasterLiveNodeDecoder.decode(result);
        if (amLiveNodeData == null) {
          return;
        }

        synchronized (YarnTwillRunnerService.this) {
          if (!controllers.contains(appName, runId)) {
            ZKClient zkClient = ZKClients.namespace(zkClientService, "/" + appName);
            YarnAppClient yarnAppClient = new VersionDetectYarnAppClientFactory().create(new Configuration(yarnConfig));

            YarnTwillController controller = listenController(
              new YarnTwillController(appName, runId, zkClient, amLiveNodeData, yarnAppClient));
            controllers.put(appName, runId, controller);
            controller.start();
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.warn("Failed in fetching application instance node.", t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Stops the given scheduler and block until is it stopped.
   */
  private void stopScheduler(final ScheduledExecutorService scheduler) {
    scheduler.shutdown();
    boolean interrupted = false;
    try {
      while (true) {
        try {
          scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Creates a {@link Runnable} for renewing {@link SecureStore} for running applications.
   *
   * @param scheduler the schedule to schedule next renewal execution
   * @param renewer the {@link SecureStoreRenewer} to use for renewal
   * @param retryRuns if non-empty, only the given set of application name and run id that need to have
   *                  secure store renewed; if empty, renew all running applications
   * @param retryDelay the delay before retrying applications that are failed to have secure store renewed
   * @param timeUnit the unit for the {@code delay} and {@code failureDelay}.
   * @return a {@link Runnable}
   */
  private Runnable createSecureStoreUpdateRunnable(final ScheduledExecutorService scheduler,
                                                   final SecureStoreRenewer renewer,
                                                   final Multimap<String, RunId> retryRuns,
                                                   final long retryDelay, final TimeUnit timeUnit) {
    return new Runnable() {
      @Override
      public void run() {
        // Collects the set of running application runs
        Table<String, RunId, YarnTwillController> liveApps;

        synchronized (YarnTwillRunnerService.this) {
          if (retryRuns.isEmpty()) {
            liveApps = HashBasedTable.create(controllers);
          } else {
            // If this is a renew retry, only renew the one in the retryRuns set
            liveApps = HashBasedTable.create();
            for (Table.Cell<String, RunId, YarnTwillController> cell : controllers.cellSet()) {
              if (retryRuns.containsEntry(cell.getRowKey(), cell.getColumnKey())) {
                liveApps.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
              }
            }
          }
        }

        Multimap<String, RunId> failureRenews = renewSecureStore(liveApps, renewer, false);

        if (!failureRenews.isEmpty()) {
          // If there are failure during the renewal, schedule a retry with a new Runnable.
          LOG.info("Schedule to retry on secure store renewal for applications {} in {} {}",
                   failureRenews.keySet(), retryDelay, timeUnit.name().toLowerCase());
          try {
            scheduler.schedule(
              createSecureStoreUpdateRunnable(scheduler, renewer, failureRenews, retryDelay, timeUnit),
              retryDelay, timeUnit);
          } catch (RejectedExecutionException e) {
            // If the renewal is stopped, the scheduler will be stopped,
            // hence this exception will be thrown and can be safely ignore.
          }
        }
      }
    };
  }

  /**
   * Renews the {@link SecureStore} for all the running applications.
   *
   * @param liveApps set of running applications that need to have secure store renewal
   * @param renewer the {@link SecureStoreRenewer} for renewal
   * @param mergeCredentials {@code true} to merge with existing credentials
   * @return a {@link Multimap} containing the application runs that were failed to have secure store renewed
   */
  private Multimap<String, RunId> renewSecureStore(Table<String, RunId, YarnTwillController> liveApps,
                                                   SecureStoreRenewer renewer, boolean mergeCredentials) {
    Multimap<String, RunId> failureRenews = HashMultimap.create();

    // Renew the secure store for each running application
    for (Table.Cell<String, RunId, YarnTwillController> liveApp : liveApps.cellSet()) {
      String application = liveApp.getRowKey();
      RunId runId = liveApp.getColumnKey();
      YarnTwillController controller = liveApp.getValue();

      try {
        renewer.renew(application, runId, new YarnSecureStoreWriter(application, runId, controller, mergeCredentials));
      } catch (Exception e) {
        LOG.warn("Failed to renew secure store for {}:{}", application, runId, e);
        failureRenews.put(application, runId);
      }
    }

    return failureRenews;
  }

  private static LocationFactory createDefaultLocationFactory(Configuration configuration) {
    try {
      FileContext fc = FileContext.getFileContext(configuration);
      String basePath = fc.getHomeDirectory().toUri().getPath();
      return new FileContextLocationFactory(configuration, basePath);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * A {@link SecureStoreWriter} for updating secure store for YARN application via a shared location with the
   * running application.
   */
  private final class YarnSecureStoreWriter implements SecureStoreWriter {

    private final String application;
    private final RunId runId;
    private final YarnTwillController controller;
    private final boolean mergeCredentials;

    private YarnSecureStoreWriter(String application, RunId runId,
                                  YarnTwillController controller, boolean mergeCredentials) {
      this.application = application;
      this.runId = runId;
      this.controller = controller;
      this.mergeCredentials = mergeCredentials;
    }

    @Override
    public void write(SecureStore secureStore) throws IOException {
      Object store = secureStore.getStore();
      if (!(store instanceof Credentials)) {
        LOG.warn("Only Hadoop Credentials is supported. Ignore update for {}:{} with secure store {}",
                 application, runId, secureStore);
        return;
      }

      Location credentialsLocation = locationFactory.create(String.format("/%s/%s/%s", application, runId.getId(),
                                                                          Constants.Files.CREDENTIALS));

      LOG.debug("Writing new secure store for {}:{} to {}", application, runId, credentialsLocation);

      Credentials credentials = new Credentials();
      if (mergeCredentials) {
        // Try to read the old credentials.
        try (DataInputStream is = new DataInputStream(new BufferedInputStream(credentialsLocation.getInputStream()))) {
          credentials.readTokenStorageStream(is);
        } catch (FileNotFoundException e) {
          // This is safe to ignore as the file may not be there
        } catch (Exception e) {
          // Just log and proceed.
          LOG.warn("Failed to read existing credentials from {} for merging due to {}.",
                   credentialsLocation, e.toString());
        }
      }

      // Overwrite with credentials from the secure store
      credentials.addAll((Credentials) store);
      Location tmpLocation = credentialsLocation.getTempFile(Constants.Files.CREDENTIALS);

      // Save the credentials store with user-only permission.
      try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(tmpLocation.getOutputStream("600")))) {
        credentials.writeTokenStorageToStream(os);
      }

      // Rename the tmp file into the credentials location
      tmpLocation.renameTo(credentialsLocation);

      // Notify the application that the credentials has been updated
      controller.secureStoreUpdated();

      LOG.debug("Secure store for {} {} saved to {}.", application, runId, credentialsLocation);
    }
  }
}
