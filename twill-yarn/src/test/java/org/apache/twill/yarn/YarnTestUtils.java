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

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.internal.yarn.VersionDetectYarnAppClientFactory;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utilities for testing YARN.
 */
public final class YarnTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(YarnTestUtils.class);

  private static InMemoryZKServer zkServer;
  private static MiniDFSCluster dfsCluster;
  private static MiniYARNCluster cluster;
  private static TwillRunnerService runnerService;
  private static YarnConfiguration config;
  private static YarnAppClient yarnAppClient;

  private static final AtomicBoolean once = new AtomicBoolean(false);

  public static final boolean initOnce() throws IOException {
    if (once.compareAndSet(false, true)) {
      final TemporaryFolder tmpFolder = new TemporaryFolder();
      tmpFolder.create();
      init(tmpFolder.newFolder());

      // add shutdown hook because we want to initialized/cleanup once
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            finish();
          } finally {
            tmpFolder.delete();
          }
        }
      }));
      return true;
    }
    return false;
  }

  private static final void init(File folder) throws IOException {
    // Starts Zookeeper
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    // Start YARN mini cluster
    LOG.info("Starting Mini DFS on path {}", folder);
    Configuration fsConf = new HdfsConfiguration(new Configuration());
    fsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, folder.getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(fsConf).numDataNodes(1).build();

    Configuration conf = new YarnConfiguration(dfsCluster.getFileSystem().getConf());

    if (YarnUtils.getHadoopVersion().equals(YarnUtils.HadoopVersions.HADOOP_20)) {
      conf.set("yarn.resourcemanager.scheduler.class",
                 "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler");
    } else {
      conf.set("yarn.resourcemanager.scheduler.class",
                 "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler");
      conf.set("yarn.scheduler.capacity.resource-calculator",
                 "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator");
      conf.setBoolean("yarn.scheduler.include-port-in-node-name", true);
    }
    conf.set("yarn.nodemanager.vmem-pmem-ratio", "20.1");
    conf.set("yarn.nodemanager.vmem-check-enabled", "false");
    conf.set("yarn.scheduler.minimum-allocation-mb", "128");
    conf.set("yarn.nodemanager.delete.debug-delay-sec", "3600");

    cluster = new MiniYARNCluster("test-cluster", 3, 1, 1);
    cluster.init(conf);
    cluster.start();

    config = new YarnConfiguration(cluster.getConfig());

    runnerService = createTwillRunnerService();
    runnerService.startAndWait();

    yarnAppClient = new VersionDetectYarnAppClientFactory().create(conf);
    yarnAppClient.start();
  }

  public static final boolean finish() {
    if (once.compareAndSet(true, false)) {
      runnerService.stopAndWait();
      cluster.stop();
      dfsCluster.shutdown();
      zkServer.stopAndWait();
      yarnAppClient.stop();

      return true;
    }

    return false;
  }

  public static final TwillRunner getTwillRunner() {
    return runnerService;
  }

  public static final InMemoryZKServer getZkServer() {
    return zkServer;
  }

  /**
   * Creates an unstarted instance of {@link org.apache.twill.api.TwillRunnerService}.
   */
  public static final TwillRunnerService createTwillRunnerService() throws IOException {
    YarnTwillRunnerService runner = new YarnTwillRunnerService(config, zkServer.getConnectionStr() + "/twill");
    // disable tests stealing focus
    runner.setJVMOptions("-Djava.awt.headless=true");
    return runner;
  }

  /**
   * Returns {@link org.apache.hadoop.yarn.api.records.NodeReport} for the nodes in the MiniYarnCluster.
   * @return a list of {@link org.apache.hadoop.yarn.api.records.NodeReport} for the nodes in the cluster.
   * @throws Exception Propagates exceptions thrown by {@link org.apache.hadoop.yarn.client.api.YarnClient}.
   */
  public static final List<NodeReport> getNodeReports() throws Exception {
    return yarnAppClient.getNodeReports();
  }

  public static final <T> boolean waitForSize(Iterable<T> iterable, int count, int limit) throws InterruptedException {
    int trial = 0;
    int size = Iterables.size(iterable);
    while (size != count && trial < limit) {
      LOG.info("Waiting for {} size {} == {}", iterable, size, count);
      TimeUnit.SECONDS.sleep(1);
      trial++;
      size = Iterables.size(iterable);
    }
    return trial < limit;
  }
}
