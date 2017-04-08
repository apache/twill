/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.util.Records;
import org.apache.twill.api.Configs;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.yarn.VersionDetectYarnAppClientFactory;
import org.apache.twill.internal.yarn.YarnAppClient;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A TwillTester rule allows creation of mini Yarn cluster and {@link TwillRunner} used for testing that is
 * guaranteed to be teared down when tests are done.
 *
 * <pre>
 *   public class TwillTest {
 *     &#064;ClassRule
 *     public static final TwillTester TWILL_TESTER = new TwillTester();
 *
 *     &#064;Test
 *     public void test() {
 *       TwillRunner twillRunner = TWILL_TESTER.getTwillRunner();
 *       twillRunner.prepare(...).start();
 *     }
 *   }
 * </pre>
 */
public class TwillTester extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(TwillTester.class);

  private final TemporaryFolder tmpFolder = new TemporaryFolder();
  private final Map<String, String> extraConfig;

  private InMemoryZKServer zkServer;
  private MiniDFSCluster dfsCluster;
  private MiniYARNCluster cluster;
  private YarnConfiguration config;
  private TwillRunnerService twillRunner;
  private YarnAppClient yarnAppClient;

  /**
   * Creates a new instance with the give list of configurations.
   *
   * @param configs list of configuration pairs.
   *                The list must be in the form of {@code (key1, value1, key2, value2, ...)},
   *                hence the length of configs must be even.
   *                The {@link Object#toString()} method will be called to obtain the keys and values that go into
   *                the configuration.
   */
  public TwillTester(Object...configs) {
    Preconditions.checkArgument(configs.length % 2 == 0,
                                "Arguments must be in pair form like (k1, v1, k2, v2): %s", Arrays.toString(configs));

    this.extraConfig = new HashMap<>();
    for (int i = 0; i < configs.length; i += 2) {
      this.extraConfig.put(configs[i].toString(), configs[i + 1].toString());
    }
  }

  @Override
  protected void before() throws Throwable {
    tmpFolder.create();

    // Starts Zookeeper
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    // Start YARN mini cluster
    File miniDFSDir = tmpFolder.newFolder();
    LOG.info("Starting Mini DFS on path {}", miniDFSDir);
    Configuration fsConf = new HdfsConfiguration(new Configuration());
    fsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, miniDFSDir.getAbsolutePath());

    for (Map.Entry<String, String> entry : extraConfig.entrySet()) {
      fsConf.set(entry.getKey(), entry.getValue());
    }

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
    conf.set("yarn.nodemanager.vmem-pmem-ratio", "100.1");
    conf.set("yarn.nodemanager.vmem-check-enabled", "false");
    conf.set("yarn.scheduler.minimum-allocation-mb", "128");
    conf.set("yarn.nodemanager.delete.debug-delay-sec", "3600");

    conf.set(Configs.Keys.LOCAL_STAGING_DIRECTORY, tmpFolder.newFolder().getAbsolutePath());

    cluster = new MiniYARNCluster("test-cluster", 3, 1, 1);
    cluster.init(conf);
    cluster.start();

    config = new YarnConfiguration(cluster.getConfig());

    twillRunner = createTwillRunnerService();
    twillRunner.start();

    yarnAppClient = new VersionDetectYarnAppClientFactory().create(conf);
  }

  @Override
  protected void after() {
    // Stop all runnable applications
    for (TwillRunner.LiveInfo info : twillRunner.lookupLive()) {
      for (TwillController controller : info.getControllers()) {
        try {
          controller.terminate().get();
        } catch (Exception e) {
          LOG.warn("Exception raised when awaiting termination of {}", info.getApplicationName());
        }
      }
    }

    try {
      twillRunner.stop();
    } catch (Exception e) {
      LOG.warn("Failed to stop TwillRunner", e);
    }
    try {
      cluster.stop();
    } catch (Exception e) {
      LOG.warn("Failed to stop mini Yarn cluster", e);
    }
    try {
      dfsCluster.shutdown();
    } catch (Exception e) {
      LOG.warn("Failed to stop mini dfs cluster", e);
    }
    stopQuietly(zkServer);

    tmpFolder.delete();
  }

  /**
   * Creates an unstarted instance of {@link org.apache.twill.api.TwillRunnerService}.
   */
  public TwillRunnerService createTwillRunnerService() throws IOException {
    YarnTwillRunnerService runner = new YarnTwillRunnerService(config, zkServer.getConnectionStr() + "/twill",
                                                               createLocationFactory());
    // disable tests stealing focus
    runner.setJVMOptions("-Djava.awt.headless=true");
    return runner;
  }

  /**
   * Creates a {@link LocationFactory} for the mini YARN cluster.
   */
  public LocationFactory createLocationFactory() {
    try {
      FileContext fc = FileContext.getFileContext(config);
      String basePath = fc.getHomeDirectory().toUri().getPath();
      return new FileContextLocationFactory(config, basePath);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns a {@link TwillRunner} that interact with the mini Yarn cluster.
   */
  public TwillRunner getTwillRunner() {
    return twillRunner;
  }

  /**
   * Returns a list of {@link NodeReport} about the mini yarn cluster.
   */
  public List<NodeReport> getNodeReports() throws Exception {
    return yarnAppClient.getNodeReports();
  }

  public String getZKConnectionString() {
    return zkServer.getConnectionStr();
  }

  public ApplicationResourceUsageReport getApplicationResourceReport(String appId) throws Exception {
    List<String> splits = Lists.newArrayList(Splitter.on('_').split(appId));
    Preconditions.checkArgument(splits.size() == 3, "Invalid application id - " + appId);
    ApplicationId applicationId =
      YarnUtils.createApplicationId(Long.parseLong(splits.get(1)), Integer.parseInt(splits.get(2)));

    ClientRMService clientRMService = cluster.getResourceManager().getClientRMService();
    GetApplicationReportRequest request = Records.newRecord(GetApplicationReportRequest.class);
    request.setApplicationId(applicationId);
    return clientRMService.getApplicationReport(request)
      .getApplicationReport().getApplicationResourceUsageReport();
  }

  private void stopQuietly(Service service) {
    try {
      service.stopAndWait();
    } catch (Exception e) {
      LOG.warn("Failed to stop service {}.", service, e);
    }
  }
}
