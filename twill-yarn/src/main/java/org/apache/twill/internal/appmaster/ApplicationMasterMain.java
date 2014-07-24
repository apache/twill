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

import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.ServiceMain;
import org.apache.twill.internal.yarn.VersionDetectYarnAMClientFactory;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * Main class for launching {@link ApplicationMasterService}.
 */
public final class ApplicationMasterMain extends ServiceMain {

  private final String kafkaZKConnect;

  private ApplicationMasterMain(String kafkaZKConnect) {
    this.kafkaZKConnect = kafkaZKConnect;
  }

  /**
   * Starts the application master.
   */
  public static void main(String[] args) throws Exception {
    String zkConnect = System.getenv(EnvKeys.TWILL_ZK_CONNECT);
    File twillSpec = new File(Constants.Files.TWILL_SPEC);
    RunId runId = RunIds.fromString(System.getenv(EnvKeys.TWILL_RUN_ID));

    ZKClientService zkClientService =
      ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zkConnect).build(),
            RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));

    Configuration conf = new YarnConfiguration(new HdfsConfiguration(new Configuration()));
    setRMSchedulerAddress(conf);
    Service service = new ApplicationMasterService(runId, zkClientService, twillSpec,
                                                   new VersionDetectYarnAMClientFactory(conf), createAppLocation(conf));
    new ApplicationMasterMain(String.format("%s/%s/kafka", zkConnect, runId.getId())).doMain(zkClientService, service);
  }

  /**
   * Optionally sets the RM scheduler address based on the environment variable if it is not set in the cluster config.
   */
  private static void setRMSchedulerAddress(Configuration conf) {
    String schedulerAddress = System.getenv(EnvKeys.YARN_RM_SCHEDULER_ADDRESS);
    if (schedulerAddress == null) {
      return;
    }

    // If the RM scheduler address is not in the config or it's from yarn-default.xml,
    // replace it with the one from the env, which is the same as the one client connected to.
    String[] sources = conf.getPropertySources(YarnConfiguration.RM_SCHEDULER_ADDRESS);
    if (sources == null || sources.length == 0 || "yarn-default.xml".equals(sources[sources.length - 1])) {
      conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress);
    }
  }

  @Override
  protected String getHostname() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return "unknown";
    }
  }

  @Override
  protected String getKafkaZKConnect() {
    return kafkaZKConnect;
  }
}
