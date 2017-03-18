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
package org.apache.twill.internal.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.api.Configs;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.appmaster.ApplicationMasterInfo;
import org.apache.twill.internal.appmaster.ApplicationMasterProcessLauncher;
import org.apache.twill.internal.appmaster.ApplicationSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * <p>
 * The service implementation of {@link YarnAppClient} for Apache Hadoop 2.1 and beyond.
 *
 * The {@link VersionDetectYarnAppClientFactory} class will decide to return instance of this class for
 * Apache Hadoop 2.1 and beyond.
 * </p>
 */
@SuppressWarnings("unused")
public class Hadoop21YarnAppClient implements YarnAppClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop21YarnAppClient.class);
  private final Configuration configuration;

  public Hadoop21YarnAppClient(Configuration configuration) {
    this.configuration = configuration;
  }

  // Creates and starts a yarn client
  private YarnClient createYarnClient() {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(configuration);
    yarnClient.start();
    return yarnClient;
  }

  @Override
  public ProcessLauncher<ApplicationMasterInfo> createLauncher(TwillSpecification twillSpec,
                                                               @Nullable String schedulerQueue) throws Exception {
    YarnClient yarnClient = createYarnClient();
    try {
      // Request for new application
      YarnClientApplication application = yarnClient.createApplication();
      final GetNewApplicationResponse response = application.getNewApplicationResponse();
      final ApplicationId appId = response.getApplicationId();

      // Setup the context for application submission
      final ApplicationSubmissionContext appSubmissionContext = application.getApplicationSubmissionContext();
      appSubmissionContext.setApplicationId(appId);
      appSubmissionContext.setApplicationName(twillSpec.getName());

      if (schedulerQueue != null) {
        appSubmissionContext.setQueue(schedulerQueue);
      }

      // Set the resource requirement for AM
      int memoryMB = configuration.getInt(Configs.Keys.YARN_AM_MEMORY_MB, Configs.Defaults.YARN_AM_MEMORY_MB);
      final Resource capability = adjustMemory(response, Resource.newInstance(memoryMB, 1));
      ApplicationMasterInfo appMasterInfo = new ApplicationMasterInfo(appId, capability.getMemory(),
                                                                      capability.getVirtualCores());

      ApplicationSubmitter submitter = new ApplicationSubmitter() {
        @Override
        public ProcessController<YarnApplicationReport> submit(YarnLaunchContext context) {
          ContainerLaunchContext launchContext = context.getLaunchContext();

          YarnClient yarnClient = createYarnClient();
          try {
            addRMToken(launchContext, yarnClient, appId);
            appSubmissionContext.setAMContainerSpec(launchContext);
            appSubmissionContext.setResource(capability);
            appSubmissionContext.setMaxAppAttempts(2);

            yarnClient.submitApplication(appSubmissionContext);
            return new ProcessControllerImpl(appId);
          } catch (YarnException | IOException e) {
            throw new RuntimeException("Failed to submit application " + appId, e);
          } finally {
            yarnClient.stop();
          }
        }
      };

      return new ApplicationMasterProcessLauncher(appMasterInfo, submitter);
    } finally {
      yarnClient.stop();
    }
  }

  private Resource adjustMemory(GetNewApplicationResponse response, Resource capability) {
    int maxMemory = response.getMaximumResourceCapability().getMemory();
    int updatedMemory = capability.getMemory();

    if (updatedMemory > maxMemory) {
      capability.setMemory(maxMemory);
    }

    return capability;
  }

  /**
   * Adds RM delegation token to the given {@link ContainerLaunchContext} so that the AM can authenticate itself
   * with RM using the delegation token.
   */
  protected void addRMToken(ContainerLaunchContext context, YarnClient yarnClient, ApplicationId appId) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    try {
      Credentials credentials = YarnUtils.decodeCredentials(context.getTokens());

      Configuration config = yarnClient.getConfig();
      Token<TokenIdentifier> token = ConverterUtils.convertFromYarn(
        yarnClient.getRMDelegationToken(new Text(YarnUtils.getYarnTokenRenewer(config))),
        YarnUtils.getRMAddress(config));

      LOG.debug("Added RM delegation token {} for application {}", token, appId);
      credentials.addToken(token.getService(), token);

      context.setTokens(YarnUtils.encodeCredentials(credentials));
    } catch (YarnException | IOException e) {
      throw new RuntimeException("Failed to acquire RM delegation token", e);
    }
  }

  @Override
  public ProcessLauncher<ApplicationMasterInfo> createLauncher(String user,
                                                               TwillSpecification twillSpec,
                                                               @Nullable String schedulerQueue) throws Exception {
    // Ignore user
    return createLauncher(twillSpec, schedulerQueue);
  }

  @Override
  public ProcessController<YarnApplicationReport> createProcessController(ApplicationId appId) {
    return new ProcessControllerImpl(appId);
  }

  @Override
  public List<NodeReport> getNodeReports() throws Exception {
    YarnClient yarnClient = createYarnClient();
    try {
      return yarnClient.getNodeReports();
    } finally {
      yarnClient.stop();
    }
  }

  private final class ProcessControllerImpl implements ProcessController<YarnApplicationReport> {
    private final ApplicationId appId;

    ProcessControllerImpl(ApplicationId appId) {
      this.appId = appId;
    }

    @Override
    public YarnApplicationReport getReport() {
      YarnClient yarnClient = createYarnClient();
      try {
        return new Hadoop21YarnApplicationReport(yarnClient.getApplicationReport(appId));
      } catch (YarnException | IOException e) {
        throw new RuntimeException("Failed to get application report for " + appId, e);
      } finally {
        yarnClient.stop();
      }
    }

    @Override
    public void cancel() {
      YarnClient yarnClient = createYarnClient();
      try {
        yarnClient.killApplication(appId);
      } catch (YarnException | IOException e) {
        throw new RuntimeException("Failed to kill application " + appId, e);
      } finally {
        yarnClient.stop();
      }
    }

    @Override
    public void close() throws Exception {
      // no-op
    }
  }
}
