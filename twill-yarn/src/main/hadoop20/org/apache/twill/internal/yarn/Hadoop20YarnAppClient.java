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

import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.appmaster.ApplicationMasterProcessLauncher;
import org.apache.twill.internal.appmaster.ApplicationSubmitter;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.DelegationToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public final class Hadoop20YarnAppClient extends AbstractIdleService implements YarnAppClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop20YarnAppClient.class);
  private final YarnClient yarnClient;
  private String user;

  public Hadoop20YarnAppClient(Configuration configuration) {
    this.yarnClient = new YarnClientImpl();
    yarnClient.init(configuration);
    this.user = System.getProperty("user.name");
  }

  @Override
  public ProcessLauncher<ApplicationId> createLauncher(TwillSpecification twillSpec) throws Exception {
    // Request for new application
    final GetNewApplicationResponse response = yarnClient.getNewApplication();
    final ApplicationId appId = response.getApplicationId();

    // Setup the context for application submission
    final ApplicationSubmissionContext appSubmissionContext = Records.newRecord(ApplicationSubmissionContext.class);
    appSubmissionContext.setApplicationId(appId);
    appSubmissionContext.setApplicationName(twillSpec.getName());
    appSubmissionContext.setUser(user);

    ApplicationSubmitter submitter = new ApplicationSubmitter() {

      @Override
      public ProcessController<YarnApplicationReport> submit(YarnLaunchContext launchContext, Resource capability) {
        ContainerLaunchContext context = launchContext.getLaunchContext();
        addRMToken(context);
        context.setUser(appSubmissionContext.getUser());
        context.setResource(adjustMemory(response, capability));
        appSubmissionContext.setAMContainerSpec(context);

        try {
          yarnClient.submitApplication(appSubmissionContext);
          return new ProcessControllerImpl(yarnClient, appId);
        } catch (YarnRemoteException e) {
          LOG.error("Failed to submit application {}", appId, e);
          throw Throwables.propagate(e);
        }
      }
    };

    return new ApplicationMasterProcessLauncher(appId, submitter);
  }

  private Resource adjustMemory(GetNewApplicationResponse response, Resource capability) {
    int minMemory = response.getMinimumResourceCapability().getMemory();

    int updatedMemory = Math.min(capability.getMemory(), response.getMaximumResourceCapability().getMemory());
    updatedMemory = (int) Math.ceil(((double) updatedMemory / minMemory)) * minMemory;

    if (updatedMemory != capability.getMemory()) {
      capability.setMemory(updatedMemory);
    }

    return capability;
  }

  private void addRMToken(ContainerLaunchContext context) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    try {
      Credentials credentials = YarnUtils.decodeCredentials(context.getContainerTokens());

      Configuration config = yarnClient.getConfig();
      Token<TokenIdentifier> token = convertToken(
        yarnClient.getRMDelegationToken(new Text(YarnUtils.getYarnTokenRenewer(config))),
        YarnUtils.getRMAddress(config));

      LOG.info("Added RM delegation token {}", token);
      credentials.addToken(token.getService(), token);

      context.setContainerTokens(YarnUtils.encodeCredentials(credentials));

    } catch (Exception e) {
      LOG.error("Fails to create credentials.", e);
      throw Throwables.propagate(e);
    }
  }

  private <T extends TokenIdentifier> Token<T> convertToken(DelegationToken protoToken, InetSocketAddress serviceAddr) {
    Token<T> token = new Token<T>(protoToken.getIdentifier().array(),
                                  protoToken.getPassword().array(),
                                  new Text(protoToken.getKind()),
                                  new Text(protoToken.getService()));
    if (serviceAddr != null) {
      SecurityUtil.setTokenService(token, serviceAddr);
    }
    return token;
  }

  @Override
  public ProcessLauncher<ApplicationId> createLauncher(String user, TwillSpecification twillSpec) throws Exception {
    this.user = user;
    return createLauncher(twillSpec);
  }

  @Override
  public ProcessController<YarnApplicationReport> createProcessController(ApplicationId appId) {
    return new ProcessControllerImpl(yarnClient, appId);
  }

  @Override
  protected void startUp() throws Exception {
    yarnClient.start();
  }

  @Override
  protected void shutDown() throws Exception {
    yarnClient.stop();
  }

  private static final class ProcessControllerImpl implements ProcessController<YarnApplicationReport> {
    private final YarnClient yarnClient;
    private final ApplicationId appId;

    public ProcessControllerImpl(YarnClient yarnClient, ApplicationId appId) {
      this.yarnClient = yarnClient;
      this.appId = appId;
    }

    @Override
    public YarnApplicationReport getReport() {
      try {
        return new Hadoop20YarnApplicationReport(yarnClient.getApplicationReport(appId));
      } catch (YarnRemoteException e) {
        LOG.error("Failed to get application report {}", appId, e);
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void cancel() {
      try {
        yarnClient.killApplication(appId);
      } catch (YarnRemoteException e) {
        LOG.error("Failed to kill application {}", appId, e);
        throw Throwables.propagate(e);
      }
    }
  }
}
