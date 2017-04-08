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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * The service implementation of {@link YarnAppClient} for Apache Hadoop 2.3 and beyond.
 *
 * The {@link VersionDetectYarnAppClientFactory} class will decide to return instance of this class for
 * Apache Hadoop 2.3 and beyond.
 * </p>
 */
@SuppressWarnings("unused")
public final class Hadoop23YarnAppClient extends Hadoop21YarnAppClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop23YarnAppClient.class);
  private final Configuration configuration;

  public Hadoop23YarnAppClient(Configuration configuration) {
    super(configuration);
    this.configuration = configuration;
  }

  /**
   * Overrides parent method to adds RM delegation token to the given context. If YARN is running with HA RM,
   * delegation tokens for each RM service will be added.
   */
  protected void addRMToken(ContainerLaunchContext context, YarnClient yarnClient, ApplicationId appId) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    try {
      Text renewer = new Text(UserGroupInformation.getCurrentUser().getShortUserName());
      org.apache.hadoop.yarn.api.records.Token rmDelegationToken = yarnClient.getRMDelegationToken(renewer);

      // The following logic is copied from ClientRMProxy.getRMDelegationTokenService, which is not available in
      // YARN older than 2.4
      List<String> services = new ArrayList<>();
      if (HAUtil.isHAEnabled(configuration)) {
        // If HA is enabled, we need to enumerate all RM hosts
        // and add the corresponding service name to the token service
        // Copy the yarn conf since we need to modify it to get the RM addresses
        YarnConfiguration yarnConf = new YarnConfiguration(configuration);
        for (String rmId : HAUtil.getRMHAIds(configuration)) {
          yarnConf.set(YarnConfiguration.RM_HA_ID, rmId);
          InetSocketAddress address = yarnConf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                                                             YarnConfiguration.DEFAULT_RM_ADDRESS,
                                                             YarnConfiguration.DEFAULT_RM_PORT);
          services.add(SecurityUtil.buildTokenService(address).toString());
        }
      } else {
        services.add(SecurityUtil.buildTokenService(YarnUtils.getRMAddress(configuration)).toString());
      }

      Credentials credentials = YarnUtils.decodeCredentials(context.getTokens());

      // casting needed for later Hadoop version
      @SuppressWarnings("RedundantCast")
      Token<TokenIdentifier> token = ConverterUtils.convertFromYarn(rmDelegationToken, (InetSocketAddress) null);

      token.setService(new Text(Joiner.on(',').join(services)));
      credentials.addToken(new Text(token.getService()), token);

      LOG.debug("Added RM delegation token {} for application {}", token, appId);
      credentials.addToken(token.getService(), token);

      context.setTokens(YarnUtils.encodeCredentials(credentials));

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
