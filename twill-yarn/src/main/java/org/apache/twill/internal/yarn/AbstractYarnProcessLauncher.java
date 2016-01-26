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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.api.LocalFile;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.ResourceCapability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract class to help creating different types of process launcher that process on yarn.
 *
 * @param <T> Type of the object that contains information about the container that the process is going to launch.
 */
public abstract class AbstractYarnProcessLauncher<T extends ResourceCapability> implements ProcessLauncher<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractYarnProcessLauncher.class);

  private final T containerInfo;

  protected AbstractYarnProcessLauncher(T containerInfo) {
    this.containerInfo = containerInfo;
  }

  @Override
  public T getContainerInfo() {
    return containerInfo;
  }

  @Override
  public <C> PrepareLaunchContext prepareLaunch(Map<String, String> environments,
                                                Iterable<LocalFile> resources, C credentials) {
    if (credentials != null) {
      Preconditions.checkArgument(credentials instanceof Credentials, "Credentials should be of type %s",
                                  Credentials.class.getName());
    }
    return new PrepareLaunchContextImpl(environments, resources, (Credentials) credentials);
  }

  /**
   * For children class to override to perform actual process launching.
   */
  protected abstract <R> ProcessController<R> doLaunch(YarnLaunchContext launchContext);

  /**
   * Implementation for the {@link PrepareLaunchContext}.
   */
  private final class PrepareLaunchContextImpl implements PrepareLaunchContext {

    private final Credentials credentials;
    private final YarnLaunchContext launchContext;
    private final Map<String, YarnLocalResource> localResources;
    private final Map<String, String> environment;
    private final List<String> commands;

    private PrepareLaunchContextImpl(Map<String, String> env, Iterable<LocalFile> localFiles, Credentials credentials) {
      this.credentials = credentials;
      this.launchContext = YarnUtils.createLaunchContext();
      this.localResources = Maps.newHashMap();
      this.environment = Maps.newHashMap(env);
      this.commands = Lists.newLinkedList();

      for (LocalFile localFile : localFiles) {
        addLocalFile(localFile);
      }
    }

    private void addLocalFile(LocalFile localFile) {
      localResources.put(localFile.getName(), YarnUtils.createLocalResource(localFile));
    }

    @Override
    public PrepareLaunchContext addResources(LocalFile... localFiles) {
      return addResources(Arrays.asList(localFiles));
    }

    @Override
    public PrepareLaunchContext addResources(Iterable<LocalFile> localFiles) {
      for (LocalFile localFile : localFiles) {
        addLocalFile(localFile);
      }
      return this;
    }

    @Override
    public <V> PrepareLaunchContext addEnvironment(String key, V value) {
      environment.put(key, value.toString());
      return this;
    }

    @Override
    public PrepareLaunchContext addCommand(String cmd, String... args) {
      StringBuilder builder = new StringBuilder(cmd);
      for (String arg : args) {
        builder.append(' ').append(arg);
      }

      // Redirect stdout and stderr
      redirect(1, ApplicationConstants.STDOUT, builder);
      redirect(2, ApplicationConstants.STDERR, builder);

      commands.add(builder.toString());
      return this;
    }

    @Override
    public <R> ProcessController<R> launch() {
      launchContext.setLocalResources(localResources);
      launchContext.setEnvironment(environment);
      if (credentials != null && !credentials.getAllTokens().isEmpty()) {
        for (Token<?> token : credentials.getAllTokens()) {
          LOG.info("Launch with delegation token {}", token);
        }
        launchContext.setCredentials(credentials);
      }
      launchContext.setCommands(commands);
      return doLaunch(launchContext);
    }

    private void redirect(int type, String out, StringBuilder commandBuilder) {
      commandBuilder.append(' ')
        .append(type).append('>')
        .append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append('/').append(out);
    }
  }
}
