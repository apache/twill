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
package org.apache.twill.internal;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A {@link ContainerInfo} based on information on the environment.
 */
public final class EnvContainerInfo implements ContainerInfo {
  private final String id;
  private final InetAddress host;
  private final int port;
  private final int virtualCores;
  private final int memoryMB;

  public EnvContainerInfo() throws UnknownHostException {
    id = System.getenv(EnvKeys.YARN_CONTAINER_ID);
    host = InetAddress.getByName(System.getenv(EnvKeys.YARN_CONTAINER_HOST));
    port = Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_PORT));
    virtualCores = Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES));
    memoryMB = Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_MEMORY_MB));
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public InetAddress getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getMemoryMB() {
    return memoryMB;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }
}
