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

import com.google.common.base.Throwables;
import org.apache.hadoop.yarn.api.records.Container;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 */
public final class Hadoop20YarnContainerInfo implements YarnContainerInfo {

  private final Container container;

  public Hadoop20YarnContainerInfo(Container container) {
    this.container = container;
  }

  @Override
  public <T> T getContainer() {
    return (T) container;
  }

  @Override
  public String getId() {
    return container.getId().toString();
  }

  @Override
  public InetAddress getHost() {
    try {
      return InetAddress.getByName(container.getNodeId().getHost());
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getPort() {
    return container.getNodeId().getPort();
  }

  @Override
  public int getMemoryMB() {
    return container.getResource().getMemory();
  }

  @Override
  public int getVirtualCores() {
    return YarnUtils.getVirtualCores(container.getResource());
  }
}
