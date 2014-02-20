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

import com.google.common.collect.ImmutableList;
import org.apache.twill.api.ResourceSpecification;

import java.util.Collections;
import java.util.List;

/**
 * Straightforward implementation of {@link org.apache.twill.api.ResourceSpecification}.
 */
public final class DefaultResourceSpecification implements ResourceSpecification {
  private final int virtualCores;
  private final int memorySize;
  private final int instances;
  private final int uplink;
  private final int downlink;
  private final List<String> hosts;
  private final List<String> racks;

  public DefaultResourceSpecification(int virtualCores, int memorySize, int instances, int uplink, int downlink) {
    this(virtualCores, memorySize, instances, uplink, downlink,
            Collections.<String>emptyList(), Collections.<String>emptyList());
  }

  public DefaultResourceSpecification(int virtualCores, int memorySize, int instances,
                                      int uplink, int downlink,
                                      List<String> hosts, List<String> racks) {
    this.virtualCores = virtualCores;
    this.memorySize = memorySize;
    this.instances = instances;
    this.uplink = uplink;
    this.downlink = downlink;
    this.hosts = ImmutableList.copyOf(hosts);
    this.racks = ImmutableList.copyOf(racks);
  }

  @Deprecated
  @Override
  public int getCores() {
    return virtualCores;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  @Override
  public int getMemorySize() {
    return memorySize;
  }

  @Override
  public int getInstances() {
    return instances;
  }

  @Override
  public List<String> getHosts() {
    return this.hosts;
  }

  @Override
  public List<String> getRacks() {
    return this.racks;
  }

  @Override
  public int getUplink() {
    return uplink;
  }

  @Override
  public int getDownlink() {
    return downlink;
  }
}
