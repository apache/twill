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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillRunResources;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link org.apache.twill.api.ResourceReport} with some
 * additional methods for maintaining the report.
 */
public final class DefaultResourceReport implements ResourceReport {
  private final SetMultimap<String, TwillRunResources> usedResources;
  private final TwillRunResources appMasterResources;
  private final String applicationId;
  private final AtomicReference<List<String>> services;

  public DefaultResourceReport(String applicationId, TwillRunResources masterResources) {
    this(applicationId, masterResources, Collections.<String, Collection<TwillRunResources>>emptyMap(),
         Collections.<String>emptyList());
  }

  public DefaultResourceReport(String applicationId, TwillRunResources masterResources,
                               Map<String, Collection<TwillRunResources>> resources, List<String> services) {
    this.applicationId = applicationId;
    this.appMasterResources = masterResources;
    this.usedResources = HashMultimap.create();
    for (Map.Entry<String, Collection<TwillRunResources>> entry : resources.entrySet()) {
      this.usedResources.putAll(entry.getKey(), entry.getValue());
    }
    this.services = new AtomicReference<>(services);
  }

  /**
   * Add resources used by an instance of the runnable.
   *
   * @param runnableName name of runnable.
   * @param resources resources to add.
   */
  public void addRunResources(String runnableName, TwillRunResources resources) {
    usedResources.put(runnableName, resources);
  }

  /**
   * Remove the resource corresponding to the given runnable and container.
   *
   * @param runnableName name of runnable.
   * @param containerId container id of the runnable.
   */
  public void removeRunnableResources(String runnableName, String containerId) {
    TwillRunResources toRemove = null;
    // could be faster if usedResources was a Table, but that makes returning the
    // report a little more complex, and this does not need to be terribly fast.
    for (TwillRunResources resources : usedResources.get(runnableName)) {
      if (resources.getContainerId().equals(containerId)) {
        toRemove = resources;
        break;
      }
    }
    usedResources.remove(runnableName, toRemove);
  }

  /**
   * Get all the run resources being used by all instances of the specified runnable.
   *
   * @param runnableName the runnable name.
   * @return resources being used by all instances of the runnable.
   */
  @Override
  public Collection<TwillRunResources> getRunnableResources(String runnableName) {
    return usedResources.get(runnableName);
  }

  /**
   * Get all the run resources being used across all runnables.
   *
   * @return all run resources used by all instances of all runnables.
   */
  @Override
  public Map<String, Collection<TwillRunResources>> getResources() {
    return Multimaps.unmodifiableSetMultimap(usedResources).asMap();
  }

  /**
   * Get the resources application master is using.
   *
   * @return resources being used by the application master.
   */
  @Override
  public TwillRunResources getAppMasterResources() {
    return appMasterResources;
  }

  /**
   * Get the id of the application master.
   *
   * @return id of the application master.
   */
  @Override
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * Set the list of services of the application master.
   *
   * @param services list of services to set.
   */
  public void setServices(List<String> services) {
    this.services.set(ImmutableList.copyOf(services));
  }

  /**
   * Get the list of services of the application master.
   *
   * @return list of services of the application master.
   */
  @Override
  public List<String> getServices() {
    return services.get();
  }

  @Override
  public String toString() {
    return "DefaultResourceReport{" +
      "usedResources=" + usedResources +
      ", appMasterResources=" + appMasterResources +
      ", applicationId='" + applicationId + '\'' +
      ", services=" + services +
      '}';
  }
}
