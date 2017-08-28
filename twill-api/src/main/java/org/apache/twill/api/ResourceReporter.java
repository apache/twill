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
package org.apache.twill.api;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides access to {@link ResourceReport}.
 */
public interface ResourceReporter {

  /**
   * Gets a snapshot of the resources used by the application, broken down by each runnable.
   *
   * @return A {@link ResourceReport} containing information about resources used by the application, or
   *         {@code null} will be returned if the report is not available.
   */
  @Nullable
  ResourceReport getResourceReport();

  /**
   * Gets a snapshot of the resources used by the application master.
   *
   * @return A {@link TwillRunResources} containing information about the resources used by the application master, or
   *         {@code null} will be returned if the report is not available.
   */
  @Nullable
  TwillRunResources getApplicationMasterResources();

  /**
   * Gets a snapshot of the resources used by runnables instances, grouped by runnable.
   *
   * @return A {@link Map} from runnable name to a {@link Collection} of {@link TwillRunResources} containing
   *         information about the resources used by each runnable instance. if the report is not available or
   *         if a runnable doesn't have any running instance, it will not have an entry in the returned map.
   */
  Map<String, Collection<TwillRunResources>> getRunnablesResources();

  /**
   * Gets a snapshot of the resources used by runnable instances.
   *
   * @param runnableName name of the runnable
   * @return A {@link Collection} of {@link TwillRunResources} containing information
   *         about the resources used by each runnable instance. An empty collection will be returned
   *         if the report is not available or if there is no instance of that runnable is running.
   */
  Collection<TwillRunResources> getInstancesResources(String runnableName);
}
