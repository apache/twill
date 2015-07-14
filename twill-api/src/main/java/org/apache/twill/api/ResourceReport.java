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
package org.apache.twill.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This interface provides a snapshot of the resources an application is using
 * broken down by each runnable.
 */
public interface ResourceReport {
  /**
   * Get all the run resources being used by all instances of the specified runnable.
   *
   * @param runnableName the runnable name.
   * @return resources being used by all instances of the runnable.
   */
  Collection<TwillRunResources> getRunnableResources(String runnableName);

  /**
   * Get all the run resources being used across all runnables.
   *
   * @return all run resources used by all instances of all runnables.
   */
  Map<String, Collection<TwillRunResources>> getResources();

  /**
   * Get the resources application master is using.
   *
   * @return resources being used by the application master.
   */
  TwillRunResources getAppMasterResources();

  /**
   * Get the id of the application master.
   *
   * @return id of the application master.
   */
  String getApplicationId();

  /**
   * Get the list of services of the application master.
   *
   * @return list of services of the application master.
   */
  List<String> getServices();
}
