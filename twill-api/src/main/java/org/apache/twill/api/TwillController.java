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

import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

/**
 * For controlling a running application.
 */
public interface TwillController extends ServiceController {

  /**
   * Adds a {@link LogHandler} for receiving application log.
   * @param handler The handler to add.
   */
  void addLogHandler(LogHandler handler);

  /**
   * Discovers the set of {@link Discoverable} endpoints that provides service for the given service name.
   * @param serviceName Name of the service to discovery.
   * @return A {@link org.apache.twill.discovery.ServiceDiscovered} object representing the result.
   */
  ServiceDiscovered discoverService(String serviceName);

  /**
   * Changes the number of running instances of a given runnable.
   *
   * @param runnable The name of the runnable.
   * @param newCount Number of instances for the given runnable.
   * @return A {@link Future} that will be completed when the number running instances has been
   *         successfully changed. The future will carry the new count as the result. If there is any error
   *         while changing instances, it'll be reflected in the future.
   */
  Future<Integer> changeInstances(String runnable, int newCount);

  /**
   * Get a snapshot of the resources used by the application, broken down by each runnable.
   *
   * @return A {@link ResourceReport} containing information about resources used by the application or
   *         null in case the user calls this before the application completely starts.
   */
  @Nullable
  ResourceReport getResourceReport();

  /**
   * Restart all instances of a particular {@link TwillRunnable}.
   *
   * @param runnable The name of the runnable to restart.
   * @return A {@link Future} that will be completed when the restart operation has been done.
   */
  Future<String> restartAllInstances(String runnable);

  /**
   * Restart instances of some {@link TwillRunnable}.
   *
   * @param runnableToInstanceIds A map of runnable ID to list of instance IDs to be restarted.
   * @return A {@link Future} that will be completed when the restart operation has been done.
   */
  Future<Set<String>> restartInstances(Map<String, ? extends Set<Integer>> runnableToInstanceIds);

  /**
   * Restart instances of some {@link TwillRunnable}.
   *
   * @param runnable The name of the runnable to restart.
   * @param instanceId The main instance id to be restarted.
   * @param moreInstanceIds The optional instance ids.
   * @return A {@link Future} that will be completed when the restart operation has been done.
   */
  Future<String> restartInstances(String runnable, int instanceId, int... moreInstanceIds);

  /**
   * Set the root log level for Twill applications running in a container.
   *
   * @param logLevel The log level for the root logger to change.
   * @return A {@link Future} that will be completed when the set log level operation has been done.
   */
  Future<String> setLogLevel(LogEntry.Level logLevel);

  /**
   * Set the log levels for requested logger names for Twill applications running in a container.
   *
   * @param logLevelAppArgs The {@link Map} contains the requested logger names and log levels that need to be set.
   * @return A {@link Future} that will be completed when the set log level operation has been done.
   */
  Future<String> setLogLevel(Map<String, LogEntry.Level> logLevelAppArgs);

  /**
   * Set the log levels for requested logger names for a {@link TwillRunnable}.
   *
   * @param runnableName The name of the runnable to set the log level.
   * @param logLevels The {@link Map} contains the requested logger name and log level that need to be changed.
   * @return A {@link Future} that will be completed when the set log level operation has been done.
   */
  Future<String> setLogLevel(String runnableName, Map<String, LogEntry.Level> logLevels);
}
