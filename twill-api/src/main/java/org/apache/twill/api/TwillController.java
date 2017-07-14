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
   * Restart instances of some {@link TwillRunnable}.
   *
   * @param runnable    The name of the runnable to restart.
   * @param instanceIds Instances to be restarted
   * @return A {@link Future} that will be completed when the restart operation has been done.
   */
  Future<String> restartInstances(String runnable, Set<Integer> instanceIds);

  /**
   * Update the log levels for requested logger names for Twill applications running in a container.
   * The log level for a logger name can be {@code null} except for the root logger, which will reset the log level for
   * the specified logger.
   *
   * @param logLevels The {@link Map} contains the requested logger names and log levels that need to be updated.
   * @return A {@link Future} that will be completed when the log level update has been done. It will carry the
   *         {@link Map} of log levels as the result.
   */
  Future<Map<String, LogEntry.Level>> updateLogLevels(Map<String, LogEntry.Level> logLevels);

  /**
   * Update the log levels for requested logger names for a {@link TwillRunnable}.
   * The log level for a logger name can be {@code null} except for the root logger,
   * which will reset the log level for
   * the specified logger.
   *
   * @param runnableName The name of the runnable to update the log level.
   * @param logLevelsForRunnable The {@link Map} contains the requested logger name and log level that
   *                             need to be updated.
   * @return A {@link Future} that will be completed when the log level update has been done. It will carry the
   *         {@link Map} of log levels as the result.
   */
  Future<Map<String, LogEntry.Level>> updateLogLevels(String runnableName,
                                                      Map<String, LogEntry.Level> logLevelsForRunnable);

  /**
   * Reset the log levels of all runnables.
   * The log levels will be the same as when the runnables start up.
   *
   * @param loggerNames The optional logger names to be reset for all runnables, if not provided, all log levels will
   *                    be reset.
   * @return A {@link Future} that will be completed when the set log level operation has been done. The future result
   *         is the logger names provided in the parameter.
   */
  Future<String[]> resetLogLevels(String...loggerNames);

  /**
   * Reset the log levels of the given runnable.
   * The log levels will be same as when the runnable starts up.
   *
   * @param loggerNames The optional logger names to be reset for the runnable, if not provided, all log levels will
   *                    be reset.
   * @return A {@link Future} that will be completed when the set log level operation has been done. The future result
   *         is the logger names provided in the parameter.
   */
  Future<String[]> resetRunnableLogLevels(String runnableName, String...loggerNames);

}
