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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * This interface is for controlling a remote running service.
 */
public interface ServiceController {

  /**
   * Returns the {@link RunId} of the running application.
   */
  RunId getRunId();

  /**
   * Sends a user command to the running application.
   * @param command The command to send.
   * @return A {@link Future} that will be completed when the command is successfully processed
   *         by the target application.
   */
  Future<Command> sendCommand(Command command);

  /**
   * Sends a user command to the given runnable of the running application.
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param command The command to send.
   * @return A {@link Future} that will be completed when the command is successfully processed
   *         by the target runnable.
   */
  Future<Command> sendCommand(String runnableName, Command command);

  /**
   * Requests to terminate the running service. The service will be given chance to shutdown gracefully.
   * This method returns immediately and caller can get the termination state through the future returned.
   * Calling this method multiple times is allowed and a {@link Future} representing the termination state
   * will be returned.
   *
   * @return a {@link Future} that represents the termination of the service. The future result will be
   * this {@link ServiceController}. If the service terminated with a {@link TerminationStatus#FAILED} status,
   * calling the {@link Future#get()} on the returning future will throw {@link ExecutionException}.
   */
  Future<? extends ServiceController> terminate();

  /**
   * Requests to forcefully kill a running service.
   */
  void kill();

  /**
   * Attaches a {@link Runnable} that will get executed when the service is running.
   *
   * @param runnable the {@link Runnable} to be executed when the service is running.
   * @param executor the executor in which the runnable will be executed with.
   */
  void onRunning(Runnable runnable, Executor executor);

  /**
   * Attaches a {@link Runnable} that will get executed when the serivce is terminated.
   *
   * @param runnable the {@link Runnable} to be executed when the service is terminated.
   * @param executor the executor in which the runnable will be executed with.
   */
  void onTerminated(Runnable runnable, Executor executor);

  /**
   * Waits for termination of the remote service.
   *
   * @throws ExecutionException if the service terminated due to exception.
   */
  void awaitTerminated() throws ExecutionException;

  /**
   * Waits for termination of the remote service for no more than the given timeout limit.
   *
   * @param timeout the maximum time to wait
   * @param timeoutUnit the time unit of the timeout
   * @throws TimeoutException if the service is not terminated within the given time.
   * @throws ExecutionException if the service terminated due to exception.
   */
  void awaitTerminated(long timeout, TimeUnit timeoutUnit) throws TimeoutException, ExecutionException;

  /**
   * Gets the termination status of the application represented by this controller.
   *
   * @return the termination status or {@code null} if the application is still running
   */
  @Nullable
  TerminationStatus getTerminationStatus();

  /**
   * Enum to represent termination status of the application when it completed.
   */
  enum TerminationStatus {
    /**
     * Application was completed successfully.
     */
    SUCCEEDED,

    /**
     * Application was killed explicitly.
     */
    KILLED,

    /**
     * Application failed.
     */
    FAILED
  }
}
