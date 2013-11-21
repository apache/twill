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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.util.concurrent.Executor;

/**
 * This interface is for controlling a remote running service.
 */
public interface ServiceController extends Service {

  /**
   * Returns the {@link RunId} of the running application.
   */
  RunId getRunId();

  /**
   * Sends a user command to the running application.
   * @param command The command to send.
   * @return A {@link ListenableFuture} that will be completed when the command is successfully processed
   *         by the target application.
   */
  ListenableFuture<Command> sendCommand(Command command);

  /**
   * Sends a user command to the given runnable of the running application.
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param command The command to send.
   * @return A {@link ListenableFuture} that will be completed when the command is successfully processed
   *         by the target runnable.
   */
  ListenableFuture<Command> sendCommand(String runnableName, Command command);

  /**
   * Requests to forcefully kill a running service.
   */
  void kill();

  /**
   * Registers a {@link Listener} to be {@linkplain Executor#execute executed} on the given
   * executor.  The listener will have the corresponding transition method called whenever the
   * service changes state. When added, the current state of the service will be reflected through
   * callback to the listener. Methods on the listener is guaranteed to be called no more than once.
   *
   * @param listener the listener to run when the service changes state is complete
   * @param executor the executor in which the the listeners callback methods will be run. For fast,
   *     lightweight listeners that would be safe to execute in any thread, consider
   *     {@link com.google.common.util.concurrent.MoreExecutors#sameThreadExecutor}.
   */
  @Override
  void addListener(Listener listener, Executor executor);
}
