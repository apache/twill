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

/**
 * The {@link TwillRunnable} interface should be implemented by any
 * class whose instances are intended to be executed in a Twill cluster.
 */
public interface TwillRunnable extends Runnable {

  /**
   * Called at submission time. Executed on the client side.
   * @return A {@link TwillRunnableSpecification} built by {@link TwillRunnableSpecification.Builder}.
   */
  TwillRunnableSpecification configure();

  /**
   * Called when the container process starts. Executed in container machine. If any exception is thrown from this
   * method, this runnable won't get retry.
   *
   * @param context Contains information about the runtime context.
   */
  void initialize(TwillContext context);

  /**
   * Called when a command is received. A normal return denotes the command has been processed successfully, otherwise
   * {@link Exception} should be thrown.
   * @param command Contains details of the command.
   * @throws Exception
   */
  void handleCommand(Command command) throws Exception;

  /**
   * Requests to stop the running service.
   */
  void stop();

  /**
   * Called when the {@link TwillRunnable#run()} completed. Useful for doing
   * resource cleanup. This method would only get called if the call to {@link #initialize(TwillContext)} was
   * successful.
   */
  void destroy();
}
