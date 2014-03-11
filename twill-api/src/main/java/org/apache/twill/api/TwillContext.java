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

import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.InetAddress;

/**
 * Represents the runtime context of a {@link TwillRunnable}.
 */
public interface TwillContext extends ServiceAnnouncer, DiscoveryServiceClient {

  /**
   * Returns the {@link RunId} of this running instance of {@link TwillRunnable}.
   */
  RunId getRunId();

  /**
   * Returns the {@link RunId} of this running application.
   */
  RunId getApplicationRunId();

  /**
   * Returns the number of running instances assigned for this {@link TwillRunnable}.
   */
  int getInstanceCount();

  /**
   * Returns the hostname that the runnable is running on.
   */
  InetAddress getHost();

  /**
   * Returns the runtime arguments that are passed to the {@link TwillRunnable}.
   */
  String[] getArguments();

  /**
   * Returns the runtime arguments that are passed to the {@link TwillApplication}.
   */
  String[] getApplicationArguments();

  /**
   * Returns the {@link TwillRunnableSpecification} that was created by {@link TwillRunnable#configure()}.
   */
  TwillRunnableSpecification getSpecification();

  /**
   * Returns an integer id from 0 to (instanceCount - 1).
   */
  int getInstanceId();

  /**
   * Returns the number of virtual cores the runnable is allowed to use.
   */
  int getVirtualCores();

  /**
   * Returns the amount of memory in MB the runnable is allowed to use.
   */
  int getMaxMemoryMB();

  /**
   * Discover service with the given name that is announced within the same {@link TwillApplication}.
   *
   * @param name Name of the service
   * @return A {@link ServiceDiscovered} object representing the result.
   */
  @Override
  ServiceDiscovered discover(String name);
}
