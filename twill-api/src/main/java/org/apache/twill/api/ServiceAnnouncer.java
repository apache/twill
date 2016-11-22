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

import org.apache.twill.common.Cancellable;

/**
 * This interface provides a way to announce the availability of a service.
 */
public interface ServiceAnnouncer {

  /**
   * Registers an endpoint that could be discovered by external party.
   * @param serviceName Name of the endpoint
   * @param port Port of the endpoint
   */
  Cancellable announce(String serviceName, int port);

  /**
   * Registers an endpoint that could be discovered by external party with a payload.
   * @param serviceName Name of the endpoint
   * @param port Port of the endpoint
   * @param payload byte array payload
   */
  Cancellable announce(String serviceName, int port, byte[] payload);
}
