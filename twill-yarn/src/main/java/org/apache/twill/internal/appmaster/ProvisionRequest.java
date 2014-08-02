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
package org.apache.twill.internal.appmaster;

import org.apache.twill.api.RuntimeSpecification;

/**
 * Package private class to help AM to track in progress container request.
 */
final class ProvisionRequest {
  private final RuntimeSpecification runtimeSpec;
  private final String requestId;
  private int requestCount;
  private final AllocationSpecification.Type type;

  ProvisionRequest(RuntimeSpecification runtimeSpec, String requestId, int requestCount) {
    this(runtimeSpec, requestId, requestCount, AllocationSpecification.Type.DEFAULT);
  }

  ProvisionRequest(RuntimeSpecification runtimeSpec, String requestId, int requestCount,
                   AllocationSpecification.Type type) {
    this.runtimeSpec = runtimeSpec;
    this.requestId = requestId;
    this.requestCount = requestCount;
    this.type = type;
  }

  RuntimeSpecification getRuntimeSpec() {
    return runtimeSpec;
  }

  String getRequestId() {
    return requestId;
  }

  /**
   * Called to notify a container has been provision for this request.
   * @return {@code true} if the requested container count has been provisioned.
   */
  boolean containerAcquired() {
    requestCount--;
    return requestCount == 0;
  }

  AllocationSpecification.Type getType() {
    return this.type;
  }
}
