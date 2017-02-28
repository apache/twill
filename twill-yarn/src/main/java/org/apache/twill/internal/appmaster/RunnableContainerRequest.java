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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Data structure for holding set of runnable specifications based on resource capability.
 */
final class RunnableContainerRequest {
  private final TwillSpecification.Order.Type orderType;
  private final Iterator<Map.Entry<AllocationSpecification, Collection<RuntimeSpecification>>> requests;
  private volatile boolean isReadyToBeProvisioned;

  RunnableContainerRequest(TwillSpecification.Order.Type orderType,
                           Map<AllocationSpecification, Collection<RuntimeSpecification>> requests) {
    this(orderType, requests, true);
  }

  RunnableContainerRequest(TwillSpecification.Order.Type orderType,
                           Map<AllocationSpecification, Collection<RuntimeSpecification>> requests,
                           boolean isReadyToBeProvisioned) {
    this.orderType = orderType;
    this.requests = requests.entrySet().iterator();
    this.isReadyToBeProvisioned = isReadyToBeProvisioned;
  }

  TwillSpecification.Order.Type getOrderType() {
    return orderType;
  }

  boolean isReadyToBeProvisioned() {
    return isReadyToBeProvisioned;
  }

  void setReadyToBeProvisioned() {
    this.isReadyToBeProvisioned = true;
  }

  /**
   * Remove a resource request and return it.
   * @return The {@link Resource} and {@link Collection} of {@link RuntimeSpecification} or
   *         {@code null} if there is no more request.
   */
  Map.Entry<AllocationSpecification, ? extends Collection<RuntimeSpecification>> takeRequest() {
    Map.Entry<AllocationSpecification, Collection<RuntimeSpecification>> next = Iterators.getNext(requests, null);
    return next == null ? null : Maps.immutableEntry(next.getKey(), ImmutableList.copyOf(next.getValue()));
  }
}
