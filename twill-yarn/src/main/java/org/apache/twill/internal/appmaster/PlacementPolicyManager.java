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

import org.apache.twill.api.TwillSpecification;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class provides helper functions for operating on a set of Placement Policies.
 */
final class PlacementPolicyManager {

  private final Map<TwillSpecification.PlacementPolicy.Type, Set<String>> policyTypeToRunnables;
  private final Map<String, TwillSpecification.PlacementPolicy> runnablePolicies;

  PlacementPolicyManager(List<TwillSpecification.PlacementPolicy> policies) {
    this.policyTypeToRunnables = new EnumMap<>(TwillSpecification.PlacementPolicy.Type.class);
    this.runnablePolicies = new HashMap<>();

    for (TwillSpecification.PlacementPolicy policy : policies) {
      policyTypeToRunnables.put(policy.getType(), policy.getNames());
      for (String runnable : policy.getNames()) {
        runnablePolicies.put(runnable, policy);
      }
    }
  }

  /**
   * Returns all runnables which belong to DISTRIBUTED placement policies.
   */
  Set<String> getDistributedRunnables() {
    Set<String> runnables = policyTypeToRunnables.get(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED);
    return runnables == null ? Collections.<String>emptySet() : runnables;
  }

  /**
   * Get the placement policy of the runnable.
   * Returns null if runnable does not belong to a placement policy.
   * @param runnableName Name of runnable.
   * @return Placement policy of the runnable.
   */
  @Nullable
  TwillSpecification.PlacementPolicy getPlacementPolicy(String runnableName) {
    return runnablePolicies.get(runnableName);
  }
}
