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

import com.google.common.collect.Sets;
import org.apache.twill.api.TwillSpecification;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This class provides helper functions for operating on a set of Placement Policies.
 */
public class PlacementPolicyManager {
  List<TwillSpecification.PlacementPolicy> placementPolicies;

  public PlacementPolicyManager(List<TwillSpecification.PlacementPolicy> placementPolicies) {
    this.placementPolicies = placementPolicies;
  }

  /**
   * Given a set of runnables, get all runnables which belong to DISTRIBUTED placement policies.
   * @param givenRunnables Set of runnables.
   * @return Subset of runnables, which belong to DISTRIBUTED placement policies.
   */
  public Set<String> getDistributedRunnables(Set<String> givenRunnables) {
    Set<String> distributedRunnables = getAllDistributedRunnables();
    distributedRunnables.retainAll(givenRunnables);
    return distributedRunnables;
  }

  /**
   * Given a runnable, get the type of placement policy. Returns DEFAULT if no placement policy is specified.
   * @param runnableName Name of runnable.
   * @return Placement policy type of the runnable.
   */
  public TwillSpecification.PlacementPolicy.Type getPlacementPolicyType(String runnableName) {
    for (TwillSpecification.PlacementPolicy placementPolicy : placementPolicies) {
      if (placementPolicy.getNames().contains(runnableName)) {
        return placementPolicy.getType();
      }
    }
    return TwillSpecification.PlacementPolicy.Type.DEFAULT;
  }

  /**
   * Get all runnables which belong to the same Placement policy as the given runnable.
   * @param runnableName Name of runnable.
   * @return Set of runnables, with same placement policy.
   */
  public Set<String> getFellowRunnables(String runnableName) {
    for (TwillSpecification.PlacementPolicy placementPolicy : placementPolicies) {
      if (placementPolicy.getNames().contains(runnableName)) {
        return placementPolicy.getNames();
      }
    }
    return Collections.emptySet();
  }

  /**
   * Get the placement policy of the runnable.
   * Returns null if runnable does not belong to a placement policy.
   * @param runnableName Name of runnable.
   * @return Placement policy of the runnable.
   */
  public TwillSpecification.PlacementPolicy getPlacementPolicy(String runnableName) {
    for (TwillSpecification.PlacementPolicy placementPolicy : placementPolicies) {
      if (placementPolicy.getNames().contains(runnableName)) {
        return placementPolicy;
      }
    }
    return null;
  }

  /**
   * Gets all runnables which belong to DISTRIBUTED placement policies.
   */
  private Set<String> getAllDistributedRunnables() {
    Set<String> distributedRunnables = Sets.newHashSet();
    for (TwillSpecification.PlacementPolicy placementPolicy : placementPolicies) {
      if (placementPolicy.getType().equals(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED)) {
        distributedRunnables.addAll(placementPolicy.getNames());
      }
    }
    return  distributedRunnables;
  }
}
