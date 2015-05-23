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
package org.apache.twill.internal;

import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.Hosts;
import org.apache.twill.api.Racks;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Straightforward implementation of {@link org.apache.twill.api.TwillSpecification}.
 */
public final class DefaultTwillSpecification implements TwillSpecification {

  private final String name;
  private final Map<String, RuntimeSpecification> runnables;
  private final List<Order> orders;
  private final List<PlacementPolicy> placementPolicies;
  private final EventHandlerSpecification eventHandler;

  public DefaultTwillSpecification(String name, Map<String, RuntimeSpecification> runnables,
                                   List<Order> orders, List<PlacementPolicy> placementPolicies,
                                   EventHandlerSpecification eventHandler) {
    this.name = name;
    this.runnables = Collections.unmodifiableMap(new HashMap<String, RuntimeSpecification>(runnables));
    this.orders = Collections.unmodifiableList(new ArrayList<Order>(orders));
    this.placementPolicies = placementPolicies;
    this.eventHandler = eventHandler;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return runnables;
  }

  @Override
  public List<Order> getOrders() {
    return orders;
  }

  @Override
  public List<PlacementPolicy> getPlacementPolicies() {
    return placementPolicies;
  }

  @Nullable
  @Override
  public EventHandlerSpecification getEventHandler() {
    return eventHandler;
  }

  /**
   * Straightforward implementation of {@link Order}.
   */
  public static final class DefaultOrder implements Order {

    private final Set<String> names;
    private final Type type;

    public DefaultOrder(Set<String> names, Type type) {
      this.names = Collections.unmodifiableSet(new HashSet<String>(names));
      this.type = type;
    }

    @Override
    public Set<String> getNames() {
      return names;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public String toString() {
      return "DefaultOrder{" +
        "names=" + names +
        ", type=" + type +
        '}';
    }
  }

  /**
   * Straightforward implementation of {@link org.apache.twill.api.TwillSpecification.PlacementPolicy}.
   */
  public static final class DefaultPlacementPolicy implements PlacementPolicy {

    private final Set<String> names;
    private final Type type;
    private final Hosts hosts;
    private final Racks racks;

    public DefaultPlacementPolicy(Set<String> names, Type type, Hosts hosts, Racks racks) {
      this.names = Collections.unmodifiableSet(new HashSet<String>(names));
      this.type = type;
      this.hosts = hosts;
      this.racks = racks;
    }

    /**
     * @return Set of {@link org.apache.twill.api.TwillRunnable} names that belongs to this placement policy.
     */
    @Override
    public Set<String> getNames() {
      return names;
    }

    /**
     * @return {@link org.apache.twill.api.TwillSpecification.PlacementPolicy.Type Type} of this placement policy.
     */
    @Override
    public Type getType() {
      return type;
    }

    /**
     * @return Set of hosts associated with this placement policy.
     */
    @Override
    public Set<String> getHosts() {
      if (this.hosts == null) {
        return Collections.emptySet();
      }
      return this.hosts.get();
    }

    /**
     * @return Set of racks associated with this placement policy.
     */
    @Override
    public Set<String> getRacks() {
      if (this.racks == null) {
        return Collections.emptySet();
      }
      return this.racks.get();
    }

    @Override
    public String toString() {
      return "DefaultPlacementPolicy{" +
        "hosts=" + hosts +
        ", names=" + names +
        ", type=" + type +
        ", racks=" + racks +
        '}';
    }
  }
}
