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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

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
  private final EventHandlerSpecification eventHandler;

  public DefaultTwillSpecification(String name, Map<String, RuntimeSpecification> runnables,
                                   List<Order> orders, EventHandlerSpecification eventHandler) {
    this.name = name;
    this.runnables = ImmutableMap.copyOf(runnables);
    this.orders = ImmutableList.copyOf(orders);
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

    public DefaultOrder(Iterable<String> names, Type type) {
      this.names = ImmutableSet.copyOf(names);
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
      return Objects.toStringHelper(this)
        .add("names", names)
        .add("type", type)
        .toString();
    }
  }
}
