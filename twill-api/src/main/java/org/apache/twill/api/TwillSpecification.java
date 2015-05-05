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

import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.DefaultRuntimeSpecification;
import org.apache.twill.internal.DefaultTwillRunnableSpecification;
import org.apache.twill.internal.DefaultTwillSpecification;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents specification of a {@link TwillApplication}.
 */
public interface TwillSpecification {

  /**
   * Defines execution order.
   */
  interface Order {

    enum Type {
      STARTED,
      COMPLETED
    }

    /**
     * @return Set of {@link TwillRunnable} name that belongs to this order.
     */
    Set<String> getNames();

    Type getType();
  }

  /**
   * Defines a container placement policy.
   */
  interface PlacementPolicy {

    /**
     * Lists different types of Placement Policies available.
     */
    enum Type {
      /**
       * Runnables should be scattered over different hosts.
       */
      DISTRIBUTED,
      /**
       * No specific placement policy.
       */
      DEFAULT
    }

    /**
     * @return Set of {@link org.apache.twill.api.TwillRunnable} names that belongs to this placement policy.
     */
    Set<String> getNames();

    /**
     * @return {@link org.apache.twill.api.TwillSpecification.PlacementPolicy.Type Type} of this placement policy.
     */
    Type getType();

    /**
     * @return Set of hosts associated with this placement policy.
     */
    Set<String> getHosts();

    /**
     * @return Set of racks associated with this placement policy.
     */
    Set<String> getRacks();
  }

  /**
   * @return Name of the application.
   */
  String getName();

  /**
   * @return A map from {@link TwillRunnable} name to {@link RuntimeSpecification}.
   */
  Map<String, RuntimeSpecification> getRunnables();

  /**
   * @return Returns a list of runnable names that should be executed in the given order.
   */
  List<Order> getOrders();

  /**
   * @return Returns all {@link org.apache.twill.api.TwillSpecification.PlacementPolicy} for this application.
   */
  List<PlacementPolicy> getPlacementPolicies();

  /**
   * @return The {@link EventHandlerSpecification} for the {@link EventHandler} to be used for this application,
   *         or {@code null} if no event handler has been provided.
   */
  @Nullable
  EventHandlerSpecification getEventHandler();

  /**
   * Builder for constructing instance of {@link TwillSpecification}.
   */
  final class Builder {

    private String name;
    private Map<String, RuntimeSpecification> runnables = new HashMap<String, RuntimeSpecification>();
    private List<Order> orders = new ArrayList<Order>();
    private List<PlacementPolicy> placementPolicies = new ArrayList<PlacementPolicy>();
    private EventHandlerSpecification eventHandler;

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {
      public AfterName setName(String name) {
        Builder.this.name = name;
        return new AfterName();
      }
    }

    public final class AfterName {
      public MoreRunnable withRunnable() {
        return new RunnableSetter();
      }
    }

    public interface MoreRunnable {
      RuntimeSpecificationAdder add(TwillRunnable runnable);

      RuntimeSpecificationAdder add(TwillRunnable runnable, ResourceSpecification resourceSpec);

      /**
       * Adds a {@link TwillRunnable} with {@link ResourceSpecification#BASIC} resource specification.
       * @param name Name of runnable
       * @param runnable {@link TwillRunnable} to be run
       * @return instance of {@link RuntimeSpecificationAdder}
       */
      RuntimeSpecificationAdder add(String name, TwillRunnable runnable);

      RuntimeSpecificationAdder add(String name, TwillRunnable runnable, ResourceSpecification resourceSpec);
    }

    public interface AfterRunnable {
      FirstOrder withOrder();

      AfterOrder anyOrder();

      PlacementPolicySetter withPlacementPolicy();
    }

    public final class RunnableSetter implements MoreRunnable, AfterRunnable {

      @Override
      public RuntimeSpecificationAdder add(TwillRunnable runnable) {
        return add(runnable.configure().getName(), runnable);
      }

      @Override
      public RuntimeSpecificationAdder add(TwillRunnable runnable, ResourceSpecification resourceSpec) {
        return add(runnable.configure().getName(), runnable, resourceSpec);
      }

      @Override
      public RuntimeSpecificationAdder add(String name, TwillRunnable runnable) {
        return add(name, runnable, ResourceSpecification.BASIC);
      }

      @Override
      public RuntimeSpecificationAdder add(String name, TwillRunnable runnable,
                                           final ResourceSpecification resourceSpec) {
        final TwillRunnableSpecification spec = new DefaultTwillRunnableSpecification(
                                            runnable.getClass().getName(), name, runnable.configure().getConfigs());
        return new RuntimeSpecificationAdder(new LocalFileCompleter() {
          @Override
          public RunnableSetter complete(Collection<LocalFile> files) {
            runnables.put(spec.getName(), new DefaultRuntimeSpecification(spec.getName(), spec, resourceSpec, files));
            return RunnableSetter.this;
          }
        });
      }

      @Override
      public FirstOrder withOrder() {
        return new OrderSetter();
      }

      @Override
      public AfterOrder anyOrder() {
        return new OrderSetter();
      }

      @Override
      public PlacementPolicySetter withPlacementPolicy() {
        return new PlacementPolicySetter();
      }
    }

    /**
     * Internal interface for completing addition of {@link LocalFile} to a runnable.
     */
    private interface LocalFileCompleter {
      RunnableSetter complete(Collection<LocalFile> files);
    }

    /**
     * For setting runtime specific settings.
     */
    public final class RuntimeSpecificationAdder {

      private final LocalFileCompleter completer;

      RuntimeSpecificationAdder(LocalFileCompleter completer) {
        this.completer = completer;
      }

      public LocalFileAdder withLocalFiles() {
        return new MoreFile(completer);
      }

      public RunnableSetter noLocalFiles() {
        return completer.complete(Collections.<LocalFile>emptyList());
      }
    }

    public interface LocalFileAdder {
      MoreFile add(String name, File file);

      MoreFile add(String name, URI uri);

      MoreFile add(String name, File file, boolean archive);

      MoreFile add(String name, URI uri, boolean archive);

      MoreFile add(String name, File file, String pattern);

      MoreFile add(String name, URI uri, String pattern);
    }

    public final class MoreFile implements LocalFileAdder {

      private final LocalFileCompleter completer;
      private final List<LocalFile> files = new ArrayList<LocalFile>();

      public MoreFile(LocalFileCompleter completer) {
        this.completer = completer;
      }

      @Override
      public MoreFile add(String name, File file) {
        return add(name, file, false);
      }

      @Override
      public MoreFile add(String name, URI uri) {
        return add(name, uri, false);
      }

      @Override
      public MoreFile add(String name, File file, boolean archive) {
        return add(name, file.toURI(), archive);
      }

      @Override
      public MoreFile add(String name, URI uri, boolean archive) {
        files.add(new DefaultLocalFile(name, uri, -1, -1, archive, null));
        return this;
      }

      @Override
      public MoreFile add(String name, File file, String pattern) {
        return add(name, file.toURI(), pattern);
      }

      @Override
      public MoreFile add(String name, URI uri, String pattern) {
        files.add(new DefaultLocalFile(name, uri, -1, -1, true, pattern));
        return this;
      }

      public RunnableSetter apply() {
        return completer.complete(files);
      }
    }

    /**
     * Interface to add placement policies to the application.
     */
    public interface MorePlacementPolicies {

      /**
       * Specify hosts for a list of runnables.
       * @param hosts {@link org.apache.twill.api.Hosts} specifying a set of hosts.
       * @param runnableName a runnable name.
       * @param runnableNames a list of runnable names.
       * @return A reference to either add more placement policies or skip to defining execution order.
       */
      PlacementPolicySetter add(Hosts hosts, String runnableName, String... runnableNames);

      /**
       * Specify racks for a list of runnables.
       * @param racks {@link org.apache.twill.api.Racks} specifying a set of racks.
       * @param runnableName a runnable name.
       * @param runnableNames a list of runnable names.
       * @return A reference to either add more placement policies or skip to defining execution order.
       */
      PlacementPolicySetter add(Racks racks, String runnableName, String... runnableNames);

      /**
       * Specify hosts and racks for a list of runnables.
       * @param hosts {@link org.apache.twill.api.Hosts} specifying a set of hosts.
       * @param racks {@link org.apache.twill.api.Racks} specifying a set of racks.
       * @param runnableName a runnable name.
       * @param runnableNames a list of runnable names.
       * @return A reference to either add more placement policies or skip to defining execution order.
       */
      PlacementPolicySetter add(Hosts hosts, Racks racks, String runnableName, String... runnableNames);

      /**
       * Specify a placement policy for a list of runnables.
       * @param type {@link PlacementPolicy.Type} specifying a specific placement policy type.
       * @param runnableName a runnable name.
       * @param runnableNames a list of runnable names.
       * @return A reference to either add more placement policies or skip to defining execution order.
       */
      PlacementPolicySetter add(PlacementPolicy.Type type, String runnableName, String... runnableNames);
    }

    /**
     * Interface to define execution order after adding placement policies.
     */
    public interface AfterPlacementPolicy {
      /**
       * Start defining execution order.
       */
      FirstOrder withOrder();

      /**
       * No particular execution order is needed.
       */
      AfterOrder anyOrder();
    }

    public final class PlacementPolicySetter implements MorePlacementPolicies, AfterPlacementPolicy {

      @Override
      public PlacementPolicySetter add(Hosts hosts, String runnableName, String... runnableNames) {
        return addPlacementPolicy(PlacementPolicy.Type.DEFAULT, hosts, null, runnableName, runnableNames);
      }

      @Override
      public PlacementPolicySetter add(Racks racks, String runnableName, String... runnableNames) {
        return addPlacementPolicy(PlacementPolicy.Type.DEFAULT, null, racks, runnableName, runnableNames);
      }

      @Override
      public PlacementPolicySetter add(Hosts hosts, Racks racks, String runnableName, String... runnableNames) {
        return addPlacementPolicy(PlacementPolicy.Type.DEFAULT, hosts, racks, runnableName, runnableNames);
      }

      @Override
      public PlacementPolicySetter add(PlacementPolicy.Type type, String runnableName, String...runnableNames) {
        return addPlacementPolicy(type, null, null, runnableName, runnableNames);
      }

      private PlacementPolicySetter addPlacementPolicy(PlacementPolicy.Type type, Hosts hosts, Racks racks,
                                                       String runnableName, String...runnableNames) {
        checkArgument(runnableName != null, "Name cannot be null.");
        checkArgument(runnables.containsKey(runnableName), "Runnable not exists.");
        checkArgument(!contains(runnableName),
                      "Runnable (" + runnableName + ") cannot belong to more than one Placement Policy");
        Set<String> runnableNamesSet = new HashSet<String>(Collections.singleton(runnableName));
        for (String name : runnableNames) {
          checkArgument(name != null, "Name cannot be null.");
          checkArgument(runnables.containsKey(name), "Runnable not exists.");
          checkArgument(!contains(name),
                        "Runnable (" + name + ") cannot belong to more than one Placement Policy");
          runnableNamesSet.add(name);
        }
        placementPolicies.add(
          new DefaultTwillSpecification.DefaultPlacementPolicy(runnableNamesSet, type, hosts, racks));
        return this;
      }

      private boolean contains(String runnableName) {
        for (PlacementPolicy placementPolicy : placementPolicies) {
          if (placementPolicy.getNames().contains(runnableName)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public FirstOrder withOrder() {
        return new OrderSetter();
      }

      @Override
      public AfterOrder anyOrder() {
        return new OrderSetter();
      }
    }

    public interface FirstOrder {
      NextOrder begin(String name, String...names);
    }

    public interface NextOrder extends AfterOrder {
      NextOrder nextWhenStarted(String name, String...names);

      NextOrder nextWhenCompleted(String name, String...names);
    }

    public interface AfterOrder {
      AfterOrder withEventHandler(EventHandler handler);

      TwillSpecification build();
    }

    public final class OrderSetter implements FirstOrder, NextOrder {
      @Override
      public NextOrder begin(String name, String... names) {
        addOrder(Order.Type.STARTED, name, names);
        return this;
      }

      @Override
      public NextOrder nextWhenStarted(String name, String... names) {
        addOrder(Order.Type.STARTED, name, names);
        return this;
      }

      @Override
      public NextOrder nextWhenCompleted(String name, String... names) {
        addOrder(Order.Type.COMPLETED, name, names);
        return this;
      }

      @Override
      public AfterOrder withEventHandler(EventHandler handler) {
        eventHandler = handler.configure();
        return this;
      }

      @Override
      public TwillSpecification build() {
        // Set to track with runnable hasn't been assigned an order.
        Set<String> runnableNames = new HashSet<String>(runnables.keySet());
        for (Order order : orders) {
          runnableNames.removeAll(order.getNames());
        }

        // For all unordered runnables, add it to the end of orders list
        orders.add(new DefaultTwillSpecification.DefaultOrder(runnableNames, Order.Type.STARTED));
        return new DefaultTwillSpecification(name, runnables, orders, placementPolicies, eventHandler);
      }

      private void addOrder(final Order.Type type, String name, String...names) {
        checkArgument(name != null, "Name cannot be null.");
        checkArgument(runnables.containsKey(name), "Runnable not exists.");

        Set<String> runnableNames = new HashSet<String>(Collections.singleton(name));
        for (String runnableName : names) {
          checkArgument(name != null, "Name cannot be null.");
          checkArgument(runnables.containsKey(name), "Runnable not exists.");
          runnableNames.add(runnableName);
        }

        orders.add(new DefaultTwillSpecification.DefaultOrder(runnableNames, type));
      }
    }

    private void checkArgument(boolean condition, String msgFormat, Object...args) {
      if (!condition) {
        throw new IllegalArgumentException(String.format(msgFormat, args));
      }
    }

    private Builder() {}
  }
}
