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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   * @return The {@link EventHandlerSpecification} for the {@link EventHandler} to be used for this application,
   *         or {@code null} if no event handler has been provided.
   */
  @Nullable
  EventHandlerSpecification getEventHandler();

  /**
   * Builder for constructing instance of {@link TwillSpecification}.
   */
  static final class Builder {

    private String name;
    private Map<String, RuntimeSpecification> runnables = Maps.newHashMap();
    private List<Order> orders = Lists.newArrayList();
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
        return new RuntimeSpecificationAdder(new Function<Collection<LocalFile>, RunnableSetter>() {
          @Override
          public RunnableSetter apply(Collection<LocalFile> files) {
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
    }

    /**
     * For setting runtime specific settings.
     */
    public final class RuntimeSpecificationAdder {

      private final Function<Collection<LocalFile>, RunnableSetter> completer;

      RuntimeSpecificationAdder(Function<Collection<LocalFile>, RunnableSetter> completer) {
        this.completer = completer;
      }

      public LocalFileAdder withLocalFiles() {
        return new MoreFile(completer);
      }

      public RunnableSetter noLocalFiles() {
        return completer.apply(ImmutableList.<LocalFile>of());
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

      private final Function<Collection<LocalFile>, RunnableSetter> completer;
      private final List<LocalFile> files = Lists.newArrayList();

      public MoreFile(Function<Collection<LocalFile>, RunnableSetter> completer) {
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
        return completer.apply(files);
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
        Set<String> runnableNames = Sets.newHashSet(runnables.keySet());
        for (Order order : orders) {
          runnableNames.removeAll(order.getNames());
        }

        // For all unordered runnables, add it to the end of orders list
        orders.add(new DefaultTwillSpecification.DefaultOrder(runnableNames, Order.Type.STARTED));

        return new DefaultTwillSpecification(name, runnables, orders, eventHandler);
      }

      private void addOrder(final Order.Type type, String name, String...names) {
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        Preconditions.checkArgument(runnables.containsKey(name), "Runnable not exists.");

        Set<String> runnableNames = Sets.newHashSet(name);
        for (String runnableName : names) {
          Preconditions.checkArgument(name != null, "Name cannot be null.");
          Preconditions.checkArgument(runnables.containsKey(name), "Runnable not exists.");
          runnableNames.add(runnableName);
        }

        orders.add(new DefaultTwillSpecification.DefaultOrder(runnableNames, type));
      }
    }

    private Builder() {}
  }
}
