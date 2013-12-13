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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * Wrapper for {@link Service.Listener} to have callback executed on a given {@link Executor}.
 * Also make sure each method is called at most once.
 */
final class ListenerExecutor implements Service.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(ListenerExecutor.class);

  private final Service.Listener delegate;
  private final Executor executor;
  private final ConcurrentMap<Service.State, Boolean> callStates = Maps.newConcurrentMap();

  ListenerExecutor(Service.Listener delegate, Executor executor) {
    this.delegate = delegate;
    this.executor = executor;
  }

  @Override
  public void starting() {
    if (hasCalled(Service.State.STARTING)) {
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          delegate.starting();
        } catch (Throwable t) {
          LOG.warn("Exception thrown from listener", t);
        }
      }
    });
  }

  @Override
  public void running() {
    if (hasCalled(Service.State.RUNNING)) {
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          delegate.running();
        } catch (Throwable t) {
          LOG.warn("Exception thrown from listener", t);
        }
      }
    });
  }

  @Override
  public void stopping(final Service.State from) {
    if (hasCalled(Service.State.STOPPING)) {
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          delegate.stopping(from);
        } catch (Throwable t) {
          LOG.warn("Exception thrown from listener", t);
        }
      }
    });
  }

  @Override
  public void terminated(final Service.State from) {
    if (hasCalled(Service.State.TERMINATED)) {
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          delegate.terminated(from);
        } catch (Throwable t) {
          LOG.warn("Exception thrown from listener", t);
        }
      }
    });
  }

  @Override
  public void failed(final Service.State from, final Throwable failure) {
    // Both failed and terminate are using the same state for checking as only either one could be called.
    if (hasCalled(Service.State.TERMINATED)) {
      return;
    }
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          delegate.failed(from, failure);
        } catch (Throwable t) {
          LOG.warn("Exception thrown from listener", t);
        }
      }
    });
  }

  private boolean hasCalled(Service.State state) {
    return callStates.putIfAbsent(state, true) != null;
  }
}
