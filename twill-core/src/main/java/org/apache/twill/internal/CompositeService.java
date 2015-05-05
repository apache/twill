/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * A {@link Service} that starts/stops list of services in order.
 */
public final class CompositeService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(CompositeService.class);
  private final Deque<Service> services;

  public CompositeService(Service...services) {
    this(ImmutableList.copyOf(services));
  }

  public CompositeService(Iterable<? extends Service> services) {
    this.services = new ArrayDeque<Service>();
    Iterables.addAll(this.services, services);
  }

  @Override
  protected void startUp() throws Exception {
    Throwable failureCause = null;

    for (Service service : services) {
      try {
        service.startAndWait();
      } catch (UncheckedExecutionException e) {
        failureCause = e.getCause();
        break;
      }
    }

    if (failureCause != null) {
      // Stop all running services and then throw the failure exception
      try {
        stopAll();
      } catch (Throwable t) {
        // Ignore the stop error. Just log.
        LOG.warn("Failed when stopping all services on start failure", t);
      }

      Throwables.propagateIfPossible(failureCause, Exception.class);
      throw new RuntimeException(failureCause);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    stopAll();
  }

  private void stopAll() throws Exception {
    Throwable failureCause = null;

    // Stop services in reverse order.
    Iterator<Service> itor = services.descendingIterator();
    while (itor.hasNext()) {
      Service service = itor.next();
      try {
        if (service.isRunning() || service.state() == State.STARTING) {
          service.stopAndWait();
        }
      } catch (UncheckedExecutionException e) {
        // Just catch as we want all services stopped
        if (failureCause == null) {
          failureCause = e.getCause();
        } else {
          // Log for sub-sequence service shutdown error, as only the first failure cause will be thrown.
          LOG.warn("Failed to stop service {}", service, e);
        }
      }
    }

    if (failureCause != null) {
      Throwables.propagateIfPossible(failureCause, Exception.class);
      throw new RuntimeException(failureCause);
    }
  }
}
