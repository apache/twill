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
package org.apache.twill.discovery;

import org.apache.twill.common.Cancellable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple in memory implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 */
public class InMemoryDiscoveryService implements DiscoveryService, DiscoveryServiceClient {

  private final Multimap<String, Discoverable> services = HashMultimap.create();
  private final Lock lock = new ReentrantLock();

  @Override
  public Cancellable register(final Discoverable discoverable) {
    lock.lock();
    try {
      final Discoverable wrapper = new DiscoverableWrapper(discoverable);
      services.put(wrapper.getName(), wrapper);
      return new Cancellable() {
        @Override
        public void cancel() {
          lock.lock();
          try {
            services.remove(wrapper.getName(), wrapper);
          } finally {
            lock.unlock();
          }
        }
      };
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Iterable<Discoverable> discover(final String name) {
    return new Iterable<Discoverable>() {
      @Override
      public Iterator<Discoverable> iterator() {
        lock.lock();
        try {
          return ImmutableList.copyOf(services.get(name)).iterator();
        } finally {
          lock.unlock();
        }
      }
    };
  }
}
