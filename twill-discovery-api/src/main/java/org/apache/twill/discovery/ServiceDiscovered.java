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

import java.util.concurrent.Executor;

/**
 * Represents the result of service discovery. It extends from
 * {@link Iterable} that gives set of latest {@link Discoverable} every time when
 * {@link #iterator()} is invoked.
 */
public interface ServiceDiscovered extends Iterable<Discoverable> {

  /**
   * Represents a callback for watching changes in the discovery list.
   *
   * @see #watchChanges(ServiceDiscovered.ChangeListener, java.util.concurrent.Executor)
   */
  interface ChangeListener {

    /**
     * This method will be invoked when the discovery list changed.
     *
     * @param serviceDiscovered The {@link ServiceDiscovered} that this listener is attached to.
     */
    void onChange(ServiceDiscovered serviceDiscovered);
  }

  /**
   * Returns the name of the service being discovered.
   *
   * @return Name of the service.
   */
  String getName();

  /**
   * Registers a {@link ChangeListener} to watch for changes in the discovery list.
   * The {@link ChangeListener#onChange(ServiceDiscovered)} method will be triggered when start watching,
   * and on every subsequent changes in the discovery list.
   *
   * @param listener A {@link ChangeListener} to watch for changes.
   * @param executor A {@link Executor} for issuing call to the given listener.
   * @return A {@link Cancellable} to cancel the watch.
   */
  Cancellable watchChanges(ChangeListener listener, Executor executor);

  /**
   * Checks if the given discoverable contains in the current discovery list.
   *
   * @param discoverable The {@link Discoverable} to check for.
   * @return {@code true} if it exists, {@code false} otherwise.
   */
  boolean contains(Discoverable discoverable);
}
