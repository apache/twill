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

import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.SystemMessages;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * A base implementation of {@link Service} handle secure token update.
 */
public abstract class AbstractTwillService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillService.class);

  protected final Location applicationLocation;

  protected volatile Credentials credentials;

  protected AbstractTwillService(Location applicationLocation) {
    this.applicationLocation = applicationLocation;
  }

  protected abstract Service getServiceDelegate();

  /**
   * Returns the location of the secure store, or {@code null} if either not running in secure mode or an error
   * occur when trying to acquire the location.
   */
  protected final Location getSecureStoreLocation() {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return null;
    }
    try {
      return applicationLocation.append(Constants.Files.CREDENTIALS);
    } catch (IOException e) {
      LOG.error("Failed to create secure store location.", e);
      return null;
    }
  }

  /**
   * Attempts to handle secure store update.
   *
   * @param message The message received
   * @return {@code true} if the message requests for secure store update, {@code false} otherwise.
   */
  protected final boolean handleSecureStoreUpdate(Message message) {
    if (!SystemMessages.SECURE_STORE_UPDATED.equals(message)) {
      return false;
    }

    // If not in secure mode, simply ignore the message.
    if (!UserGroupInformation.isSecurityEnabled()) {
      return true;
    }

    try {
      Credentials credentials = new Credentials();
      Location location = getSecureStoreLocation();
      DataInputStream input = new DataInputStream(new BufferedInputStream(location.getInputStream()));
      try {
        credentials.readTokenStorageStream(input);
      } finally {
        input.close();
      }

      UserGroupInformation.getCurrentUser().addCredentials(credentials);
      this.credentials = credentials;

      LOG.info("Secure store updated from {}.", location.toURI());

    } catch (Throwable t) {
      LOG.error("Failed to update secure store.", t);
    }

    return true;
  }

  @Override
  public final ListenableFuture<State> start() {
    return getServiceDelegate().start();
  }

  @Override
  public final State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public final boolean isRunning() {
    return getServiceDelegate().isRunning();
  }

  @Override
  public final State state() {
    return getServiceDelegate().state();
  }

  @Override
  public final ListenableFuture<State> stop() {
    return getServiceDelegate().stop();
  }

  @Override
  public final State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public final void addListener(Listener listener, Executor executor) {
    getServiceDelegate().addListener(listener, executor);
  }
}
