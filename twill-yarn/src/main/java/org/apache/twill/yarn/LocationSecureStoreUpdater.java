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
package org.apache.twill.yarn;

import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.yarn.YarnUtils;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;

/**
 * Package private class for updating location related secure store.
 */
final class LocationSecureStoreUpdater implements SecureStoreUpdater {

  private final Configuration configuration;
  private final LocationFactory locationFactory;

  LocationSecureStoreUpdater(Configuration configuration, LocationFactory locationFactory) {
    this.configuration = configuration;
    this.locationFactory = locationFactory;
  }

  @Override
  public SecureStore update(String application, RunId runId) {
    try {
      Credentials credentials = new Credentials();
      YarnUtils.addDelegationTokens(configuration, locationFactory, credentials);
      return YarnSecureStore.create(credentials);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
