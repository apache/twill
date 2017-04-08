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
package org.apache.twill.api.security;

import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.filesystem.Location;

import java.io.IOException;

/**
 * This class is responsible for renewing the secure store used by application.
 */
public abstract class SecureStoreRenewer {

  /**
   * Renew the secure store for an application run. It must uses the {@link SecureStoreWriter} to update the
   * {@link SecureStore}.
   *
   * @param application The name of the application.
   * @param runId The runId of the live application.
   * @param secureStoreWriter a {@link SecureStoreWriter} for writing out the new {@link SecureStore}.
   * @throws IOException if renewal failed
   */
  public abstract void renew(String application, RunId runId, SecureStoreWriter secureStoreWriter) throws IOException;
}
