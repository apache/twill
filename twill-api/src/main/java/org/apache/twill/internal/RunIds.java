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

import org.apache.twill.api.RunId;

import java.util.UUID;

/**
 * Factory class for creating instance of {@link org.apache.twill.api.RunId}.
 */
public final class RunIds {

  public static RunId generate() {
    return new RunIdImpl(UUID.randomUUID().toString());
  }

  public static RunId fromString(String str) {
    return new RunIdImpl(str);
  }

  private RunIds() {
  }

  private static final class RunIdImpl implements RunId {

    final String id;

    private RunIdImpl(String id) {
      if (id == null) {
        throw new IllegalArgumentException("RunId cannot be null.");
      }
      this.id = id;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String toString() {
      return getId();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || !(other instanceof RunId)) {
        return false;
      }
      return id.equals(((RunId) other).getId());
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }
}
