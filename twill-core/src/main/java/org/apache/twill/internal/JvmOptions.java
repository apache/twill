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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Booleans;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class that encapsulates the different options for starting Java in containers.
 */
public final class JvmOptions {

  private final String extraOptions;
  private final Map<String, String> runnableExtraOptions;
  private final DebugOptions debugOptions;

  public JvmOptions(String extraOptions, Map<String, String> runnableExtraOptions, DebugOptions debugOptions) {
    this.extraOptions = extraOptions;
    this.runnableExtraOptions = runnableExtraOptions;
    this.debugOptions = debugOptions;
  }

  /**
   * Returns the extra options for the application master.
   */
  public String getAMExtraOptions() {
    return extraOptions;
  }

  /**
   * Returns the extra options for the given runnable.
   */
  public String getRunnableExtraOptions(String runnableName) {
    return runnableExtraOptions.containsKey(runnableName) ? runnableExtraOptions.get(runnableName) : extraOptions;
  }

  public DebugOptions getDebugOptions() {
    return debugOptions;
  }

  /**
   * Represents the debugging options of the JVM.
   * <ul>
   *   <li>whether debugging is enabled</li>
   *   <li>whether VMs should suspend and wait for debugger</li>
   *   <li>optionally specify what runnables should be debuggable (default is all)</li>
   * </ul>
   */
  public static final class DebugOptions {
    private final boolean doDebug;
    private final boolean doSuspend;
    private final Set<String> runnables;

    public static final DebugOptions NO_DEBUG = new DebugOptions(false, false, null);

    public DebugOptions(boolean doDebug, boolean doSuspend, Iterable<String> runnables) {
      this.doDebug = doDebug;
      this.doSuspend = doDebug && doSuspend;
      this.runnables =
        doDebug && runnables != null && Iterables.size(runnables) > 0 ? ImmutableSet.copyOf(runnables) : null;
    }

    public boolean doDebug() {
      return doDebug;
    }

    public boolean doSuspend() {
      return doSuspend;
    }

    public Set<String> getRunnables() {
      return runnables;
    }

    /**
     * @return whether debugging is enabled for the given runnable name.
     */
    public boolean doDebug(String runnable) {
      return doDebug && (runnables == null || runnables.contains(runnable));
    }

    @Override
    public boolean equals(Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof DebugOptions)) {
        return false;
      }

      DebugOptions that = (DebugOptions) object;
      return (this.doDebug == that.doDebug()) && (this.doSuspend == this.doSuspend()) &&
        Objects.equals(this.runnables, that.getRunnables());
    }

    @Override
    public int hashCode() {
      int hash = 17;
      hash = 31 *  hash + Booleans.hashCode(doDebug);
      hash = 31 *  hash + Booleans.hashCode(doSuspend);
      hash = 31 *  hash + Objects.hashCode(runnables);
      return hash;
    }

    @Override
    public String toString() {
      return "{\"doDebug\":" + doDebug + ",\"doSuspend\":" + doSuspend + ",\"runnables\":" + (runnables != null ?
      runnables.toString() : "none") + "}";
    }

  }
}
