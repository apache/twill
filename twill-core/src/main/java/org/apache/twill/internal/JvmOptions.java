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

import java.util.Set;

/**
 * A class that encapsulates the different options for starting Java in containers.
 */
public final class JvmOptions {

  private final String extraOptions;
  private final DebugOptions debugOptions;

  public JvmOptions(String extraOptions, DebugOptions debugOptions) {
    this.extraOptions = extraOptions;
    this.debugOptions = debugOptions;
  }

  public String getExtraOptions() {
    return extraOptions;
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
      this.runnables = doDebug && runnables != null ? ImmutableSet.copyOf(runnables) : null;
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
  }
}
