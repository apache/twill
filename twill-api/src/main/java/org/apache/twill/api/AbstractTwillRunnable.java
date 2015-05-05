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
package org.apache.twill.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This abstract class provides default implementation of the {@link TwillRunnable}.
 */
public abstract class AbstractTwillRunnable implements TwillRunnable {

  private Map<String, String> args;
  private TwillContext context;

  protected AbstractTwillRunnable() {
    this(Collections.<String, String>emptyMap());
  }

  protected AbstractTwillRunnable(Map<String, String> args) {
    this.args = Collections.unmodifiableMap(new HashMap<String, String>(args));
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(getClass().getSimpleName())
      .withConfigs(args)
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    this.context = context;
    this.args = context.getSpecification().getConfigs();
  }

  @Override
  public void handleCommand(org.apache.twill.api.Command command) throws Exception {
    // No-op by default. Left for children class to override.
  }

  @Override
  public void stop() {
    // No-op by default. Left for children class to override.
  }

  @Override
  public void destroy() {
    // No-op by default. Left for children class to override.
  }

  protected Map<String, String> getArguments() {
    return args;
  }

  protected String getArgument(String key) {
    return args.get(key);
  }

  protected TwillContext getContext() {
    return context;
  }
}
