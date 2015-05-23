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

import org.apache.twill.api.TwillRunnableSpecification;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Straightforward implementation of {@link org.apache.twill.api.TwillRunnableSpecification}.
 */
public final class DefaultTwillRunnableSpecification implements TwillRunnableSpecification {

  private final String className;
  private final String name;
  private final Map<String, String> arguments;

  public DefaultTwillRunnableSpecification(String className, String name, Map<String, String> arguments) {
    this.className = className;
    this.name = name;
    this.arguments = Collections.unmodifiableMap(new HashMap<String, String>(arguments));
  }

  public DefaultTwillRunnableSpecification(String className, TwillRunnableSpecification other) {
    this.className = className;
    this.name = other.getName();
    this.arguments = Collections.unmodifiableMap(new HashMap<String, String>(other.getConfigs()));
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Map<String, String> getConfigs() {
    return arguments;
  }
}
