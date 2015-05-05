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

import org.apache.twill.internal.DefaultTwillRunnableSpecification;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a specification of a {@link TwillRunnable}.
 */
public interface TwillRunnableSpecification {

  String getClassName();

  String getName();

  Map<String, String> getConfigs();

  /**
   * Builder for constructing {@link TwillRunnableSpecification}.
   */
  final class Builder {

    private String name;
    private Map<String, String> args;

    public static NameSetter with() {
      return new Builder().new NameSetter();
    }

    public final class NameSetter {
      public AfterName setName(String name) {
        Builder.this.name = name;
        return new AfterName();
      }
    }

    public final class AfterName {
      public AfterConfigs withConfigs(Map<String, String> args) {
        Builder.this.args = args;
        return new AfterConfigs();
      }

      public AfterConfigs noConfigs() {
        Builder.this.args = Collections.emptyMap();
        return new AfterConfigs();
      }
    }

    public final class AfterConfigs {
      public TwillRunnableSpecification build() {
        return new DefaultTwillRunnableSpecification(null, name, args);
      }
    }

    private Builder() {
    }
  }
}
