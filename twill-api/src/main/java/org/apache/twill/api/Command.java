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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents command objects.
 */
public interface Command {

  String getCommand();

  Map<String, String> getOptions();

  /**
   * Builder for creating {@link Command} object.
   */
  static final class Builder {

    private final String command;
    private final ImmutableMap.Builder<String, String> options = ImmutableMap.builder();

    public static Builder of(String command) {
      Preconditions.checkArgument(command != null, "Command cannot be null.");
      return new Builder(command);
    }

    public Builder addOption(String key, String value) {
      options.put(key, value);
      return this;
    }

    public Builder addOptions(Map<String, String> map) {
      options.putAll(map);
      return this;
    }

    public Command build() {
      return new SimpleCommand(command, options.build());
    }

    private Builder(String command) {
      this.command = command;
    }

    /**
     * Simple implementation of {@link org.apache.twill.api.Command}.
     */
    private static final class SimpleCommand implements Command {
      private final String command;
      private final Map<String, String> options;

      SimpleCommand(String command, Map<String, String> options) {
        this.command = command;
        this.options = options;
      }

      @Override
      public String getCommand() {
        return command;
      }

      @Override
      public Map<String, String> getOptions() {
        return options;
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(command, options);
      }

      @Override
      public String toString() {
        return Objects.toStringHelper(Command.class)
          .add("command", command)
          .add("options", options)
          .toString();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (!(obj instanceof Command)) {
          return false;
        }
        Command other = (Command) obj;
        return command.equals(other.getCommand()) && options.equals(other.getOptions());
      }
    }
  }
}
