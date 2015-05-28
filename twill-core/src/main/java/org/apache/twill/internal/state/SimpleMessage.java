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
package org.apache.twill.internal.state;

import com.google.common.base.Objects;
import org.apache.twill.api.Command;

/**
 * Implementation of {@code Message} interface to pass information about {@code Command} to execute.
 */
final class SimpleMessage implements Message {

  private final Type type;
  private final Scope scope;
  private final String runnableName;
  private final Command command;

  SimpleMessage(Type type, Scope scope, String runnableName, Command command) {
    this.type = type;
    this.scope = scope;
    this.runnableName = runnableName;
    this.command = command;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public Scope getScope() {
    return scope;
  }

  @Override
  public String getRunnableName() {
    return runnableName;
  }

  @Override
  public Command getCommand() {
    return command;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(Message.class)
      .add("type", type)
      .add("scope", scope)
      .add("runnable", runnableName)
      .add("command", command)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, scope, runnableName, command);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Message)) {
      return false;
    }
    Message other = (Message) obj;
    return type == other.getType()
      && scope == other.getScope()
      && Objects.equal(runnableName, other.getRunnableName())
      && Objects.equal(command, other.getCommand());
  }
}
