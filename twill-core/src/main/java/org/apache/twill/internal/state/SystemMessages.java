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

import com.google.common.base.Preconditions;
import org.apache.twill.api.Command;

/**
 * Collection of predefined system messages.
 */
public final class SystemMessages {

  public static final Command STOP_COMMAND = Command.Builder.of("stop").build();
  public static final Message SECURE_STORE_UPDATED = new SimpleMessage(
    Message.Type.SYSTEM, Message.Scope.APPLICATION, null, Command.Builder.of("secureStoreUpdated").build());

  public static Message stopApplication() {
    return new SimpleMessage(Message.Type.SYSTEM, Message.Scope.APPLICATION, null, STOP_COMMAND);
  }

  public static Message stopRunnable(String runnableName) {
    return new SimpleMessage(Message.Type.SYSTEM, Message.Scope.RUNNABLE, runnableName, STOP_COMMAND);
  }

  public static Message setInstances(String runnableName, int instances) {
    Preconditions.checkArgument(instances > 0, "Instances should be > 0.");
    return new SimpleMessage(Message.Type.SYSTEM, Message.Scope.RUNNABLE, runnableName,
                             Command.Builder.of("instances").addOption("count", Integer.toString(instances)).build());
  }

  private SystemMessages() {
  }
}
