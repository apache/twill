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

import org.apache.twill.api.Command;

import com.google.common.base.Preconditions;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.Constants;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collection of predefined system messages.
 */
public final class SystemMessages {

  public static final Command STOP_COMMAND = Command.Builder.of("stop").build();
  public static final Message SECURE_STORE_UPDATED = new SimpleMessage(
    Message.Type.SYSTEM, Message.Scope.APPLICATION, null, Command.Builder.of("secureStoreUpdated").build());
  public static final String ALL_RUNNABLES = "ALL";

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

  /**
   * Helper method to get System {@link Message} for update instances for a runnable.
   *
   * @param updateCommand The {@link Command} to be added to the message.
   * @param runnableName The name of the runnable to be restarted.
   * @return An instance of System {@link Message} to restart runnable instances.
   */
  public static Message updateRunnableInstances(Command updateCommand, String runnableName) {
    Preconditions.checkNotNull(updateCommand);
    Preconditions.checkNotNull(runnableName);
    return new SimpleMessage(Message.Type.SYSTEM, Message.Scope.RUNNABLE, runnableName, updateCommand);
  }

  /**
   * Helper method to get System {@link Message} for restarting certain instances from certain runnables.
   *
   * @param updateCommand The {@link Command} to be added to the message.
   * @return An instance of System {@link Message} to restart runnables' instances.
   */
  public static Message updateRunnablesInstances(Command updateCommand) {
    Preconditions.checkNotNull(updateCommand);
    return new SimpleMessage(Message.Type.SYSTEM, Message.Scope.RUNNABLES, null, updateCommand);
  }

  public static Message setLogLevel(Map<String, LogEntry.Level> logLevelArguments) {
    return setLogLevel(ALL_RUNNABLES, logLevelArguments);
  }

  public static Message setLogLevel(String runnableName, Map<String, LogEntry.Level> logLevelArguments) {
    Preconditions.checkNotNull(runnableName);
    Preconditions.checkNotNull(logLevelArguments);
    Map<String, String> options = new LinkedHashMap<>();
    for (Map.Entry<String, LogEntry.Level> entry : logLevelArguments.entrySet()) {
      options.put(entry.getKey(), entry.getValue().name());
    }
    return new SimpleMessage(Message.Type.SYSTEM,
                             runnableName.equals(ALL_RUNNABLES) ? Message.Scope.ALL_RUNNABLE : Message.Scope.RUNNABLE,
                             runnableName, Command.Builder.of(Constants.SystemMessages.LOG_LEVEL)
                               .addOptions(options).build());
  }

  private SystemMessages() {
  }
}
