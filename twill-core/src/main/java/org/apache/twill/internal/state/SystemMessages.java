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

import ch.qos.logback.classic.Logger;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.twill.api.Command;
import org.apache.twill.api.logging.LogEntry;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Collection of predefined system messages.
 */
public final class SystemMessages {

  public static final String SET_LOG_LEVEL = "setLogLevels";
  public static final String RESET_LOG_LEVEL = "resetLogLevels";
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

  /**
   * Helper method to get System {@link Message} for updating the log levels for all runnables.
   *
   * @param logLevels The log levels to be updated.
   * @return An instance of System {@link Message} to update the log levels.
   */
  public static Message updateLogLevels(Map<String, LogEntry.Level> logLevels) {
    return updateLogLevels(null, logLevels);
  }

  /**
   * Helper method to get System {@link Message} for updating the log levels for one or all runnables.
   *
   * @param runnableName The name of the runnable to update the log level, null if apply to all runnables.
   * @param logLevels The log levels to be updated.
   * @return An instance of System {@link Message} to update the log levels.
   */
  public static Message updateLogLevels(@Nullable String runnableName, Map<String, LogEntry.Level> logLevels) {
    Preconditions.checkNotNull(logLevels);
    Preconditions.checkArgument(!(logLevels.containsKey(Logger.ROOT_LOGGER_NAME)
                                  && logLevels.get(Logger.ROOT_LOGGER_NAME) == null));
    Map<String, String> options = convertLogEntryToString(logLevels);
    return new SimpleMessage(Message.Type.SYSTEM,
                             runnableName == null ? Message.Scope.ALL_RUNNABLE : Message.Scope.RUNNABLE,
                             runnableName, Command.Builder.of(SET_LOG_LEVEL).addOptions(options).build());
  }

  /**
   * Helper method to get System {@link Message} for resetting the log levels for all runnables.
   */
  public static Message resetLogLevels(Set<String> loggerNames) {
    return resetLogLevels(null, loggerNames);
  }

  /**
   * Helper method to get System {@link Message} for resetting log levels for one or all runnables.
   *
   * @param runnableName The name of the runnable to set the log level, null if apply to all runnables.
   * @return An instance of System {@link Message} to reset the log levels.
   */
   public static Message resetLogLevels(@Nullable String runnableName, Set<String> loggerNames) {
     return new SimpleMessage(Message.Type.SYSTEM,
                              runnableName == null ? Message.Scope.ALL_RUNNABLE : Message.Scope.RUNNABLE,
                              runnableName, Command.Builder.of(RESET_LOG_LEVEL)
                                .addOptions(Maps.uniqueIndex(loggerNames, Functions.toStringFunction())).build());
   }

  private static Map<String, String> convertLogEntryToString(Map<String, LogEntry.Level> logLevels) {
    return Maps.transformEntries(logLevels, new Maps.EntryTransformer<String, LogEntry.Level, String>() {
      @Override
      public String transformEntry(String loggerName, LogEntry.Level level) {
        return level == null ? null : level.name();
      }
    });
  }

  private SystemMessages() {
  }
}
