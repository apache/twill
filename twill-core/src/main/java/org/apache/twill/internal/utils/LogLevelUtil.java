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
package org.apache.twill.internal.utils;

import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.internal.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class LogLevelUtil {

  public static Map<String, String> convertLogLevelArgs(Map<String, LogEntry.Level> logLevelArguments) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, LogEntry.Level> entry : logLevelArguments.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toString());
    }
    return result;
  }

  public static Map<String, String> getLogLevelForRunnable(String runnableName,
                                                           Map<String, Map<String, LogEntry.Level>> logLevelArguments) {
    Map<String, LogEntry.Level> logAppArguments = logLevelArguments.get(Constants.SystemMessages.LOG_ALL_RUNNABLES);
    Map<String, LogEntry.Level> logRunnableArguments = logLevelArguments.get(runnableName);
    Map<String, LogEntry.Level> result = logAppArguments;
    if (logAppArguments == null && logRunnableArguments == null) {
      return new HashMap<>();
    } else if (logAppArguments == null) {
      result = logRunnableArguments;
    } else if (logRunnableArguments != null) {
      result.putAll(logRunnableArguments);
    }
    return convertLogLevelArgs(result);
  }

  private LogLevelUtil() {
  }
}
