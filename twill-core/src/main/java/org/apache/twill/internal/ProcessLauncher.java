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

import org.apache.twill.api.LocalFile;

import java.util.Map;

/**
 * Class for launching container process.
 *
 * @param <T> Type of the object that contains information about the container that the process is going to launch.
 */
public interface ProcessLauncher<T> {

  /**
   * Returns information about the container that this launch would launch process in.
   */
  T getContainerInfo();

  /**
   * Returns a preparer with the given default set of environments, resources and credentials.
   */
  <C> PrepareLaunchContext prepareLaunch(Map<String, String> environments,
                                         Iterable<LocalFile> resources, C credentials);

  /**
   * For setting up the launcher.
   */
  interface PrepareLaunchContext {

    ResourcesAdder withResources();

    AfterResources noResources();

    interface ResourcesAdder {
      MoreResources add(LocalFile localFile);
    }

    interface AfterResources {
      EnvironmentAdder withEnvironment();

      AfterEnvironment noEnvironment();
    }

    interface EnvironmentAdder {
      <V> MoreEnvironment add(String key, V value);
    }

    interface MoreEnvironment extends EnvironmentAdder, AfterEnvironment {
    }

    interface AfterEnvironment {
      CommandAdder withCommands();
    }

    interface MoreResources extends ResourcesAdder, AfterResources { }

    interface CommandAdder {
      StdOutSetter add(String cmd, String...args);
    }

    interface StdOutSetter {
      StdErrSetter redirectOutput(String stdout);

      StdErrSetter noOutput();
    }

    interface StdErrSetter {
      MoreCommand redirectError(String stderr);

      MoreCommand noError();
    }

    interface MoreCommand extends CommandAdder {
      <R> ProcessController<R> launch();
    }
  }
}
