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

import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;

import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This interface exposes methods to set up the Twill runtime environment and start a Twill application.
 */
public interface TwillPreparer {

  /**
   * Overrides the default configuration with the given set of configurations.
   *
   * @param config set of configurations to override
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withConfiguration(Map<String, String> config);

  /**
   * Overrides the default configuration with the given set of configurations for the given runnable only.
   * This is useful to override configurations that affects runnables, such as
   * {@link Configs.Keys#JAVA_RESERVED_MEMORY_MB} and {@link Configs.Keys#HEAP_RESERVED_MIN_RATIO}.
   *
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param config set of configurations to override
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withConfiguration(String runnableName, Map<String, String> config);

  /**
   * Adds a {@link LogHandler} for receiving an application log.
   * @param handler The {@link LogHandler}.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer addLogHandler(LogHandler handler);

  /**
   * Sets the user name that runs the application. Default value is get from {@code "user.name"} by calling
   * {@link System#getProperty(String)}.
   * @param user User name
   * @return This {@link TwillPreparer}.
   *
   * @deprecated This method will be removed in future version.
   */
  @Deprecated
  TwillPreparer setUser(String user);

  /**
   * Sets the name of the scheduler queue to use.
   *
   * @param name Name of the scheduler queue
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer setSchedulerQueue(String name);

  /**
   * This methods sets the extra JVM options that will be passed to the java command line for every runnable
   * of the application started through this {@link org.apache.twill.api.TwillPreparer} instance.
   *
   * This is intended for advance usage. All options will be passed unchanged to the java command line. Invalid
   * options could cause application not able to start.
   *
   * @param options extra JVM options.
   */
  TwillPreparer setJVMOptions(String options);

  /**
   * This methods sets the extra JVM options that will be passed to the java command line for the given runnable
   * of the application started through this {@link org.apache.twill.api.TwillPreparer} instance.
   * The options set for the given runnable will be appended to any global options set through the
   * {@link #setJVMOptions(String)} or {@link #addJVMOptions(String)} method.
   *
   * This is intended for advance usage. All options will be passed unchanged to the java command line. Invalid
   * options could cause application not able to start.
   *
   * @param options extra JVM options.
   */
  TwillPreparer setJVMOptions(String runnableName, String options);

  /**
   * This methods adds extra JVM options that will be passed to the java command line for every runnable
   * of the application started through this {@link org.apache.twill.api.TwillPreparer} instance.
   *
   * This is intended for advance usage. All options will be passed unchanged to the java command line. Invalid
   * options could cause application not able to start.
   *
   * @param options extra JVM options.
   */
  TwillPreparer addJVMOptions(String options);

  /**
   * Enable debugging for runnables, without suspending the virtual machine to wait for the debugger.
   * This replaces any previous debug settings.
   * @param runnables the names of runnables to enable for debugging. If empty, it means all runnables.
   */
  TwillPreparer enableDebugging(String ... runnables);

  /**
   * Enable debugging for runnables. This replaces any previous debug settings.
   * @param doSuspend whether the virtual machines should be supended until the debugger connects. This
   *                  option allows to debug a container from the very beginning. Note that in that case,
   *                  the container cannot notify the controller of its debug port until the debugger is
   *                  attached - you must figure out where it is running using the YARN console or APIs.
   * @param runnables the names of runnables to enable for debugging. If empty, it means all runnables.
   */
  TwillPreparer enableDebugging(boolean doSuspend, String ... runnables);

  /**
   * Sets the list of arguments that will be passed to the application. The arguments can be retrieved
   * from {@link TwillContext#getApplicationArguments()}.
   *
   * @param args Array of arguments.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withApplicationArguments(String... args);

  /**
   * Sets the list of arguments that will be passed to the application. The arguments can be retrieved
   * from {@link TwillContext#getApplicationArguments()}.
   *
   * @param args Iterable of arguments.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withApplicationArguments(Iterable<String> args);

  /**
   * Sets the list of arguments that will be passed to the {@link TwillRunnable} identified by the given name.
   * The arguments can be retrieved from {@link TwillContext#getArguments()}.
   *
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param args Array of arguments.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withArguments(String runnableName, String...args);

  /**
   * Sets the list of arguments that will be passed to the {@link TwillRunnable} identified by the given name.
   * The arguments can be retrieved from {@link TwillContext#getArguments()}.
   *
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param args Iterable of arguments.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withArguments(String runnableName, Iterable<String> args);

  /**
   * Adds extra classes that the application is dependent on and is not traceable from the application itself.
   * @see #withDependencies(Iterable)
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withDependencies(Class<?>...classes);

  /**
   * Adds extra classes that the application is dependent on and is not traceable from the application itself.
   * E.g. Class name used in {@link Class#forName(String)}.
   * @param classes set of classes to add to dependency list for generating the deployment jar.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withDependencies(Iterable<Class<?>> classes);

  /**
   * Adds resources that will be available through the ClassLoader of the {@link TwillRunnable runnables}.
   * @see #withResources(Iterable)
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withResources(URI...resources);

  /**
   * Adds resources that will be available through the ClassLoader of the {@link TwillRunnable runnables}.
   * Useful for adding extra resource files or libraries that are not traceable from the application itself.
   * If the URI is a jar file, classes inside would be loadable by the ClassLoader. If the URI is a directory,
   * everything underneath would be available.
   *
   * @param resources Set of URI to the resources.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer withResources(Iterable<URI> resources);

  /**
   * Adds the set of paths to the classpath on the target machine for all runnables.
   * @see #withClassPaths(Iterable)
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withClassPaths(String... classPaths);

  /**
   * Adds the set of paths to the classpath on the target machine for all runnables.
   * Note that the paths would be just added without verification.
   * @param classPaths Set of classpaths
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withClassPaths(Iterable<String> classPaths);

  /**
   * Adds the set of environment variables that will be set as container environment variables for all runnables.
   *
   * @param env set of environment variables
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withEnv(Map<String, String> env);

  /**
   * Adds the set of environment variables that will be set as container environment variables for the given runnable.
   * Environment variables set through this method has higher precedence than the one set through {@link #withEnv(Map)}
   * if there is a key clash.
   *
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param env set of environment variables
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withEnv(String runnableName, Map<String, String> env);

  /**
   * Adds the set of paths to the classpath on the target machine for ApplicationMaster and all runnables.
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withApplicationClassPaths(String... classPaths);

  /**
   * Adds the set of paths to the classpath on the target machine for ApplicationMaster and all runnables.
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withApplicationClassPaths(Iterable<String> classPaths);

  /**
   * Uses {@link ClassAcceptor} to determine the classes to include in the bundle jar for
   * ApplicationMaster and all runnables.
   * @param classAcceptor to specify which classes to include in the bundle jar
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withBundlerClassAcceptor(ClassAcceptor classAcceptor);

  /**
   * Sets the maximum number of times (per instance) a runnable will be retried if it exits without success. The default
   * behavior is to retry indefinitely.
   * 
   * @param runnableName Name of the {@link TwillRunnable}.
   * @param maxRetries maximum number of retries per instance
   * @return This {@link TwillPreparer}
   */
  TwillPreparer withMaxRetries(String runnableName, int maxRetries);

  /**
   * Adds security credentials for the runtime environment to gives application access to resources.
   *
   * @param secureStore Contains security token available for the runtime environment.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer addSecureStore(SecureStore secureStore);

  /**
   * Set the root log level for Twill applications in all containers.
   *
   * @param logLevel the {@link LogEntry.Level} that should be set.
   *                 The level match the {@code Logback} levels.
   * @return This {@link TwillPreparer}.
   * @deprecated Use {@link #setLogLevels(Map)} with key {@link org.slf4j.Logger#ROOT_LOGGER_NAME} instead.
   */
  @Deprecated
  TwillPreparer setLogLevel(LogEntry.Level logLevel);

  /**
   * Set the log levels for requested logger names for Twill applications running in a container. The log level of any
   * logger cannot be {@code null}, if there is {@code null} value, a {@link IllegalArgumentException} will be thrown.
   *
   * @param logLevels The {@link Map} contains the requested logger names and log levels that need to be set.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer setLogLevels(Map<String, LogEntry.Level> logLevels);

  /**
   * Set the log levels for requested logger names for a {@link TwillRunnable}. The log level of any logger cannot be
   * {@code null}, if there is {@code null} value, a {@link IllegalArgumentException} will be thrown.
   *
   * @param runnableName The name of the runnable to set the log level.
   * @param logLevelsForRunnable The {@link Map} contains the requested logger names and log levels that need to be set.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer setLogLevels(String runnableName, Map<String, LogEntry.Level> logLevelsForRunnable);

  /**
   * Sets the class name of the {@link ClassLoader} to be used for loading twill and application classes for
   * all containers. The {@link ClassLoader} class should have a public constructor that takes two parameters in the
   * form of {@code (URL[] urls, ClassLoader parentClassLoader)}.
   * The first parameter is an array of {@link URL} that contains the list of {@link URL} for loading classes and
   * resources; the second parameter is the parent {@link ClassLoader}.
   *
   * @param classLoaderClassName name of the {@link ClassLoader} class.
   * @return This {@link TwillPreparer}.
   */
  TwillPreparer setClassLoader(String classLoaderClassName);

  /**
   * Starts the application. It's the same as calling {@link #start(long, TimeUnit)} with timeout of 60 seconds.
   *
   * @return A {@link TwillController} for controlling the running application.
   */
  TwillController start();

  /**
   * Starts the application. The application must be started within the given timeout. An application may fail to start
   * within a given time if there is insufficient resources.
   *
   * @param timeout maximum time to wait for the application to start
   * @param timeoutUnit unit for the timeout
   * @return A {@link TwillController} for controlling the running application.
   */
  TwillController start(long timeout, TimeUnit timeoutUnit);
}
