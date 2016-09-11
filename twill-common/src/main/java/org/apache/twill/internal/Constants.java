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

/**
 * This class contains collection of common constants used in Twill.
 */
public final class Constants {

  public static final String LOG_TOPIC = "log";

  /** Maximum number of seconds for AM to start. */
  public static final int APPLICATION_MAX_START_SECONDS = 60;
  /** Maximum number of seconds for AM to stop. */
  public static final int APPLICATION_MAX_STOP_SECONDS = 60;

  public static final long PROVISION_TIMEOUT = 30000;

  /**
   * Milliseconds AM should wait for RM to allocate a constrained provision request.
   * On timeout, AM relaxes the request constraints.
   */
  public static final int CONSTRAINED_PROVISION_REQUEST_TIMEOUT = 5000;

  public static final double HEAP_MIN_RATIO = 0.7d;

  /** Memory size of AM. */
  public static final int APP_MASTER_MEMORY_MB = 512;

  /** CPUs for the AM. */
  public static final int APP_MASTER_CPU_VCORES = 1;

  public static final int APP_MASTER_RESERVED_MEMORY_MB = 150;

  public static final String CLASSPATH = "classpath";
  public static final String APPLICATION_CLASSPATH = "application-classpath";

  /** Command names for the restart runnable instances. */
  public static final String RESTART_ALL_RUNNABLE_INSTANCES = "restartAllRunnableInstances";
  public static final String RESTART_RUNNABLES_INSTANCES = "restartRunnablesInstances";

  /**
   * Common ZK paths constants
   */
  public static final String DISCOVERY_PATH_PREFIX = "/discoverable";
  public static final String INSTANCES_PATH_PREFIX = "/instances";


  /**
   * Constants for names of internal files that are shared between client, AM and containers.
   */
  public static final class Files {

    public static final String LAUNCHER_JAR = "launcher.jar";
    public static final String APP_MASTER_JAR = "appMaster.jar";
    public static final String CONTAINER_JAR = "container.jar";
    public static final String LOCALIZE_FILES = "localizeFiles.json";
    public static final String TWILL_SPEC = "twillSpec.json";
    public static final String ARGUMENTS = "arguments.json";
    public static final String ENVIRONMENTS = "environments.json";
    public static final String LOGBACK_TEMPLATE = "logback-template.xml";
    public static final String JVM_OPTIONS = "jvm.opts";
    public static final String CREDENTIALS = "credentials.store";

    private Files() {
    }
  }

  private Constants() {
  }
}
