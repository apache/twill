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
package org.apache.twill.yarn;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests with mini yarn cluster.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  ContainerSizeTestRun.class,
  CustomClassLoaderTestRun.class,
  DebugTestRun.class,
  DistributeShellTestRun.class,
  EchoServerTestRun.class,
  EnvironmentTestRun.class,
  EventHandlerTestRun.class,
  FailureRestartTestRun.class,
  InitializeFailTestRun.class,
  JvmOptionsTestRun.class,
  LocalFileTestRun.class,
  LogHandlerTestRun.class,
  LogLevelChangeTestRun.class,
  LogLevelTestRun.class,
  PlacementPolicyTestRun.class,
  ProvisionTimeoutTestRun.class,
  ResourceReportTestRun.class,
  ServiceDiscoveryTestRun.class,
  SessionExpireTestRun.class,
  TaskCompletedTestRun.class,
  RestartRunnableTestRun.class,
  MaxRetriesTestRun.class
})
public final class YarnTestSuite extends BaseYarnTest {
}
