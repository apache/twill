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
package org.apache.twill.internal.yarn;

import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.ProcessLauncher;

/**
 * Interface for launching Yarn application from client.
 */
public interface YarnAppClient extends Service {

  /**
   * Creates a {@link ProcessLauncher} for launching the application represented by the given spec.
   */
  ProcessLauncher<ApplicationId> createLauncher(TwillSpecification twillSpec) throws Exception;

  /**
   * Creates a {@link ProcessLauncher} for launching application with the given user and spec.
   *
   * @deprecated This method will get removed.
   */
  @Deprecated
  ProcessLauncher<ApplicationId> createLauncher(String user, TwillSpecification twillSpec) throws Exception;

  ProcessController<YarnApplicationReport> createProcessController(ApplicationId appId);
}
