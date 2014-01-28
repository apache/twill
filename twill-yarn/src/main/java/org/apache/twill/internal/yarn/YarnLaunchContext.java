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

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * This interface is for adapting ContainerLaunchContext in different Hadoop version.
 */
public interface YarnLaunchContext {

  <T> T getLaunchContext();

  void setCredentials(Credentials credentials);

  void setLocalResources(Map<String, YarnLocalResource> localResources);

  void setServiceData(Map<String, ByteBuffer> serviceData);

  Map<String, String> getEnvironment();

  void setEnvironment(Map<String, String> environment);

  List<String> getCommands();

  void setCommands(List<String> commands);

  void setApplicationACLs(Map<ApplicationAccessType, String> acls);
}
