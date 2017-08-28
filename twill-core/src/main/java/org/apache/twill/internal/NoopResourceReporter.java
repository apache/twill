/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal;

import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceReporter;
import org.apache.twill.api.TwillRunResources;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link ResourceReporter} that act as if resource report is not available.
 */
public final class NoopResourceReporter implements ResourceReporter {

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return null;
  }

  @Nullable
  @Override
  public TwillRunResources getApplicationMasterResources() {
    return null;
  }

  @Override
  public Map<String, Collection<TwillRunResources>> getRunnablesResources() {
    return Collections.emptyMap();
  }

  @Override
  public Collection<TwillRunResources> getInstancesResources(String runnableName) {
    return Collections.emptyList();
  }
}
