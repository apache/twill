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
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Straightforward implementation of {@link RuntimeSpecification}.
 */
public final class DefaultRuntimeSpecification implements RuntimeSpecification {

  private final String name;
  private final TwillRunnableSpecification runnableSpec;
  private final ResourceSpecification resourceSpec;
  private final Collection<LocalFile> localFiles;

  public DefaultRuntimeSpecification(String name,
                                     TwillRunnableSpecification runnableSpec,
                                     ResourceSpecification resourceSpec,
                                     Collection<LocalFile> localFiles) {
    this.name = name;
    this.runnableSpec = runnableSpec;
    this.resourceSpec = resourceSpec;
    this.localFiles = Collections.unmodifiableList(new ArrayList<LocalFile>(localFiles));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public TwillRunnableSpecification getRunnableSpecification() {
    return runnableSpec;
  }

  @Override
  public ResourceSpecification getResourceSpecification() {
    return resourceSpec;
  }

  @Override
  public Collection<LocalFile> getLocalFiles() {
    return localFiles;
  }
}
