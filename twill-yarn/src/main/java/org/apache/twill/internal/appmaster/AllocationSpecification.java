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
package org.apache.twill.internal.appmaster;

import com.google.common.base.Objects;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * This class defines how the containers should be allocated.
 */
public class AllocationSpecification {

  /**
   * Defines different types of allocation strategies.
   */
  enum Type {
    ALLOCATE_ONE_INSTANCE_AT_A_TIME,
    DEFAULT
  }

  /**
   * Resource specification of runnables.
   */
  private Resource resource;

  /**
   * Allocation strategy Type.
   */
  private Type type;

  /**
   * Name of runnable. Set to null if the class represents more than one runnable.
   */
  private String runnableName;

  /**
   * Instance number for the runnable. Set to  0 if the class represents more than one instance / runnable.
   */
  private int instanceId;

  public AllocationSpecification(Resource resource) {
    this(resource, Type.DEFAULT, null, 0);
  }

  public AllocationSpecification(Resource resource, Type type, String runnableName, int instanceId) {
    this.resource = resource;
    this.type = type;
    this.runnableName = runnableName;
    this.instanceId = instanceId;
  }

  public Resource getResource() {
    return this.resource;
  }

  public Type getType() {
    return this.type;
  }

  public String getRunnableName() {
    return this.runnableName;
  }

  public int getInstanceId() {
    return this.instanceId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof AllocationSpecification)) {
      return false;
    }
    AllocationSpecification other = (AllocationSpecification) obj;
    return (instanceId == other.instanceId) &&
      Objects.equal(resource, other.resource) &&
      Objects.equal(type, other.type) &&
      Objects.equal(runnableName, other.runnableName);
  }

  @Override
  public int hashCode() {
    return this.resource.hashCode();
  }
}
