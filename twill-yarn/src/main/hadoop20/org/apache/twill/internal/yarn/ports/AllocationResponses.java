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
package org.apache.twill.internal.yarn.ports;

import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.List;

/**
 * Factory for building instance of {@link AllocationResponse} based on the response type.
 */
public final class AllocationResponses {

  /**
   * A hack for CDH 4.4.0, as the AllocateResponse class is being rewritten and diverted from YARN 2.0.
   */
  private static final boolean IS_CDH_4_4;

  static {
    boolean result = false;
    try {
      try {
        // See if it is standard YARN 2.0 AllocateResponse object.
        AllocateResponse.class.getMethod("getAMResponse");
      } catch (NoSuchMethodException e) {
          // See if it is CDH 4.4 AllocateResponse object.
        AllocationResponse.class.getMethod("getAllocatedContainers");
        result = true;
      }
    } catch (Exception e) {
      // Something very wrong in here, as it shouldn't arrive here.
      e.printStackTrace();
      throw Throwables.propagate(e);
    }

    IS_CDH_4_4 = result;
  }

  public static AllocationResponse create(Object response) {
    if (IS_CDH_4_4) {
      return new ReflectionAllocationResponse(response);
    }

    try {
      Object amResponse = response.getClass().getMethod("getAMResponse").invoke(response);
      return new ReflectionAllocationResponse(amResponse);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static final class ReflectionAllocationResponse implements AllocationResponse {

    private final Object response;

    private ReflectionAllocationResponse(Object response) {
      this.response = response;
    }

    @Override
    public int getResponseId() {
      return call("getResponseId", TypeToken.of(Integer.class));
    }

    @Override
    public Resource getAvailableResources() {
      return call("getAvailableResources", TypeToken.of(Resource.class));
    }

    @Override
    public List<Container> getAllocatedContainers() {
      return call("getAllocatedContainers", new TypeToken<List<Container>>() { });
    }

    @Override
    public List<ContainerStatus> getCompletedContainersStatuses() {
      return call("getCompletedContainersStatuses", new TypeToken<List<ContainerStatus>>() { });
    }

    private <T> T call(String methodName, TypeToken<T> resultType) {
      try {
        return (T) resultType.getRawType().cast(response.getClass().getMethod(methodName).invoke(response));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private AllocationResponses() {
  }
}
