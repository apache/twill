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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.Map;

/**
 * This class hold information about the expected container count for each runnable. It also
 * keep track of the timestamp where the expected count has been updated.
 */
final class ExpectedContainers {

  private final Map<String, ExpectedCount> expectedCounts;

  ExpectedContainers(TwillSpecification twillSpec) {
    Map<String, ExpectedCount> expectedCounts = Maps.newHashMap();
    long now = System.currentTimeMillis();
    for (RuntimeSpecification runtimeSpec : twillSpec.getRunnables().values()) {
      expectedCounts.put(runtimeSpec.getName(),
                         new ExpectedCount(runtimeSpec.getResourceSpecification().getInstances(), now));
    }
    this.expectedCounts = expectedCounts;
  }

  synchronized void setExpected(String runnable, int expected) {
    expectedCounts.put(runnable, new ExpectedCount(expected, System.currentTimeMillis()));
  }

  /**
   * Updates the ExpectCount timestamp to current time.
   * @param runnables List of runnable names.
   */
  synchronized void updateRequestTime(Iterable<String> runnables) {
    for (String runnable : runnables) {
      ExpectedCount oldCount = expectedCounts.get(runnable);
      expectedCounts.put(runnable, new ExpectedCount(oldCount.getCount(), System.currentTimeMillis()));
    }
  }

  synchronized int getExpected(String runnable) {
    return expectedCounts.get(runnable).getCount();
  }

  synchronized Map<String, ExpectedCount> getAll() {
    return ImmutableMap.copyOf(expectedCounts);
  }

  static final class ExpectedCount {
    private final int count;
    private final long timestamp;

    private ExpectedCount(int count, long timestamp) {
      this.count = count;
      this.timestamp = timestamp;
    }

    int getCount() {
      return count;
    }

    long getTimestamp() {
      return timestamp;
    }
  }
}
