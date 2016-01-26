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
package org.apache.twill.internal.utils;

/**
 * Utility class to help adjusting container resource requirement.
 */
public final class Resources {

  /**
   * Computes the max heap size for a JVM process.
   *
   * @param containerMemory memory in MB of the container memory.
   *                        It is the maximum memory size allowed for the process.
   * @param nonHeapMemory memory in MB that needs to be reserved for non JVM heap memory for the process.
   * @param minHeapRatio minimum ratio for heap to non-heap memory.
   * @return memory in MB representing the max heap size for a JVM process.
   */
  public static int computeMaxHeapSize(int containerMemory, int nonHeapMemory, double minHeapRatio) {
    if (((double) (containerMemory - nonHeapMemory) / containerMemory) >= minHeapRatio) {
      // Reduce -Xmx by the reserved memory size.
      return containerMemory - nonHeapMemory;
    } else {
      // If it is a small VM, just discount it by the min ratio.
      return (int) Math.ceil(containerMemory * minHeapRatio);
    }
  }

  private Resources() {
  }
}
