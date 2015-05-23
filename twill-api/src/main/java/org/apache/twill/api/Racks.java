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
package org.apache.twill.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a list of Racks.
 */

public class Racks {
  private final Set<String> racks;

  public Racks(Set<String> racks) {
    this.racks = Collections.unmodifiableSet(new HashSet<String>(racks));
  }

  public Racks(String rack, String...moreRacks) {
    Set<String> racks = new HashSet<String>();
    racks.add(rack);
    racks.addAll(Arrays.asList(moreRacks));

    this.racks = Collections.unmodifiableSet(racks);

  }

  /**
   * Convenience method to create an instance of {@link org.apache.twill.api.Racks}.
   * @param rack A rack to be added.
   * @param moreRacks A list of racks to be added.
   * @return An instance of {@link org.apache.twill.api.Racks} containing specified racks.
   */
  public static Racks of(String rack, String...moreRacks) {
    return new Racks(rack, moreRacks);
  }

  /**
   * Get the list of racks.
   * @return list of racks.
   */
  public Set<String> get() {
    return this.racks;
  }

  @Override
  public String toString() {
    return this.racks.toString();
  }
}
