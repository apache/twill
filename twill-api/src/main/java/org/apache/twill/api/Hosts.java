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
 * Represents a list of hosts.
 */

public class Hosts {
  private final Set<String> hosts;

  public Hosts(Set<String> hosts) {
    this.hosts = Collections.unmodifiableSet(new HashSet<String>(hosts));
  }

  public Hosts(String host, String...moreHosts) {
    Set<String> hosts = new HashSet<String>();
    hosts.add(host);
    hosts.addAll(Arrays.asList(moreHosts));

    this.hosts = Collections.unmodifiableSet(hosts);
  }

  /**
   * Convenience method to create an instance of {@link org.apache.twill.api.Hosts}.
   * @param host A host to be added.
   * @param moreHosts A list of hosts to be added.
   * @return An instance of {@link org.apache.twill.api.Hosts} containing specified hosts.
   */
  public static Hosts of(String host, String...moreHosts) {
    return new Hosts(host, moreHosts);
  }

  /**
   * Get the list of hosts.
   * @return list of hosts.
   */
  public Set<String> get() {
    return this.hosts;
  }

  @Override
  public String toString() {
    return this.hosts.toString();
  }
}
