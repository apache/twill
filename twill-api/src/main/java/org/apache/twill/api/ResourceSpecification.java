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

import org.apache.twill.internal.DefaultResourceSpecification;

/**
 * This interface provides specifications for resource requirements including set and get methods
 * for number of cores, amount of memory, and number of instances.
 */
public interface ResourceSpecification {

  final ResourceSpecification BASIC = Builder.with().setVirtualCores(1).setMemory(512, SizeUnit.MEGA).build();

  /**
   * Unit for specifying memory size.
   */
  enum SizeUnit {
    MEGA(1),
    GIGA(1024);

    private final int multiplier;

    private SizeUnit(int multiplier) {
      this.multiplier = multiplier;
    }
  }

  /**
   * Returns the number of virtual CPU cores. DEPRECATED, use getVirtualCores instead.
   * @return Number of virtual CPU cores.
   */
  @Deprecated
  int getCores();

  /**
   * Returns the number of virtual CPU cores.
   * @return Number of virtual CPU cores.
   */
  int getVirtualCores();

  /**
   * Returns the memory size in MB.
   * @return Memory size
   */
  int getMemorySize();

  /**
   * Returns the uplink bandwidth in Mbps.
   * @return Uplink bandwidth or -1 representing unlimited bandwidth.
   */
  int getUplink();

  /**
   * Returns the downlink bandwidth in Mbps.
   * @return Downlink bandwidth or -1 representing unlimited bandwidth.
   */
  int getDownlink();

  /**
   * Returns number of execution instances.
   * @return Number of execution instances.
   */
  int getInstances();

  /**
   * Returns the execution hosts, expects Fully Qualified Domain Names host + domain.
   * This is a suggestion for the scheduler depending on cluster load it may ignore it
   * @return An array containing the hosts where the containers should run
   */
  String[] getHosts();

  /**
   * Returns the execution racks.
   * This is a suggestion for the scheduler depending on cluster load it may ignore it
   * @return An array containing the racks where the containers should run
   */
  String[] getRacks();

  /**
   * Builder for creating {@link ResourceSpecification}.
   */
  static final class Builder {

    private int cores;
    private int memory;
    private int uplink = -1;
    private int downlink = -1;
    private int instances = 1;
    private String[] hosts = new String[0];
    private String[] racks = new String[0];

    public static CoreSetter with() {
      return new Builder().new CoreSetter();
    }

    public final class HostsSetter extends Build {
      public RackSetter setHosts(String... hosts) {
        if (hosts != null) {
          Builder.this.hosts = hosts.clone();
        }
        return new RackSetter();
      }
    }

    public final class RackSetter extends Build {
      public AfterRacks setRacks(String... racks) {
        if (racks != null) {
          Builder.this.racks = racks.clone();
        }
        return new AfterRacks();
      }
    }

    public final class CoreSetter {
      @Deprecated
      public MemorySetter setCores(int cores) {
        Builder.this.cores = cores;
        return new MemorySetter();
      }

      public MemorySetter setVirtualCores(int cores) {
        Builder.this.cores = cores;
        return new MemorySetter();
      }
    }

    public final class MemorySetter {
      public AfterMemory setMemory(int size, SizeUnit unit) {
        Builder.this.memory = size * unit.multiplier;
        return new AfterMemory();
      }
    }

    public final class AfterMemory extends Build {
      public HostsSetter setInstances(int instances) {
        Builder.this.instances = instances;
        return new HostsSetter();
      }
    }

    public final class AfterRacks extends Build {
      public AfterUplink setUplink(int uplink, SizeUnit unit) {
        Builder.this.uplink = uplink * unit.multiplier;
        return new AfterUplink();
      }
    }

    public final class AfterUplink extends Build {
      public Done setDownlink(int downlink, SizeUnit unit) {
        Builder.this.downlink = downlink * unit.multiplier;
        return new Done();
      }
    }

    public final class Done extends Build {
    }

    public abstract class Build {
      public ResourceSpecification build() {
        return new DefaultResourceSpecification(cores, memory, instances, uplink, downlink, hosts, racks);
      }
    }

    private Builder() {}
  }
}
