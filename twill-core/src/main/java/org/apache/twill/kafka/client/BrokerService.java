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
package org.apache.twill.kafka.client;

import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.Executor;

/**
 * Service for providing information of kafka brokers.
 */
public interface BrokerService extends Service {

  /**
   * Returns the broker information of the current leader of the given topic and partition.
   * @param topic Topic for looking up for leader.
   * @param partition Partition for looking up for leader.
   * @return A {@link BrokerInfo} containing information about the current leader, or {@code null} if
   *         current leader is unknown.
   */
  BrokerInfo getLeader(String topic, int partition);

  /**
   * Returns a live iterable that gives information for all the known brokers.
   * @return An {@link Iterable} of {@link BrokerInfo} that when {@link Iterable#iterator()} is called, it returns
   *         an iterator that gives the latest list of {@link BrokerInfo}.
   */
  Iterable<BrokerInfo> getBrokers();

  /**
   * Returns a comma separate string of all current brokers.
   * @return A string in the format {@code host1:port1,host2:port2} or empty string if no broker has been discovered.
   */
  String getBrokerList();

  /**
   * Adds a listener to changes in broker list managed by this service.
   *
   * @param listener The listener to invoke when there is changes.
   * @param executor Executor to use for invocation to the listener.
   * @return A {@link Cancellable} to stop listening.
   */
  Cancellable addChangeListener(BrokerChangeListener listener, Executor executor);

  /**
   * Listener for changes in broker list.
   */
  abstract class BrokerChangeListener {

    /**
     * Invoked when there is a change in the broker list.
     *
     * @param brokerService The {@link BrokerService} that has broker list changes.
     */
    public void changed(BrokerService brokerService) {
      // No-op
    }
  }
}
