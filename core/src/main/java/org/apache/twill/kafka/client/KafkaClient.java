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

import org.apache.twill.internal.kafka.client.Compression;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.util.Iterator;

/**
 * This interface provides methods for interacting with kafka broker. It also
 * extends from {@link Service} for lifecycle management. The {@link #start()} method
 * must be called prior to other methods in this class. When instance of this class
 * is not needed, call {@link #stop()}} to release any resources that it holds.
 */
public interface KafkaClient extends Service {

  PreparePublish preparePublish(String topic, Compression compression);

  Iterator<FetchedMessage> consume(String topic, int partition, long offset, int maxSize);

  /**
   * Fetches offset from the given topic and partition.
   * @param topic Topic to fetch from.
   * @param partition Partition to fetch from.
   * @param time The first offset of every segment file for a given partition with a modified time less than time.
   *             {@code -1} for latest offset, {@code -2} for earliest offset.
   * @param maxOffsets Maximum number of offsets to fetch.
   * @return A Future that carry the result as an array of offsets in descending order.
   *         The size of the result array would not be larger than maxOffsets. If there is any error during the fetch,
   *         the exception will be carried in the exception.
   */
  ListenableFuture<long[]> getOffset(String topic, int partition, long time, int maxOffsets);
}
