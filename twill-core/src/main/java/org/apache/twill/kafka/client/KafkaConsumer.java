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

import org.apache.twill.common.Cancellable;

import java.util.Iterator;

/**
 * A consumer for consuming (reading) messages published to a Kafka server cluster.
 */
public interface KafkaConsumer {

  /**
   * Callback for receiving new messages.
   */
  interface MessageCallback {

    /**
     * Invoked when new messages is available.
     * @param messages Iterator of new messages. The {@link FetchedMessage} instance maybe reused in the Iterator
     *                 and across different invocation.
     * @return The offset of the message to be fetched next.
     */
    long onReceived(Iterator<FetchedMessage> messages);

    /**
     * Invoked when message consumption is stopped. When this method is invoked,
     * no more {@link #onReceived(java.util.Iterator)} will get triggered.
     */
    void finished();
  }

  /**
   * A builder for preparing message consumption.
   */
  interface Preparer {

    /**
     * Consumes messages from a given offset. If the given offset is invalid, it'll start consuming from the
     * latest offset.
     * @param topic Topic to consume from.
     * @param partition Partition in the topic to consume from.
     * @param offset Offset to starts with.
     * @return This {@link Preparer} instance.
     */
    Preparer add(String topic, int partition, long offset);

    /**
     * Consumes messages from the earliest message available.
     * @param topic Topic to consume from.
     * @param partition Partition in the topic to consume from.
     * @return This {@link Preparer} instance.
     */
    Preparer addFromBeginning(String topic, int partition);

    /**
     * Consumes messages from the latest message.
     * @param topic Topic to consume from.
     * @param partition Partition in the topic to consume from.
     * @return This {@link Preparer} instance.
     */
    Preparer addLatest(String topic, int partition);

    /**
     * Starts the consumption as being configured by this {@link Preparer}.
     * @param callback The {@link MessageCallback} for receiving new messages.
     * @return A {@link Cancellable} for cancelling message consumption.
     */
    Cancellable consume(MessageCallback callback);
  }

  /**
   * Prepares for message consumption.
   * @return A {@link Preparer} to setup details about message consumption.
   */
  Preparer prepare();
}
