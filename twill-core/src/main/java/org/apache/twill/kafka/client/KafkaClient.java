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

/**
 * Represents a kafka client that can publish/subscribe to a Kafka server cluster.
 */
public interface KafkaClient {

  /**
   * Creates a {@link KafkaPublisher} that is ready for publish.
   * @param ack Type of ack that the publisher would use for all it's publish.
   * @param compression The compression type for messages published through the returned publisher.
   * @return A {@link KafkaPublisher}.
   */
  KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression);

  /**
   * Creates a {@link KafkaConsumer} for consuming messages.
   * @return A {@link KafkaConsumer}.
   */
  KafkaConsumer getConsumer();
}
