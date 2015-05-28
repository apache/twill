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
package org.apache.twill.internal.state;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Define interface that could provide override-able callback method when a message is received.
 */
public interface MessageCallback {

  /**
   * Called when a message is received.
   * @param message Message being received.
   * @return A {@link ListenableFuture} that would be completed when message processing is completed or failed.
   *         The result of the future should be the input message Id if succeeded.
   */
  ListenableFuture<String> onReceived(String messageId, Message message);
}
