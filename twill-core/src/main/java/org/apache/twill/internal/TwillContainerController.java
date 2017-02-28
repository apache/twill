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
package org.apache.twill.internal;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.ServiceController;
import org.apache.twill.internal.state.Message;

import javax.annotation.Nullable;

/**
 * A {@link ServiceController} that allows sending a message directly. Internal use only.
 */
public interface TwillContainerController extends ServiceController, Service {

  ListenableFuture<Message> sendMessage(Message message);

  /**
   * Calls to indicated that the container that this controller is associated with is completed.
   * Any resources it hold will be releases and all pending futures will be cancelled.
   */
  void completed(int exitStatus);

  /**
   * @return the container's live node data.
   */
  @Nullable
  ContainerLiveNodeData getLiveNodeData();

  /**
   * @return the instance ID of the runnable that running in the container controlled by this controller.
   */
  int getInstanceId();
}
