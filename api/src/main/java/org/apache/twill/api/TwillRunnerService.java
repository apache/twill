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

import com.google.common.util.concurrent.Service;

/**
 * A {@link TwillRunner} that extends {@link Service} to provide lifecycle management functions.
 * The {@link #start()} method needs to be called before calling any other method of this interface.
 * When done with this service, call {@link #stop()} to release any resources that it holds.
 */
public interface TwillRunnerService extends TwillRunner, Service {

}
