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
package org.apache.twill.yarn;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.internal.json.ResourceReportAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.List;

/**
 * Package private class to get {@link ResourceReport} from the application master.
 */
final class ResourceReportClient {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportClient.class);

  private final ResourceReportAdapter reportAdapter;
  private final List<URL> resourceUrls;

  ResourceReportClient(List<URL> resourceUrls) {
    this.resourceUrls = resourceUrls;
    this.reportAdapter = ResourceReportAdapter.create();
  }

  /**
   * Returns the resource usage of the application fetched from the resource endpoint URL.
   * @return A {@link ResourceReport} or {@code null} if failed to fetch the report.
   */
  public ResourceReport get() {
    for (URL url : resourceUrls) {
      try {
        Reader reader = new BufferedReader(new InputStreamReader(url.openStream(), Charsets.UTF_8));
        try {
          LOG.trace("Report returned by {}", url);
          return reportAdapter.fromJson(reader);
        } finally {
          Closeables.closeQuietly(reader);
        }
      } catch (IOException e) {
        // Just log a trace as it's ok to not able to fetch resource report
        LOG.trace("Exception raised when getting resource report from {}.", url, e);
      }
    }
    return null;
  }
}
