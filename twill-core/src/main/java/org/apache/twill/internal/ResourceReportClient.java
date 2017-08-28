/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal;

import com.google.common.base.Charsets;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceReporter;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.internal.json.ResourceReportAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link ResourceReporter} that fetches reports from the given set of URLs.
 */
public final class ResourceReportClient implements ResourceReporter {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportClient.class);

  private final List<URL> resourceUrls;

  public ResourceReportClient(List<URL> resourceUrls) {
    this.resourceUrls = resourceUrls;
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    for (URL url : resourceUrls) {
      try {
        return fetchURL(url, "", ResourceReport.class);
      } catch (IOException e) {
        // Just log a trace as it's ok to not able to fetch resource report
        LOG.trace("Exception raised when getting resource report from {}.", url, e);
      }
    }
    return null;
  }

  @Nullable
  @Override
  public TwillRunResources getApplicationMasterResources() {
    for (URL url : resourceUrls) {
      try {
        return fetchURL(url, "/master", TwillRunResources.class);
      } catch (IOException e) {
        // Just log a trace as it's ok to not able to fetch resource report
        LOG.trace("Exception raised when getting resource report from {}.", url, e);
      }
    }
    return null;
  }

  @Override
  public Map<String, Collection<TwillRunResources>> getRunnablesResources() {
    for (URL url : resourceUrls) {
      try {
        return fetchURL(url, "/runnables", ResourceReportAdapter.RUNNABLES_RESOURCES_TYPE);
      } catch (IOException e) {
        // Just log a trace as it's ok to not able to fetch resource report
        LOG.trace("Exception raised when getting resource report from {}.", url, e);
      }
    }
    return Collections.emptyMap();
  }

  @Override
  public Collection<TwillRunResources> getInstancesResources(String runnableName) {
    for (URL url : resourceUrls) {
      try {
        return fetchURL(url, "/runnables/" + runnableName, ResourceReportAdapter.INSTANCES_RESOURCES_TYPE);
      } catch (IOException e) {
        // Just log a trace as it's ok to not able to fetch resource report
        LOG.trace("Exception raised when getting resource report from {}.", url, e);
      }
    }
    return Collections.emptyList();
  }

  private <T> T fetchURL(URL resourceURL, String path, Type responseType) throws IOException {
    URL url = path.isEmpty()
      ? resourceURL
      : new URL(resourceURL.getProtocol(), resourceURL.getHost(),
                resourceURL.getPort(), resourceURL.getPath() + path);
    try (Reader reader = new BufferedReader(new InputStreamReader(url.openStream(), Charsets.UTF_8))) {
      return ResourceReportAdapter.GSON.fromJson(reader, responseType);
    }
  }
}
