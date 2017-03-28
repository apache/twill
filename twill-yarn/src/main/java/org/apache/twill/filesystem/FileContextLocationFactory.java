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
package org.apache.twill.filesystem;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.Configs;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;

/**
 * A {@link LocationFactory} implementation that uses {@link FileContext} to create {@link Location}.
 */
public class FileContextLocationFactory implements LocationFactory {

  private final Configuration configuration;
  private final Path pathBase;
  private final LoadingCache<UserGroupInformation, FileContext> fileContextCache;

  /**
   * Same as {@link #FileContextLocationFactory(Configuration, String) FileContextLocationFactory(configuration, "/")}.
   */
  public FileContextLocationFactory(Configuration configuration) {
    this(configuration, "/");
  }

  /**
   * Creates a new instance.
   *
   * @param configuration the hadoop configuration
   * @param pathBase base path for all non-absolute location created through this {@link LocationFactory}.
   */
  public FileContextLocationFactory(final Configuration configuration, String pathBase) {
    this.configuration = configuration;
    this.pathBase = new Path(pathBase.startsWith("/") ? pathBase : "/" + pathBase);

    int maxCacheSize = configuration.getInt(Configs.Keys.FILE_CONTEXT_CACHE_MAX_SIZE,
                                            Configs.Defaults.FILE_CONTEXT_CACHE_MAX_SIZE);
    this.fileContextCache = CacheBuilder
      .newBuilder()
      .weakKeys()
      .weakValues()
      .maximumSize(maxCacheSize)
      .build(new CacheLoader<UserGroupInformation, FileContext>() {
        @Override
        public FileContext load(UserGroupInformation ugi) throws Exception {
          return ugi.doAs(new PrivilegedExceptionAction<FileContext>() {
            @Override
            public FileContext run() throws UnsupportedFileSystemException {
              return FileContext.getFileContext(configuration);
            }
          });
        }
      });
  }

  /**
   * Creates a new instance with the given {@link FileContext} created from the given {@link Configuration}.
   *
   * @param configuration the hadoop configuration
   * @param fc {@link FileContext} instance created from the given configuration
   * @param pathBase base path for all non-absolute location created through this (@link LocationFactory}.
   *
   * @deprecated Use {@link #FileContextLocationFactory(Configuration)}
   *             or {@link #FileContextLocationFactory(Configuration, String)} instead. The {@link FileContext}
   *             provided to this method will only be used if the current user calling any methods of this class
   *             matches with the {@link UserGroupInformation} of the {@link FileContext} instance.
   */
  @Deprecated
  public FileContextLocationFactory(Configuration configuration, FileContext fc, String pathBase) {
    this(configuration, pathBase);
    this.fileContextCache.put(fc.getUgi(), fc);
  }

  @Override
  public Location create(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    Path locationPath;
    if (path.isEmpty()) {
      locationPath = pathBase;
    } else {
      locationPath = new Path(path);
    }
    FileContext fc = getFileContext();
    locationPath = locationPath.makeQualified(fc.getDefaultFileSystem().getUri(), pathBase);
    return new FileContextLocation(this, fc, locationPath);
  }

  @Override
  public Location create(URI uri) {
    FileContext fc = getFileContext();
    URI contextURI = fc.getWorkingDirectory().toUri();
    if (Objects.equals(contextURI.getScheme(), uri.getScheme())
      && Objects.equals(contextURI.getAuthority(), uri.getAuthority())) {
      // A full URI
      return new FileContextLocation(this, fc, new Path(uri));
    }

    if (uri.isAbsolute()) {
      // Needs to be of the same scheme
      Preconditions.checkArgument(Objects.equals(contextURI.getScheme(), uri.getScheme()),
                                  "Only URI with '%s' scheme is supported", contextURI.getScheme());
      Path locationPath = new Path(uri).makeQualified(fc.getDefaultFileSystem().getUri(), pathBase);
      return new FileContextLocation(this, fc, locationPath);
    }

    return create(uri.getPath());
  }

  @Override
  public Location getHomeLocation() {
    FileContext fc = getFileContext();
    // Fix for TWILL-163. FileContext.getHomeDirectory() uses System.getProperty("user.name") instead of UGI
    return new FileContextLocation(this, fc,
                                   new Path(fc.getHomeDirectory().getParent(), fc.getUgi().getShortUserName()));
  }

  /**
   * Returns the {@link FileContext} for the current user based on {@link UserGroupInformation#getCurrentUser()}.
   *
   * @throws IllegalStateException if failed to determine the current user or fail to create the FileContext.
   * @throws RuntimeException if failed to get the {@link FileContext} object for the current user due to exception
   */
  public FileContext getFileContext() {
    try {
      return fileContextCache.getUnchecked(UserGroupInformation.getCurrentUser());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get current user information", e);
    } catch (UncheckedExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UnsupportedFileSystemException) {
        String defaultURI = configuration.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                                              CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT);
        throw new IllegalStateException("File system with URI '" + defaultURI + "' is not supported", cause);
      }
      throw (cause instanceof RuntimeException) ? (RuntimeException) cause : new RuntimeException(cause);
    }
  }

  /**
   * Returns the {@link Configuration} used by this {@link LocationFactory}.
   */
  public Configuration getConfiguration() {
    return configuration;
  }
}
