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
package org.apache.twill.internal.appmaster;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.internal.json.ResourceReportAdapter;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Webservice that the Application Master will register back to the resource manager
 * for clients to track application progress.  Currently used purely for getting a
 * breakdown of resource usage as a {@link org.apache.twill.api.ResourceReport}.
 */
public final class TrackerService extends AbstractIdleService {

  // TODO: This is temporary. When support more REST API, this would get moved.
  public static final String PATH = "/resources";

  private static final Logger LOG  = LoggerFactory.getLogger(TrackerService.class);
  private static final int NUM_BOSS_THREADS = 1;
  private static final int CLOSE_CHANNEL_TIMEOUT = 5;
  private static final int MAX_INPUT_SIZE = 100 * 1024 * 1024;

  private final Supplier<ResourceReport> resourceReport;
  private final ChannelGroup channelGroup;

  private String host;
  private ServerBootstrap bootstrap;
  private InetSocketAddress bindAddress;
  private URL url;

  /**
   * Initialize the service.
   *
   * @param resourceReport live report that the service will return to clients.
   */
  TrackerService(Supplier<ResourceReport> resourceReport) {
    this.channelGroup = new DefaultChannelGroup("appMasterTracker");
    this.resourceReport = resourceReport;
  }

  /**
   * Sets the hostname which the tracker service will bind to. This method must be called before starting this
   * tracker service.
   */
  void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the address this tracker service is bounded to.
   */
  InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  /**
   * @return tracker url.
   */
  URL getUrl() {
    return url;
  }

  @Override
  protected void startUp() throws Exception {
    Executor bossThreads = Executors.newFixedThreadPool(NUM_BOSS_THREADS,
                                                        new ThreadFactoryBuilder()
                                                          .setDaemon(true)
                                                          .setNameFormat("boss-thread")
                                                          .build());

    Executor workerThreads = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                                                             .setDaemon(true)
                                                             .setNameFormat("worker-thread#%d")
                                                             .build());

    ChannelFactory factory = new NioServerSocketChannelFactory(bossThreads, workerThreads);

    bootstrap = new ServerBootstrap(factory);

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(MAX_INPUT_SIZE));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("compressor", new HttpContentCompressor());
        pipeline.addLast("handler", new ReportHandler());

        return pipeline;
      }
    });

    Channel channel = bootstrap.bind(new InetSocketAddress(host, 0));
    bindAddress = (InetSocketAddress) channel.getLocalAddress();
    url = URI.create(String.format("http://%s:%d", host, bindAddress.getPort())).toURL();
    channelGroup.add(channel);

    LOG.info("Tracker service started at {}", url);
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      bootstrap.releaseExternalResources();
    }
    LOG.info("Tracker service stopped at {}", url);
  }

  /**
   * Handler to return resources used by this application master, which will be available through
   * the host and port set when this application master registered itself to the resource manager.
   */
  final class ReportHandler extends SimpleChannelUpstreamHandler {
    private final ResourceReportAdapter reportAdapter;

    ReportHandler() {
      this.reportAdapter = ResourceReportAdapter.create();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      HttpRequest request = (HttpRequest) e.getMessage();
      if (request.getMethod() != HttpMethod.GET) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED);
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setContent(ChannelBuffers.wrappedBuffer("Only GET is supported".getBytes(StandardCharsets.UTF_8)));
        writeResponse(e.getChannel(), response);
        return;
      }

      if (!PATH.equals(request.getUri())) {
        // Redirect all GET call to the /resources path.
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.TEMPORARY_REDIRECT);
        response.setHeader(HttpHeaders.Names.LOCATION, PATH);
        writeResponse(e.getChannel(), response);
        return;
      }

      writeResourceReport(e.getChannel());
    }

    private void writeResourceReport(Channel channel) {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=UTF-8");

      ChannelBuffer content = ChannelBuffers.dynamicBuffer();
      Writer writer = new OutputStreamWriter(new ChannelBufferOutputStream(content), CharsetUtil.UTF_8);
      reportAdapter.toJson(resourceReport.get(), writer);
      try {
        writer.close();
      } catch (IOException e1) {
        LOG.error("error writing resource report", e1);
      }
      response.setContent(content);
      writeResponse(channel, response);
    }

    private void writeResponse(Channel channel, HttpResponse response) {
      ChannelFuture future = channel.write(response);
      future.addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      e.getChannel().close();
    }
  }
}

