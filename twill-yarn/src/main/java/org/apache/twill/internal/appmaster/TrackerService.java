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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.internal.json.ResourceReportAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
  private static final int NUM_WORKER_THREADS = 10;
  private static final int CLOSE_CHANNEL_TIMEOUT = 5;
  private static final int MAX_INPUT_SIZE = 100 * 1024 * 1024;

  private final Supplier<ResourceReport> resourceReport;

  private String host;
  private ServerBootstrap bootstrap;
  private ChannelGroup channelGroup;
  private InetSocketAddress bindAddress;
  private URL url;

  /**
   * Initialize the service.
   *
   * @param resourceReport live report that the service will return to clients.
   */
  TrackerService(Supplier<ResourceReport> resourceReport) {
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
    channelGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);
    EventLoopGroup bossGroup = new NioEventLoopGroup(NUM_BOSS_THREADS,
                                                     new ThreadFactoryBuilder()
                                                       .setDaemon(true).setNameFormat("boss-thread").build());
    EventLoopGroup workerGroup = new NioEventLoopGroup(NUM_WORKER_THREADS,
                                                       new ThreadFactoryBuilder()
                                                         .setDaemon(true).setNameFormat("worker-thread#%d").build());

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          channelGroup.add(ch);
          ChannelPipeline pipeline = ch.pipeline();
          pipeline.addLast("codec", new HttpServerCodec());
          pipeline.addLast("compressor", new HttpContentCompressor());
          pipeline.addLast("aggregator", new HttpObjectAggregator(MAX_INPUT_SIZE));
          pipeline.addLast("handler", new ReportHandler());
        }
      });

    Channel serverChannel = bootstrap.bind(new InetSocketAddress(host, 0)).sync().channel();
    channelGroup.add(serverChannel);

    bindAddress = (InetSocketAddress) serverChannel.localAddress();
    url = URI.create(String.format("http://%s:%d", host, bindAddress.getPort())).toURL();

    LOG.info("Tracker service started at {}", url);
  }

  @Override
  protected void shutDown() throws Exception {
    channelGroup.close().awaitUninterruptibly();

    List<Future<?>> futures = new ArrayList<>();
    futures.add(bootstrap.config().group().shutdownGracefully(0, CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS));
    futures.add(bootstrap.config().childGroup().shutdownGracefully(0, CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS));

    for (Future<?> future : futures) {
      future.awaitUninterruptibly();
    }

    LOG.info("Tracker service stopped at {}", url);
  }

  /**
   * Handler to return resources used by this application master, which will be available through
   * the host and port set when this application master registered itself to the resource manager.
   */
  final class ReportHandler extends ChannelInboundHandlerAdapter {
    private final ResourceReportAdapter reportAdapter;

    ReportHandler() {
      this.reportAdapter = ResourceReportAdapter.create();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      try {
        if (!(msg instanceof HttpRequest)) {
          // Ignore if it is not HttpRequest
          return;
        }

        HttpRequest request = (HttpRequest) msg;
        if (!HttpMethod.GET.equals(request.method())) {
          FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.METHOD_NOT_ALLOWED,
            Unpooled.copiedBuffer("Only GET is supported", StandardCharsets.UTF_8));

          HttpUtil.setContentLength(response, response.content().readableBytes());
          response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
          writeAndClose(ctx.channel(), response);
          return;
        }

        if (!PATH.equals(request.uri())) {
          // Redirect all GET call to the /resources path.
          HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                                                              HttpResponseStatus.TEMPORARY_REDIRECT);
          HttpUtil.setContentLength(response, 0);
          response.headers().set(HttpHeaderNames.LOCATION, PATH);
          writeAndClose(ctx.channel(), response);
          return;
        }

        writeResourceReport(ctx.channel());
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      ctx.channel().close();
    }

    private void writeResourceReport(Channel channel) {
      ByteBuf content = Unpooled.buffer();
      Writer writer = new OutputStreamWriter(new ByteBufOutputStream(content), CharsetUtil.UTF_8);
      try {
        reportAdapter.toJson(resourceReport.get(), writer);
        writer.close();
      } catch (IOException e) {
        LOG.error("error writing resource report", e);
        writeAndClose(channel, new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR,
          Unpooled.copiedBuffer(e.getMessage(), StandardCharsets.UTF_8)));
        return;
      }

      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
      HttpUtil.setContentLength(response, content.readableBytes());
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
      channel.writeAndFlush(response);
    }

    private void writeAndClose(Channel channel, HttpResponse response) {
      channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
  }
}

