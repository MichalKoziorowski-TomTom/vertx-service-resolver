/*
 * Copyright (c) 2011-2023 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.serviceresolver.kube.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.spi.endpoint.EndpointBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class KubeServiceState<B> {

  final String name;
  final Vertx vertx;
  final KubeResolverImpl<B> resolver;
  final EndpointBuilder<B, SocketAddress> endpointsBuilder;
  String lastResourceVersion;
  boolean disposed;
  WebSocket ws;
  AtomicReference<B> endpoints = new AtomicReference<>();
  private boolean refreshing;

  private KubeServiceState(EndpointBuilder<B, SocketAddress> endpointsBuilder, KubeResolverImpl<B> resolver, Vertx vertx, String lastResourceVersion, String name) {
    this.endpointsBuilder = endpointsBuilder;
    this.name = name;
    this.resolver = resolver;
    this.vertx = vertx;
    this.lastResourceVersion = lastResourceVersion;
  }

  static <B> Future<KubeServiceState<B>> create(EndpointBuilder<B, SocketAddress> builder, KubeResolverImpl<B> resolver, Vertx vertx, String serviceName) {
    return listEndpoints(resolver)
      .map(response -> {
        String resourceVersion = response.getJsonObject("metadata").getString("resourceVersion");
        KubeServiceState<B> state = new KubeServiceState<>(builder, resolver, vertx, resourceVersion, serviceName);
        JsonArray items = response.getJsonArray("items");
        if (items != null) {
          for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.getJsonObject(i);
            state.handleEndpoints(item);
          }
        }
        return state;
      })
      .andThen(ar -> {
        if (ar.succeeded()) {
          // Initiate watch after initial state is prepared; do not wait for WebSocket to establish
            ar.result().connectWebSocket();
        }
      });
  }

  private static Future<JsonObject> listEndpoints(KubeResolverImpl<?> resolver) {
    return resolver.httpClient
      .request(new RequestOptions()
        .setMethod(HttpMethod.GET)
        .setServer(resolver.server)
        .setURI("/api/v1/namespaces/" + resolver.namespace + "/endpoints"))
      .compose(req -> {
        if (resolver.bearerToken != null) {
          req.putHeader(HttpHeaders.AUTHORIZATION, "Bearer " + resolver.bearerToken);
        }
        return req.send().compose(resp -> {
          if (resp.statusCode() == 200) {
            return resp.body().map(Buffer::toJsonObject);
          } else {
            return resp.body().transform(ar -> {
              StringBuilder msg = new StringBuilder("Invalid status code " + resp.statusCode());
              if (ar.succeeded()) {
                msg.append(" : ").append(ar.result());
              }
              return Future.failedFuture(msg.toString());
            });
          }
        });
      });
  }

  void connectWebSocket() {
    String requestURI = "/api/v1/namespaces/" + resolver.namespace + "/endpoints?watch=true&allowWatchBookmarks=true&resourceVersion=" + lastResourceVersion;
    WebSocketConnectOptions connectOptions = new WebSocketConnectOptions();
    connectOptions.setServer(resolver.server);
    connectOptions.setURI(requestURI);
    if (resolver.bearerToken != null) {
      connectOptions.putHeader(HttpHeaders.AUTHORIZATION, "Bearer " + resolver.bearerToken);
    }
    resolver.wsClient.webSocket()
      .handler(buff -> handleUpdate(buff.toJsonObject()))
      .closeHandler(v -> { if (!disposed) connectWebSocket(); })
      .connect(connectOptions)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          WebSocket ws = ar.result();
          if (disposed) {
            ws.close();
          } else {
            this.ws = ws;
          }
        } else if (!disposed) {
          String msg = ar.cause() != null ? ar.cause().getMessage() : null;
            if (msg != null && msg.contains("410")) {
              triggerRefreshFromList();
              return;
            }
            vertx.setTimer(500, id -> connectWebSocket());
        }
      });
  }

  void handleUpdate(JsonObject update) {
    String type = update.getString("type");
    JsonObject object = update.getJsonObject("object");
    if (object == null) return;
    if ("ERROR".equals(type)) {
      Integer code = object.getInteger("code");
      String reason = object.getString("reason");
      if ((code != null && code == 410) || (reason != null && reason.toLowerCase().contains("expired"))) {
        triggerRefreshFromList();
        return;
      }
    }
    JsonObject metadata = object.getJsonObject("metadata");
    if (metadata == null) return;
    String resourceVersion = metadata.getString("resourceVersion");
    if (resourceVersion != null && !resourceVersion.equals(lastResourceVersion)) {
      handleEndpoints(object);
      lastResourceVersion = resourceVersion;
    }
  }

  private void triggerRefreshFromList() {
    if (refreshing || disposed) return;
    refreshing = true;
    listEndpoints(resolver).onComplete(ar -> {
      refreshing = false;
      if (disposed) return;
      if (ar.succeeded()) {
        JsonObject response = ar.result();
        String newRV = response.getJsonObject("metadata").getString("resourceVersion");
        if (newRV != null) lastResourceVersion = newRV;
        JsonArray items = response.getJsonArray("items");
        if (items != null) {
          for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.getJsonObject(i);
            handleEndpoints(item);
          }
        }
        if (ws != null) {
          ws.close(); // close handler will reconnect with new RV
        } else {
          connectWebSocket();
        }
      } else {
        vertx.setTimer(1000, id -> triggerRefreshFromList());
      }
    });
  }

  void handleEndpoints(JsonObject item) {
    JsonObject metadata = item.getJsonObject("metadata");
    if (metadata == null) return;
    String name = metadata.getString("name");
    if (this.name.equals(name)) {
      JsonArray subsets = item.getJsonArray("subsets");
      EndpointBuilder<B, SocketAddress> builder = endpointsBuilder;
      if (subsets != null) {
        for (int j = 0; j < subsets.size(); j++) {
          List<String> podIps = new ArrayList<>();
          JsonObject subset = subsets.getJsonObject(j);
          JsonArray addresses = subset.getJsonArray("addresses");
          JsonArray ports = subset.getJsonArray("ports");
          if (addresses == null || ports == null) continue;
          for (int k = 0; k < addresses.size(); k++) {
            JsonObject address = addresses.getJsonObject(k);
            String ip = address.getString("ip");
            if (ip != null) podIps.add(ip);
          }
          for (int k = 0; k < ports.size(); k++) {
            JsonObject port = ports.getJsonObject(k);
            Integer podPort = port.getInteger("port");
            if (podPort == null) continue;
            for (String podIp : podIps) {
              SocketAddress podAddress = SocketAddress.inetSocketAddress(podPort, podIp);
              builder = builder.addServer(podAddress, podIp + "-" + podPort);
            }
          }
        }
      }
      this.endpoints.set(builder.build());
    }
  }
}
