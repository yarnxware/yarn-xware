/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.yarn.remote.server;

import io.yarn.work.WorkBase;
import io.yarn.work.WorkScheduler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 *
 *
 */
public class SelectorWork extends WorkBase {

    private final ServerConfig serverConfig;

    private Selector selector;

    private ServerSocketChannel serverChannel;

    private WorkScheduler workScheduler;

    public SelectorWork(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    @Override
    public void doWork() {
        try {
            openSelector();
            listen();
            select();
        } catch (IOException ioEx) {
        }

        stop();
    }

    private void openSelector() throws IOException {
        selector = Selector.open();
    }

    private void listen() throws IOException {

        serverChannel = ServerSocketChannel.open();

        InetSocketAddress inetSocketAddress = new InetSocketAddress(serverConfig.getPort());
        serverChannel.bind(inetSocketAddress);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void select() throws IOException {
        int select = selector.select();
        if (select > 0) {
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectedKeys.iterator();
            while (iterator.hasNext()) {
                SelectionKey selectionKey = iterator.next();
                if (!selectionKey.isValid()) {
                    selectionKey.cancel();
                    iterator.remove();
                }

                if (selectionKey.isAcceptable()) {
                    handleAccept(selectionKey);
                } else if (selectionKey.isReadable()) {
                    handleRead(selectionKey);
                }
            }
        }
    }

    private void handleAccept(SelectionKey selectionKey) throws IOException {
        ServerSocketChannel readyServerChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel socketChannel = readyServerChannel.accept();
        socketChannel.register(selector, SelectionKey.OP_READ);
    }

    private void handleRead(SelectionKey selectionKey) throws IOException {
        SocketChannel readyChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer[] bsts = null;
        readyChannel.read(bsts);
    }

    private void stop() {
    }
}
