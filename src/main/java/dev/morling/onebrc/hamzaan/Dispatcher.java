/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc.hamzaan;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class Dispatcher {

    private final ExecutorService executorService;
    private final int batchSize;

    private final ConcurrentLinkedQueue<CompletableFuture<Aggregate>> futures = new ConcurrentLinkedQueue<>();
    private final AtomicReference<Queue<String>> current = new AtomicReference<>(new LinkedList<>());
    private final AtomicInteger count = new AtomicInteger(0);

    public Dispatcher(ExecutorService executorService, int batchSize) {
        this.executorService = executorService;
        this.batchSize = batchSize;
    }

    Queue<CompletableFuture<Aggregate>> dispatch(Stream<String> lines) {
        lines.forEach(this::addOrDispatch);
        return futures;
    }

    private void addOrDispatch(String line) {
        Queue<String> lines = current.updateAndGet(strings -> {
            strings.add(line);
            count.incrementAndGet();
            return strings;
        });
        if (count.get() == batchSize) {
            current.set(new LinkedList<>());
            count.set(0);
            futures.add(CompletableFuture.supplyAsync(new Aggregator(lines), executorService));
        }
    }

}
