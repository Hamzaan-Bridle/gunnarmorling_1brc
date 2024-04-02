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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CalculateAverage_hamzaan {

    private static final Path FILE = Path.of("./measurements.txt");

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, TimeoutException {

        Args parsingArgs = new Args();

        for (String arg : args) {
            if (arg.startsWith("--batchSize=")) {
                String[] split = arg.split("=");
                parsingArgs.batchSize = Integer.parseInt(split[1]);
            }
            if ("--machineThreads".equals(arg)) {
                parsingArgs.virtualThreads = false;
            }
        }

        System.out.println(parsingArgs);

        long start = System.nanoTime();
        try (ExecutorService executorService = getExecutorService(parsingArgs);
                Stream<String> lines = Files.lines(FILE)) {
            Dispatcher dispatcher = new Dispatcher(executorService, parsingArgs.batchSize);
            Queue<CompletableFuture<Aggregate>> futures = dispatcher.dispatch(lines);
            Aggregate aggregate = requireNonNull(futures.poll()).get(10, TimeUnit.SECONDS);
            for (CompletableFuture<Aggregate> future : futures) {
                aggregate.aggregateWith(future.get());
            }
            long finish = System.nanoTime();
            // Final output.
            System.out.printf("Time taken = %s ms %n", TimeUnit.NANOSECONDS.toMillis(finish - start));
            System.out.println(aggregate.getNodes());
            System.out.close();
        }
    }

    private static ExecutorService getExecutorService(Args parsingArgs) {
        return parsingArgs.virtualThreads ? Executors.newVirtualThreadPerTaskExecutor() : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    private static class Args {

        private boolean virtualThreads = true;
        private int batchSize = 500;

        @Override
        public String toString() {
            return format("Args[virtual=%s, batchSize=%s]", virtualThreads, batchSize);
        }
    }

}
