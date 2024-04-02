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

import java.util.Queue;
import java.util.function.Supplier;

public class Aggregator implements Supplier<Aggregate> {

    private static final WeatherRecordParser PARSER = new WeatherRecordParser();

    private Queue<String> lines;
    private final Aggregate aggregate = new Aggregate();

    public Aggregator(Queue<String> lines) {
        this.lines = lines;
    }

    @Override
    public Aggregate get() {
        lines.stream().map(PARSER).forEach(aggregate::addRecord);
        lines = null; // Clear reference for garbage collection
        return aggregate;
    }
}
