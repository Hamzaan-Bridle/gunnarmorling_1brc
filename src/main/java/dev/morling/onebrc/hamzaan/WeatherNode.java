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

class WeatherNode {
    private long min;
    private long max;
    private long sum;
    private long numberOfEntries;

    WeatherNode(long min, long max, long sum, long numberOfEntries) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.numberOfEntries = numberOfEntries;
    }

    void addValue(long value) {
        if (value < this.min) {
            this.min = value;
        }
        if (value > this.max) {
            this.max = value;
        }
        this.sum += value;
        this.numberOfEntries++;
    }

    WeatherNode merge(WeatherNode other) {
        if (other.min < this.min) {
            this.min = other.min;
        }
        if (other.max > this.max) {
            this.max = other.max;
        }
        this.sum += other.sum;
        this.numberOfEntries += other.numberOfEntries;
        return this;
    }

    public String toString() {
        return round(((double) min) / 10.0)
                + "/" + round((((double) sum) / 10.0) / numberOfEntries)
                + "/" + round(((double) max) / 10.0);
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    static WeatherNode newNode() {
        return new WeatherNode(Long.MAX_VALUE, Long.MIN_VALUE, 0, 0);
    }

}
