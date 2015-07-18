/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.helper.Time;

import java.util.concurrent.TimeUnit;

public class PerfExp {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1); ///

		DataStream<Tuple2<Long, Long>> ds;
		ds = env.addSource(new InfSource());
		DataStream<Tuple2<Long, Long>> ds2 = ds
				.window(Time.of(1l, TimeUnit.SECONDS))
				//.every(Time.of(1l, TimeUnit.SECONDS))
				.sum(1)
				.flatten();
		ds2.print();

		env.execute("PerfExp");
	}


	static class InfSource implements SourceFunction<Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public InfSource() {

		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			Tuple2<Long, Long> record = new Tuple2<Long, Long>(0l, 1l);
			while (true) {
				//Thread.sleep(1);
				ctx.collect(record);
			}
		}

		@Override
		public void cancel() {}
	}
}
