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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class ReducePerformance {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setParallelism(1);

		@SuppressWarnings("unchecked")
		DataSet<Tuple2<Integer, Integer>> output =
			env.fromCollection(new TupleIntIntIterator(3 * 1000 * 1000, 1 * 1000 * 1000),
				TupleTypeInfo.<Tuple2<Integer, Integer>>getBasicTupleTypeInfo(Integer.class, Integer.class))
				.groupBy("0")
				.reduce(new SumReducer());

		output.writeAsCsv("/tmp/xxxobjectreusebug", "\n", " ", FileSystem.WriteMode.OVERWRITE);
		System.out.println(output.count() + "  (THIS NUMBER SHOULD NOT BE GREATER THAN 1000000)");
	}

	private static final class TupleIntIntIterator implements Iterator<Tuple2<Integer, Integer>>, Serializable {

		private int numElements;
		private final int keyRange;

		public TupleIntIntIterator(int numElements, int keyRange) {
			this.numElements = numElements;
			this.keyRange = keyRange;
		}

		private final Random rnd = new Random(123);

		@Override
		public boolean hasNext() {
			return numElements > 0;
		}

		@Override
		public Tuple2<Integer, Integer> next() {
			numElements--;
			Tuple2<Integer, Integer> ret = new Tuple2<Integer, Integer>();
			ret.f0 = rnd.nextInt(keyRange);
			ret.f1 = 1;
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}


	private static final class SumReducer<K> implements ReduceFunction<Tuple2<K, Integer>> {
		@Override
		public Tuple2<K, Integer> reduce(Tuple2<K, Integer> a, Tuple2<K, Integer> b) throws Exception {
			if (!a.f0.equals(b.f0)) {
				throw new RuntimeException("SumReducer was called with two record that have differing keys.");
			}
			a.f1 = a.f1 + b.f1;
			return a;
		}
	}
}
