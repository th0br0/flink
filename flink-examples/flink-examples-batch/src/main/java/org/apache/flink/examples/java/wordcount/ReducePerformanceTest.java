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

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

public class ReducePerformanceTest {
	
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//env.getConfig().enableObjectReuse();
		//env.setParallelism(4); ////

		UnsortedGrouping<Tuple2<Integer, Integer>> input =
			env.fromCollection(new RandomIterator(), TupleTypeInfo.<Tuple2<Integer, Integer>>getBasicTupleTypeInfo(Integer.class, Integer.class))
				.groupBy("0");

		DataSet<Tuple2<Integer, Integer>> output = input.reduce(new SumReducer());

		System.out.println(output.count());
	}

	private static final class RandomIterator implements Iterator<Tuple2<Integer, Integer>>, Serializable {

		private int i = 0;
		private Tuple2<Integer, Integer> reuse = new Tuple2<Integer, Integer>();
		Random rnd = new Random(43);

		@Override
		public boolean hasNext() {
			return i < 100*1000*1000;
		}

		@Override
		public Tuple2<Integer, Integer> next() {
			final int keyRange = 10*1000*1000;
			//final int valueRange = 1000000;

			reuse.f0 = rnd.nextInt(keyRange);
			//reuse.f1 = rnd.nextInt(valueRange);
			reuse.f1 = 1;

			i++;

			return reuse;
		}
	}

	private static final class SumReducer implements ReduceFunction<Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) throws Exception {
			if (!a.f0.equals(b.f0)) {
				throw new RuntimeException("SumReducer was called with two record that have differing keys.");
			}
			a.f1 = a.f1 + b.f1;
			return a;
		}
	}




//	public static void main(String[] args) throws Exception {
//
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		UnsortedGrouping<Tuple2<String, Integer>> input =
//			env.fromCollection(new RandomIterator(), TupleTypeInfo.<Tuple2<String, Integer>>getBasicTupleTypeInfo(String.class, Integer.class))
//				.groupBy("0");
//
//		DataSet<Tuple2<String, Integer>> output = input.reduce(new SumReducer());
//
//		System.out.println(output.count());
//	}
//
//	private static final class RandomIterator implements Iterator<Tuple2<String, Integer>>, Serializable {
//
//		private int i = 0;
//		private Tuple2<String, Integer> reuse = new Tuple2<>();
//		Random rnd = new Random(43);
//
//		@Override
//		public boolean hasNext() {
//			return i < 20*1000*1000;
//		}
//
//		@Override
//		public Tuple2<String, Integer> next() {
//			final int keyRange = 1000*1000*1000;
//
//			reuse.f0 = String.valueOf(rnd.nextInt(keyRange));
//			reuse.f1 = 1;
//
//			i++;
//
//			return reuse;
//		}
//	}
//
//	private static final class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
//		@Override
//		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
//			if (!a.f0.equals(b.f0)) {
//				throw new RuntimeException("SumReducer was called with two record that have differing keys.");
//			}
//			a.f1 = a.f1 + b.f1;
//			return a;
//		}
//	}



}
