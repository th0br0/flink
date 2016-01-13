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

package org.apache.flink.runtime.operators.hash;

import com.google.common.collect.Ordering;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.testutils.UniformStringPairGenerator;
import org.apache.flink.runtime.operators.testutils.types.*;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ReduceHashTableTest {

	private static final long RANDOM_SEED = 58723953465322L;

	private static final int PAGE_SIZE = 16 * 1024;

	private class ReduceHashTableWithJavaHashMap<T, K> {

		TypeSerializer<T> serializer;

		TypeComparator<T> comparator;

		ReduceFunction<T> reducer;

		Collector<T> outputCollector;

		HashMap<K, T> map = new HashMap<>();

		public ReduceHashTableWithJavaHashMap(TypeSerializer<T> serializer, TypeComparator<T> comparator, ReduceFunction<T> reducer, Collector<T> outputCollector) {
			this.serializer = serializer;
			this.comparator = comparator;
			this.reducer = reducer;
			this.outputCollector = outputCollector;
		}

		public void processRecord(T record, K key) throws Exception {
			record = serializer.copy(record);

			if (!map.containsKey(key)) {
				map.put(key, record);
			} else {
				T x = map.get(key);
				x = reducer.reduce(x, record);
				map.put(key, x);
			}
		}

		public void emit() {
			for (T record: map.values()) {
				outputCollector.collect(record);
			}
			map.clear();
		}
	}

	@Test
	public void testWithIntPair() throws Exception {
		Random rnd = new Random(RANDOM_SEED);

		final int keyRange = 1000000; // varying this between 1000 and 1000000 can make a 5x speed difference (because of cache misses (also in the segment arrays))
		final int valueRange = 10;
		final int numRecords = 10000000;

		final IntPairSerializer serializer = new IntPairSerializer();
		final TypeComparator<IntPair> comparator = new IntPairComparator();
		final ReduceFunction<IntPair> reducer = new SumReducer();

		// Create the ReduceHashTableWithJavaHashMap, which will provide the correct output.
		List<IntPair> expectedOutput = new ArrayList<>();
		ReduceHashTableWithJavaHashMap<IntPair, Integer> reference = new ReduceHashTableWithJavaHashMap<>(
			serializer, comparator, reducer, new CopyingListCollector<>(expectedOutput, serializer));

		// Create the ReduceHashTable to test
		final int numMemPages = keyRange * 100 / PAGE_SIZE; // memory use should be proportional to the number of different keys
		List<IntPair> actualOutput = new ArrayList<>();
		ReduceHashTable<IntPair> table = new ReduceHashTable<>(
			serializer, comparator, reducer, getMemory(numMemPages, PAGE_SIZE), new CopyingListCollector<>(actualOutput, serializer), true);

		// Generate some input
		final List<IntPair> input = new ArrayList<>();
		for(int i = 0; i < numRecords; i++) {
			input.add(new IntPair(rnd.nextInt(keyRange), rnd.nextInt(valueRange)));
		}

		System.out.println("start"); //todo remove
		long start = System.currentTimeMillis();

		// Process the generated input
		final int numIntermingledEmits = 5;
		for (IntPair record: input) {
			table.processRecord(serializer.copy(record));
//			reference.processRecord(serializer.copy(record), record.getKey());
//			if(rnd.nextDouble() < 1.0 / ((double)numRecords / numIntermingledEmits)) {
//				// this will fire approx. numIntermingledEmits times
//				reference.emit();
//				table.emit();
//			}
		}
//		reference.emit();
//		table.emit();

		long end = System.currentTimeMillis();
		System.out.println("stop, time: " + (end - start)); //todo remove

		// Check results

		assertEquals(expectedOutput.size(), actualOutput.size());

		ArrayList<Integer> expectedValues = new ArrayList<>();
		for (IntPair record: expectedOutput) {
			expectedValues.add(record.getValue());
		}
		ArrayList<Integer> actualValues = new ArrayList<>();
		for (IntPair record: actualOutput) {
			actualValues.add(record.getValue());
		}
		expectedValues.sort(Ordering.<Integer>natural());
		actualValues.sort(Ordering.<Integer>natural());
		assertArrayEquals(expectedValues.toArray(), actualValues.toArray());
	}

	class SumReducer implements ReduceFunction<IntPair> {
		@Override
		public IntPair reduce(IntPair a, IntPair b) throws Exception {
			if (a.getKey() != b.getKey()) {
				throw new RuntimeException("SumReducer was called with two record that have differing keys.");
			}
			a.setValue(a.getValue() + b.getValue());
			return a;
		}
	}


	@Test
	public void testWithLengthChangingReduceFunction() throws Exception {
		Random rnd = new Random(RANDOM_SEED);

		final int numKeys = 10000;
		final int numVals = 10;
		final int numRecords = numKeys * numVals;

		StringPairSerializer serializer = new StringPairSerializer();
		StringPairComparator comparator = new StringPairComparator();
		ReduceFunction<StringPair> reducer = new ConcatReducer();

		// Create the ReduceHashTableWithJavaHashMap, which will provide the correct output.
		List<StringPair> expectedOutput = new ArrayList<>();
		ReduceHashTableWithJavaHashMap<StringPair, String> reference = new ReduceHashTableWithJavaHashMap<>(
			serializer, comparator, reducer, new CopyingListCollector<>(expectedOutput, serializer));

		// Create the ReduceHashTable to test
		final int numMemPages = numRecords * 100 / PAGE_SIZE;
		List<StringPair> actualOutput = new ArrayList<>();
		ReduceHashTable<StringPair> table = new ReduceHashTable<>(
			serializer, comparator, reducer, getMemory(numMemPages, PAGE_SIZE), new CopyingListCollector<>(actualOutput, serializer), true);

		// Process some manual stuff
		reference.processRecord(serializer.copy(new StringPair("foo", "bar")), "foo");
		reference.processRecord(serializer.copy(new StringPair("foo", "baz")), "foo");
		reference.processRecord(serializer.copy(new StringPair("alma", "xyz")), "alma");
		reference.processRecord(serializer.copy(new StringPair("korte", "abc")), "korte");
		table.processRecord(serializer.copy(new StringPair("foo", "bar")));
		table.processRecord(serializer.copy(new StringPair("foo", "baz")));
		table.processRecord(serializer.copy(new StringPair("alma", "xyz")));
		table.processRecord(serializer.copy(new StringPair("korte", "abc")));
		reference.emit();
		table.emit();

		// Generate some input
		UniformStringPairGenerator gen = new UniformStringPairGenerator(numKeys, numVals, true);
		List<StringPair> input = new ArrayList<>();
		StringPair cur = new StringPair();
		while (gen.next(cur) != null) {
			input.add(serializer.copy(cur));
		}
		Collections.shuffle(input, rnd);

		// Process the generated input
		final int numIntermingledEmits = 5;
		for (StringPair record: input) {
			reference.processRecord(serializer.copy(record), record.getKey());
			table.processRecord(serializer.copy(record));
			if(rnd.nextDouble() < 1.0 / ((double)numRecords / numIntermingledEmits)) {
				// this will fire approx. numIntermingledEmits times
				reference.emit();
				table.emit();
			}
		}
		reference.emit();
		table.emit();

		// Check results

		assertEquals(expectedOutput.size(), actualOutput.size());

		ArrayList<String> expectedValues = new ArrayList<>();
		for (StringPair record: expectedOutput) {
			expectedValues.add(record.getValue());
		}
		ArrayList<String> actualValues = new ArrayList<>();
		for (StringPair record: actualOutput) {
			actualValues.add(record.getValue());
		}
		expectedValues.sort(Ordering.<String>natural());
		actualValues.sort(Ordering.<String>natural());
		assertArrayEquals(expectedValues.toArray(), actualValues.toArray());
	}

	// Warning: Generally, reduce wouldn't give deterministic results with non-commutative ReduceFunction,
	// but ReduceHashTable and ReduceHashTableWithJavaHashMap calls it in the same order.
	class ConcatReducer implements ReduceFunction<StringPair> {
		@Override
		public StringPair reduce(StringPair a, StringPair b) throws Exception {
			if (a.getKey().compareTo(b.getKey()) != 0) {
				throw new RuntimeException("ConcatReducer was called with two record that have differing keys.");
			}
			return new StringPair(a.getKey(), a.getValue().concat(b.getValue()));
		}
	}


	private static List<MemorySegment> getMemory(int numPages, int pageSize) {
		List<MemorySegment> memory = new ArrayList<>();

		for (int i = 0; i < numPages; i++) {
			memory.add(MemorySegmentFactory.allocateUnpooledSegment(pageSize));
		}

		return memory;
	}
}
