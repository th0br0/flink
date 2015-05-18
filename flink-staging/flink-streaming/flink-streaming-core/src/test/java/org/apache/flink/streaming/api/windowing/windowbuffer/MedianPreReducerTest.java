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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windowbuffer.BasicWindowBufferTest.TestCollector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.junit.Test;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class MedianPreReducerTest {

	TypeInformation<Tuple2<Integer, Double>> typeInfo = TypeExtractor.getForObject(new Tuple2<Integer, Double>(1, 1.0));

	@SuppressWarnings("unchecked")
	@Test
	public void testMedianPreReducer() throws Exception {
		Random rnd = new Random(42);

		ArrayDeque<Tuple2<Integer, Double> > inputs = new ArrayDeque<Tuple2<Integer, Double> >();

		MedianPreReducer<Tuple2<Integer, Double>> preReducer =
				new MedianPreReducer<Tuple2<Integer, Double>>(1, typeInfo, null);
		TestCollector<StreamWindow<Tuple2<Integer, Double>>> collector =
				new TestCollector<StreamWindow<Tuple2<Integer, Double>>>();

		int N = 1000;
		for(int i = 0; i < N; i++) {
			switch (rnd.nextInt(3)) {
				case 0: // store
					Tuple2<Integer, Double> elem;
					// We sometimes add doubles and sometimes add small integers.
					// The latter is for ensuring that it happens that there are duplicate elements,
					// so that we test TreeMultiset properly.
					if(rnd.nextDouble() < 0.5) {
						elem = new Tuple2<Integer, Double>(i, (double)rnd.nextInt(5));
					} else {
						elem = new Tuple2<Integer, Double>(i, rnd.nextDouble());
					}
					inputs.addLast(elem);
					preReducer.store(elem);
					break;
				case 1: // evict
					int howMany = rnd.nextInt(2) + 1;
					int howMany2 = Math.min(howMany, inputs.size());
					for(int j = 0; j < howMany2; j++) {
						inputs.removeFirst();
					}
					preReducer.evict(howMany);
					break;
				case 2: // emitWindow
					if(inputs.size() > 0) {
						ArrayList<Double> inputDoubles = new ArrayList<Double>();
						for (Tuple2<Integer, Double> e : inputs) {
							inputDoubles.add((Double) (e.getField(1)));
						}
						Collections.sort(inputDoubles);
						int half = inputDoubles.size() / 2;
						Double correctMedian = inputDoubles.size() % 2 == 1 ?
								inputDoubles.get(half) :
								(inputDoubles.get(half) + inputDoubles.get(half - 1)) / 2;

						preReducer.emitWindow(collector);

						List<StreamWindow<Tuple2<Integer, Double>>> collected = collector.getCollected();
						assertEquals(
								StreamWindow.fromElements(
										new Tuple2<Integer, Double>((Integer) (inputs.peekLast().getField(0)),
												correctMedian)),
								collected.get(collected.size() - 1));
					}
					break;
			}
		}
	}
}
