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


package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.hash.ReduceHashTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.util.List;

/**
 * Chained version of ReduceCombineDriver.
 */
public class ChainedReduceCombineDriver<T> extends ChainedDriver<T, T> {

	private static final Logger LOG = LoggerFactory.getLogger(ChainedReduceCombineDriver.class);

	private AbstractInvokable parent;

	private ReduceFunction<T> reducer;

	private ReduceHashTable<T> table;

	private List<MemorySegment> memory;

	// ------------------------------------------------------------------------

	@Override
	public Function getStub() {
		return reducer;
	}

	@Override
	public String getTaskName() {
		return taskName;
	}

	@Override
	public void setup(AbstractInvokable parent) {
		this.parent = parent;

		reducer = BatchTask.instantiateUserCode(config, userCodeClassLoader, ReduceFunction.class);
		FunctionUtils.setFunctionRuntimeContext(reducer, getUdfRuntimeContext());
	}

	@Override
	public void openTask() throws Exception {
		// open the stub first
		final Configuration stubConfig = config.getStubParameters();
		BatchTask.openUserCode(reducer, stubConfig);

		// instantiate the serializer / comparator
		TypeSerializer<T> serializer = config.<T>getInputSerializer(0, userCodeClassLoader).getSerializer();
		TypeComparator<T> comparator = config.<T>getDriverComparator(0, userCodeClassLoader).createComparator();

		MemoryManager memManager = parent.getEnvironment().getMemoryManager();
		final int numMemoryPages = memManager.computeNumberOfPages(config.getRelativeMemoryDriver());
		memory = memManager.allocatePages(parent, numMemoryPages);

		LOG.debug("ChainedReduceCombineDriver object reuse: " + (objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");

		table = new ReduceHashTable<T>(serializer, comparator, memory, reducer, outputCollector, objectReuseEnabled);
		table.open();
	}

	@Override
	public void collect(T record) {
		try {
			try {
				table.processRecordWithReduce(record);
			} catch (EOFException ex) {
				// the table has run out of memory
				table.emitAndReset();
				// try again
				table.processRecordWithReduce(record);
			}
		} catch (Exception ex2) {
			throw new ExceptionInChainedStubException(taskName, ex2);
		}
	}

	@Override
	public void close() {
		// send the final batch
		try {
			table.emit();
		} catch (Exception ex2) {
			throw new ExceptionInChainedStubException(taskName, ex2);
		}

		outputCollector.close();
	}

	@Override
	public void closeTask() throws Exception {
		table.close();
		parent.getEnvironment().getMemoryManager().release(memory);
		BatchTask.closeUserCode(reducer);
	}

	@Override
	public void cancelTask() {
		table.close();
		parent.getEnvironment().getMemoryManager().release(memory);
	}
}
