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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.SameTypePairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.RandomAccessOutputView;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * todo: comment
 */

//todo: a HashTablePerformaceComparison-ban lehet, hogy megint gaz, hogy ugyanoylan a sorrend?

@SuppressWarnings("ForLoopReplaceableByForEach")
public final class OpenAddressingHashTable<T> extends AbstractMutableHashTable<T> {

	private static final Logger LOG = LoggerFactory.getLogger(OpenAddressingHashTable.class);

	/**
	 * The minimum number of memory segments the hash join needs to be supplied with in order to work.
	 */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 1;


	/** this is used by processRecordWithReduce */
	private final ReduceFunction<T> reducer;

	/** emit() sends data to outputCollector */
	private final Collector<T> outputCollector;

	private final boolean objectReuseEnabled;

	/** The total number of memory segments we have */
	private final int numSegments;

	private final int segmentSize;

	private final ArrayList<MemorySegment> segments;
	private final RandomAccessInputView inView;
	private final RandomAccessOutputView outView;

	private final int numBuckets;

	/** Points to one byte after the last bucket */
	private final long bucketsEnd;

	private final int bucketSize;

	private final int recordSize;

	private T reuse;

	private final HashTableProber<T> prober;

	private long numElements = 0;
	private final long maxElements;

	/**
	 * This constructor is for the case when will only call those operations that are also
	 * present on CompactingHashTable.
	 */
	public OpenAddressingHashTable(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) {
		this(serializer, comparator, memory, null, null, false);
	}

	public OpenAddressingHashTable(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory,
								   ReduceFunction<T> reducer, Collector<T> outputCollector, boolean objectReuseEnabled) {
		super(serializer, comparator);
		this.reducer = reducer;
		this.numSegments = memory.size();
		this.segments = new ArrayList<>(memory);
		this.outputCollector = outputCollector;
		this.objectReuseEnabled = objectReuseEnabled;

		// some sanity checks first
		this.recordSize = serializer.getLength();
		if (recordSize == -1) {
			throw new IllegalArgumentException("OpenAddressingHashTable can only be used, if the lengths of the " +
				"records are known.");
		}

		if (segments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. ReduceHashTable needs at least " +
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}

		bucketSize = recordSize + 1;

		// Get the size of the first memory segment and record it. All further buffers must have the same size.
		// the size must also be a power of 2
		segmentSize = segments.get(0).size();
		if ( (segmentSize & segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}

		numBuckets = numSegments * segmentSize / bucketSize;
		bucketsEnd = numBuckets * bucketSize;

		maxElements = (int)Math.floor(numBuckets * 0.7);
		if (maxElements < 1) {
			throw new IllegalArgumentException("Not enough memory given to OpenAddressingHashTable to store even one record.");
		}

		inView = new RandomAccessInputView(segments, segmentSize);

//		MemorySegment[] segmentArray = new MemorySegment[segments.size()];
//		for (int i = 0; i < segments.size(); i++) {
//			segmentArray[i] = segments.get(i);
//		}
//		outView = new RandomAccessOutputView(segmentArray, segmentSize);
		outView = new RandomAccessOutputView(segments, segmentSize);

		prober = new HashTableProber<>(buildSideComparator, new SameTypePairComparator<>(buildSideComparator));
	}

	public void open() {
		synchronized (stateLock) {
			if (!closed) {
				throw new IllegalStateException("currently not closed.");
			}
			closed = false;
		}

		try {
			outView.setWritePosition(0);
			for (int i = 0; i < numBuckets; i++) {
				outView.writeBoolean(false);
				outView.skipBytesToWrite(recordSize);
			}
		} catch (IOException ex) {
			throw new RuntimeException("Bug in OpenAddressingHashTable.");
		}

		reuse = buildSideSerializer.createInstance();

		for (int i = 0; i < bSize; i++) {
			bRecords[i] = buildSideSerializer.createInstance();
			seekPrefetches[i] = new RandomAccessInputView.SeekPrefetch();
			seekPrefetches2[i] = new RandomAccessInputView.SeekPrefetch();
		}
	}

	@Override
	public void close() {
		// make sure that we close only once
		synchronized (stateLock) {
			if (closed) {
				return;
			}
			closed = true;
		}

		LOG.debug("Closing OpenAdressingHashTable.");

		numElements = 0;

		bNum = 0;
	}

	@Override
	public void abort() {
		// OpenAddressingHashTable doesn't have closed loops like CompactingHashTable.buildTableWithUniqueKey.
	}

	@Override
	public List<MemorySegment> getFreeMemory() {
		if (!this.closed) {
			throw new IllegalStateException("Cannot return memory while ReduceHashTable is open.");
		}

		return segments;
	}

	//todo comment
	private static int hash(int code) {
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code >= 0 ? code : -(code + 1);
	}




	private static final int bSize = 32;
	//public int bSize = 8;

	private final Object[] bRecords = new Object[bSize];
	private int bNum = 0;
	private final RandomAccessInputView.SeekPrefetch[] seekPrefetches = new RandomAccessInputView.SeekPrefetch[bSize];
	private final RandomAccessInputView.SeekPrefetch[] seekPrefetches2 = new RandomAccessInputView.SeekPrefetch[bSize];

	/**
	 * Searches the hash table for the record with matching key, and updates it (making one reduce step) if found,
	 * otherwise inserts a new entry.
	 *
	 * (If there are multiple entries with the same key, then it will update one of them.)
	 *
	 * @param record The record to be processed.
	 */
	public void processRecordWithReduce(T record) throws Exception {
//		T match = prober.getMatchFor(record, reuse);
//		if (match == null) {
//			prober.insertAfterNoMatch(record);
//		} else {
//			// do the reduce step
//			T res = reducer.reduce(match, record);
//
//			// We have given reuse to the reducer UDF, so create new one if object reuse is disabled
//			if (!objectReuseEnabled) {
//				reuse = buildSideSerializer.createInstance();
//			}
//
//			prober.updateMatch(res);
//		}




		if (bNum == bSize) {
			processBatch();
		}

		bRecords[bNum] = buildSideSerializer.copy(record, (T)bRecords[bNum]);
		bNum++;
	}

	public byte dummy;

	private void processBatch() throws Exception {
		for (int i = 0; i < bSize; i++) {
			final int hashCode = hash(buildSideComparator.hash((T)bRecords[i]));
			final int bucketInd = hashCode % numBuckets;
			inView.prefetchSeek((long)bucketInd * bucketSize, seekPrefetches[i]);

//			if (bucketInd < numBuckets - 8) {
//				inView.prefetchSeek((long) bucketInd * bucketSize + 64, seekPrefetches2[i]);
//			} else {
//				inView.prefetchSeek((long) bucketInd * bucketSize, seekPrefetches2[i]);
//			}
		}

		for (int i = 0; i < bSize; i++) {
			inView.seekToPrefetched(seekPrefetches[i]);
			//dummy = inView.peakByteAfterSeek();
			inView.prefetchRead();

//			inView.seekToPrefetched(seekPrefetches2[i]);
//			inView.prefetchRead();
		}

		for (int i = 0; i < bSize; i++) {
			T record = (T)bRecords[i];
			T match = prober.getMatchForPrefetched(record, reuse, seekPrefetches[i]);
			if (match == null) {
				prober.insertAfterNoMatch(record);
			} else {
				// do the reduce step
				T res = reducer.reduce(match, record);

				// We have given reuse to the reducer UDF, so create new one if object reuse is disabled
				if (!objectReuseEnabled) {
					reuse = buildSideSerializer.createInstance();
				}

				prober.updateMatch(res);
			}
		}

		bNum = 0;
	}




	/**
	 * Searches the hash table for a record with the given key.
	 * If it is found, then it is overridden with the specified record.
	 * Otherwise, the specified record is inserted.
	 * @param record The record to insert or to replace with.
	 * @throws IOException (EOFException specifically, if memory ran out)
     */
	@Override
	public void insertOrReplaceRecord(T record) throws IOException {
		T match = prober.getMatchFor(record, reuse);
		if (match == null) {
			prober.insertAfterNoMatch(record);
		} else {
			prober.updateMatch(record);
		}
	}

	/**
	 * Inserts the given record into the hash table.
	 * Note: this method doesn't care about whether a record with the same key is already present.
	 * @param record The record to insert.
	 * @throws IOException (EOFException specifically, if memory ran out)
     */
	@Override
	public void insert(T record) throws IOException {
		final int hashCode = hash(buildSideComparator.hash(record));
		final int bucketInd = hashCode % numBuckets;

		inView.setReadPosition((long)bucketInd * bucketSize);

		// Look for an empty bucket
		while (inView.readBoolean()) {
			inView.skipBytesToRead(recordSize);
			if (inView.getReadPosition() == bucketsEnd) {
				// wraparound
				inView.setReadPosition(0);
			}
		}

		// Write the element
		outView.setWritePosition(inView.getReadPosition() - 1);
		outView.writeBoolean(true);
		buildSideSerializer.serialize(record, outView);

		numElements++;
	}

	public final class EntryIterator implements MutableObjectIterator<T> {

		public EntryIterator() {
			inView.setReadPosition(0);
		}

		@Override
		public T next(T reuse) throws IOException {
			try {
				while (inView.getReadPosition() < bucketsEnd) {
					if (inView.readBoolean()) {
						buildSideSerializer.deserialize(reuse, inView);
						return reuse;
					} else {
						inView.skipBytesToRead(recordSize);
					}
				}
				return null;
			} catch (EOFException ex) {
				throw new RuntimeException("Bug in OpenAddressingHashTable.");
			}
		}

		@Override
		public T next() throws IOException {
			return next(buildSideSerializer.createInstance());
		}
	}

	@Override
	public EntryIterator getEntryIterator() {
		return new EntryIterator();
	}

	public void emitAndReset() throws IOException {
		emit();
		close();
		open();
	}

	/**
	 * Emits all elements currently held by the table to the collector, and resets the table.
	 */
	public void emit() throws IOException {
		T record = buildSideSerializer.createInstance();
		EntryIterator iter = getEntryIterator();
		while ((record = iter.next(record)) != null) {
			outputCollector.collect(record);
			if (!objectReuseEnabled) {
				record = buildSideSerializer.createInstance();
			}
		}
	}


	public <PT> HashTableProber<PT> getProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
		return new HashTableProber<>(probeTypeComparator, pairComparator);
	}

	/**
	 * A prober for accessing the table.
	 * In addition to getMatchFor and updateMatch, it also has insertAfterNoMatch.
	 * Note: It is safe to interleave the function calls of different probers, since the state
	 * that is maintained between getMatchFor and the other methods is entirely contained in the
	 * prober, and not in the table.
	 * @param <PT> The type of the records that we are probing with
     */
	public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T>{

		public HashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
			super(probeTypeComparator, pairComparator);
		}

		private long matchPtr;

		/**
		 * Searches the hash table for the record with the given key.
		 * (If there are would be multiple matches, only one is returned.)
		 * @param record The record whose key we are searching for
		 * @param targetForMatch If a match is found, it will be written here
         * @return targetForMatch if a match is found, otherwise null.
         */
		@Override
		public T getMatchFor(PT record, T targetForMatch) {
			final int hashCode = hash(probeTypeComparator.hash(record));
			final int bucketInd = hashCode % numBuckets;

			inView.setReadPosition((long)bucketInd * bucketSize);

			pairComparator.setReference(record);

			try {
				while (true) {
					if (inView.readBoolean()) {
						targetForMatch = buildSideSerializer.deserialize(targetForMatch, inView);
						matchPtr = inView.getReadPosition();
						if (matchPtr == bucketsEnd) {
							// wraparound
							inView.setReadPosition(0);
							matchPtr = 0;
						}
						if (pairComparator.equalToReference(targetForMatch)) {
							// we found an element with a matching key, and not just a collision
							return targetForMatch;
						}
					} else {
						matchPtr = inView.getReadPosition() - 1; // the -1 is because we already read the bool
						return null;
					}
				}
			} catch (IOException ex) {
				throw new RuntimeException("Error deserializing record from the hashtable: " + ex.getMessage(), ex);
			}
		}

		public T getMatchForPrefetched(PT record, T targetForMatch, RandomAccessInputView.SeekPrefetch prefetch) {
			inView.seekToPrefetched(prefetch);

			pairComparator.setReference(record);

			try {
				while (true) {
					if (inView.readBoolean()) {
						targetForMatch = buildSideSerializer.deserialize(targetForMatch, inView);
						matchPtr = inView.getReadPosition();
						if (matchPtr == bucketsEnd) {
							// wraparound
							inView.setReadPosition(0);
							matchPtr = 0;
						}
						if (pairComparator.equalToReference(targetForMatch)) {
							// we found an element with a matching key, and not just a collision
							return targetForMatch;
						}
					} else {
						matchPtr = inView.getReadPosition() - 1; // the -1 is because we already read the bool
						return null;
					}
				}
			} catch (IOException ex) {
				throw new RuntimeException("Error deserializing record from the hashtable: " + ex.getMessage(), ex);
			}
		}

		/**
		 * This method can be called after getMatchFor returned a match.
		 * It will overwrite the record that was found by getMatchFor.
		 * Warning: The new record should have the same key as the old!
		 * WARNING; Don't do any modifications to the table between
		 * getMatchFor and updateMatch!
		 * @param newRecord The record to override the old record with.
		 * @throws IOException (EOFException specifically, if memory ran out)
         */
		@Override
		public void updateMatch(T newRecord) throws IOException {
			if (matchPtr == 0) {
				matchPtr = bucketsEnd; // Undo the wraparound
			}
			outView.setWritePosition(matchPtr - recordSize);
			buildSideSerializer.serialize(newRecord, outView);
		}

		/**
		 * This method can be called after getMatchFor returned null.
		 * It inserts the given record to the hash table.
		 * Important: The given record should have the same key as the record
		 * that was given to getMatchFor!
		 * WARNING; Don't do any modifications to the table between
		 * getMatchFor and insertAfterNoMatch!
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public void insertAfterNoMatch(T record) throws IOException {
			outView.setWritePosition(matchPtr);
			outView.writeBoolean(true);
			buildSideSerializer.serialize(record, outView);

			numElements++;
		}
	}

	private void checkLoadFactor() throws EOFException {
		if (numElements > maxElements) {
			throw new EOFException();
		}
	}
}
