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
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.util.MathUtils;
import org.apache.flink.util.Collector;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("ForLoopReplaceableByForEach")
public class ReduceHashTable<T> {

	/**
	 * The minimum number of memory segments the hash join needs to be supplied with in order to work.
	 */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 3;

	/**
	 * The next pointer of the last link in the linked lists will have this as next pointer.
	 */
	private static final long END_OF_LIST = -1;

	private static final long RECORD_OFFSET_IN_LINK = 8;


	private final TypeSerializer<T> serializer;

	private final TypeComparator<T> comparator;

	private final ReduceFunction<T> reducer;

	private final Collector<T> outputCollector;

	private final boolean objectReuseEnabled;

	/**
	 * This initially contains all the memory we have, and then segments
	 * are taken from it by bucketSegments and recordSegments.
	 */
	private final ArrayList<MemorySegment> freeMemory;

	/**
	 * The size of the provided memoroy segments.
	 */
	private final int segmentSize, segmentSizeBits;

	/**
	 * These will contain the buckets.
	 * The buckets contain a pointer to the linked lists containing the actual records.
	 */
	private MemorySegment[] bucketSegments;

	private final int bucketSize = 8, bucketSizeBits = 3;

	private int numBuckets;
	private final int numBucketsPerSegment, numBucketsPerSegmentBits, numBucketsPerSegmentMask;

	/**
	 * These views are for the segments that hold the actual records (in linked lists).
	 * (The ArrayList of the actual segments are inside recordSegmentsOutView.)
	 */
	private final RandomAccessInputView recordSegmentsInView;
	private final AppendableRandomAccessOutputView recordSegmentsOutView;

	/**
	 * These are the segments for the staging area, where we temporarily write a record after a reduce step,
	 * to see its size to see if it fits in the place of the old record, before writing it into recordSegments.
	 * (It should contain at most one record at all times.)
	 */
	private final ArrayList<MemorySegment> stagingSegments;
	private final RandomAccessInputView stagingSegmentsInView;
	private final StagingOutputView stagingSegmentsOutView;

	private T reuse;


	public ReduceHashTable(TypeSerializer<T> serializer, TypeComparator<T> comparator, ReduceFunction<T> reducer,
						List<MemorySegment> memory, Collector<T> outputCollector, boolean objectReuseEnabled) {
		this.serializer = serializer;
		this.comparator = comparator;
		this.reducer = reducer;
		this.freeMemory = new ArrayList<>(memory);
		this.outputCollector = outputCollector;
		this.objectReuseEnabled = objectReuseEnabled;

		// some sanity checks first
		if (freeMemory.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. ReduceHashTable needs at least " +
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}

		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.segmentSize = freeMemory.get(0).size();
		if ( (this.segmentSize & this.segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}
		this.segmentSizeBits = MathUtils.log2strict(this.segmentSize);

		this.numBucketsPerSegment = segmentSize / bucketSize;
		this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
		this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;

		//todo: Calculate fraction from record size (and maybe make size a power of 2, to have faster divisions?)
		initBucketSegments(freeMemory.size() / 2);

		ArrayList<MemorySegment> recordSegments = new ArrayList<>();
		recordSegments.add(allocateSegment()); //todo: esetleg mashogy megoldani, hogy ne durranjon el a konstruktor
		recordSegmentsInView = new RandomAccessInputView(recordSegments, this.segmentSize);
		recordSegmentsOutView = new AppendableRandomAccessOutputView(recordSegments, this.segmentSize);

		stagingSegments = new ArrayList<>();
		stagingSegments.add(allocateSegment());
		stagingSegmentsInView = new RandomAccessInputView(stagingSegments, this.segmentSize);
		stagingSegmentsOutView = new StagingOutputView(stagingSegments, this.segmentSize);

		reuse = serializer.createInstance();
	}

	private void initBucketSegments(int num) {
		bucketSegments = new MemorySegment[num];
		for(int i = 0; i < bucketSegments.length; i++) {
			bucketSegments[i] = allocateSegment();
			// Init the recordSegment of all buckets to END_OF_LIST
			for(int j = 0; j < this.numBucketsPerSegment; j++) {
				bucketSegments[i].putLong(j << this.bucketSizeBits, END_OF_LIST);
			}
		}

		this.numBuckets = bucketSegments.length * this.numBucketsPerSegment;
	}

	private MemorySegment allocateSegment() {
		int s = this.freeMemory.size();
		if (s > 0) {
			return this.freeMemory.remove(s - 1);
		} else {
			return null;
		}
	}

	/**
	 * Searches the hash table for the record with matching key, and updates it (makes one reduce step) if found,
	 * otherwise inserts a new entry.
	 *
	 * @param record The record to be processed.
	 * @return A boolean that signals whether the operation succeded
	 */
	public boolean processRecord(T record) throws Exception {
		final int hashCode = Math.abs(this.comparator.hash(record));
		final int bucket = hashCode % this.numBuckets;
		final int bucketSegmentIndex = bucket >>> this.numBucketsPerSegmentBits; // which segment contains the bucket
		final int bucketOffset = (bucket & this.numBucketsPerSegmentMask) << this.bucketSizeBits; // offset of the bucket in the segment

		this.comparator.setReference(record);

		// This value means that the prevPointer is "pointing to the bucket".
		final long INVALID_PREV_POINTER = -2;

		T currentRecordInList = this.reuse;

		long currentPointer = bucketSegments[bucketSegmentIndex].getLong(bucketOffset);
		long prevPointer = INVALID_PREV_POINTER;
		while (currentPointer != END_OF_LIST) {
			recordSegmentsInView.setReadPosition(currentPointer + RECORD_OFFSET_IN_LINK);
			currentRecordInList = this.serializer.deserialize(currentRecordInList, recordSegmentsInView);
			if (this.comparator.equalToReference(currentRecordInList)) {
				// we found an element with a matching key, and not just a hash collision
				break;
			}

			prevPointer = currentPointer;
			recordSegmentsInView.setReadPosition(currentPointer);
			currentPointer = recordSegmentsInView.readLong();
		}

		if (currentPointer == END_OF_LIST) {
			// linked list didn't contain the key, append

			//todo: szamolni a load factort, es resizeTable

			// create new link
			long pointerToAppended;
			try {
				pointerToAppended = recordSegmentsOutView.appendNilPointerAndRecord(record);
			} catch (EOFException ex) {
				return false;
			}

			// add new link to the list
			if (prevPointer == INVALID_PREV_POINTER) {
				// list was empty
				bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
			} else {
				recordSegmentsOutView.overwriteLongAt(prevPointer, pointerToAppended);
			}
		} else {
			// linked list contains the key; overwrite or remove and append new (if larger)

			//todo: duplazas
			// a 8 byte-os pointer elojel bitjeben tarolom, hogy volt-e mar noveles ezen a rekordon, es ha volt,
			// akkor mindenkeppen kettohatvanyra lesz folfele kerekitve

			final long oldRecordSize = recordSegmentsInView.getReadPosition() - (currentPointer + RECORD_OFFSET_IN_LINK);

			T res = reducer.reduce(currentRecordInList, record);

			stagingSegmentsOutView.reset();
			serializer.serialize(res, stagingSegmentsOutView);
			final int newRecordSize = (int)stagingSegmentsOutView.getWritePosition();
			stagingSegmentsInView.setReadPosition(0);
			if (newRecordSize <= oldRecordSize) {
				// overwrite record at its original place
				recordSegmentsOutView.overwriteRecordAt(currentPointer + RECORD_OFFSET_IN_LINK, stagingSegmentsInView, newRecordSize);
			} else {
				// new record doesn't fit in place of old. Append new at end of recordSegments, and modify the linked list
				recordSegmentsInView.setReadPosition(currentPointer);
				final long nextPointer = recordSegmentsInView.readLong();
				final long pointerToAppended =
					recordSegmentsOutView.appendPointerAndCopyRecord(nextPointer, stagingSegmentsInView, newRecordSize);
				if (prevPointer == INVALID_PREV_POINTER) {
					// list had only one element, so prev is in the bucketSegments
					bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
				} else {
					recordSegmentsOutView.overwriteLongAt(prevPointer, pointerToAppended);
				}
			}

			// We have given this.reuse to the reducer UDF, so create new one if object reuse is disabled
			if (!objectReuseEnabled) {
				this.reuse = serializer.createInstance();
			}
		}

		return true;
	}

	/**
	 * Emits all aggregates currently held by the table to the collector, and resets the table.
	 */
	public void emit() throws IOException {

		// go through all buckets and all linked lists, and emit all current partial aggregates
		for (int i = 0; i < bucketSegments.length; i++) {
			MemorySegment seg = bucketSegments[i];
			for (int j = 0; j < numBucketsPerSegment; j++) {
				long curElemPointer = seg.getLong(j << this.bucketSizeBits);
				while (curElemPointer != END_OF_LIST) {
					recordSegmentsInView.setReadPosition(curElemPointer);
					curElemPointer = recordSegmentsInView.readLong();
					serializer.deserialize(reuse, recordSegmentsInView);
					outputCollector.collect(reuse);
				}
			}
		}

		// reset the table

		int n = bucketSegments.length;
		for (int i = 0; i < n; i++) {
			freeMemory.add(bucketSegments[i]);
		}
		recordSegmentsOutView.giveBackSegments();
		freeMemory.addAll(stagingSegments);
		stagingSegments.clear();

		initBucketSegments(freeMemory.size() / 2);

		stagingSegments.add(allocateSegment());
	}


	/**
	 * An OutputView that
	 *  - can write a record to an arbitrary position (WARNING: the new record must not be larger than the old one)
	 *  - can append a record (with a specified pointer before it)
	 *  - takes memory from ReduceHashTable.freeMemory on append
	 */
	public class AppendableRandomAccessOutputView extends AbstractPagedOutputView
	{
		private final ArrayList<MemorySegment> segments;

		private int currentSegmentIndex;

		private final int segmentSizeBits;

		private final int segmentSizeMask;

		private long lastPosition = 0;


		public AppendableRandomAccessOutputView(ArrayList<MemorySegment> segments, int segmentSize) {
			this(segments, segmentSize, MathUtils.log2strict(segmentSize));
		}

		public AppendableRandomAccessOutputView(ArrayList<MemorySegment> segments, int segmentSize, int segmentSizeBits) {
			super(segmentSize, 0);

			if ((segmentSize & (segmentSize - 1)) != 0) {
				throw new IllegalArgumentException("Segment size must be a power of 2!");
			}

			this.segments = segments;
			this.segmentSizeBits = segmentSizeBits;
			this.segmentSizeMask = segmentSize - 1;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
			this.currentSegmentIndex++;
			if (this.currentSegmentIndex == this.segments.size()) {
				addSegment();
			}
			return this.segments.get(this.currentSegmentIndex);
		}

		private void addSegment() throws EOFException {
			MemorySegment m = allocateSegment();
			if (m == null) {
				throw new EOFException();
			}
			this.segments.add(m);
		}

		private void setWritePosition(long position) throws EOFException {
			if (position > lastPosition) {
				throw new IndexOutOfBoundsException();
			}

			final int bufferNum = (int) (position >>> this.segmentSizeBits);
			final int offset = (int) (position & this.segmentSizeMask);

			// If position == lastPosition and the last buffer is full,
			// then we will be seeking to the beginning of a new segment
			if (bufferNum == segments.size()) {
				addSegment();
			}

			this.currentSegmentIndex = bufferNum;
			seekOutput(this.segments.get(bufferNum), offset);
		}

		/**
		 * Moves all its memory segments to freeMemory.
		 * Warning: this will be in an unwritable state after this call: you have to
		 * call setWritePosition before writing again.
		 */
		public void giveBackSegments() {
			freeMemory.addAll(segments);
			segments.clear();

			lastPosition = 0;

			// this is just for sefety (making sure that we fail immediately
			// if a write happens without calling setWritePosition)
			this.currentSegmentIndex = -1;
			seekOutput(null, -1);
		}

		/**
		 * Overwrites the long value at the specified position.
		 * @param pointer Points to the position to overwrite.
		 * @param value The value to write.
		 * @throws IOException
		 */
		public void overwriteLongAt(long pointer, long value) throws IOException {
			setWritePosition(pointer);
			writeLong(value);
		}

		/**
		 * Overwrites a record at the sepcified position. The record is read from a DataInputView  (this will be the staging area).
		 * WARNING: The record must not be larger than the original record.
		 * @param pointer Points to the position to overwrite.
		 * @param input The DataInputView to read the record from
		 * @param size The size of the record
		 * @throws IOException
		 */
		public void overwriteRecordAt(long pointer, DataInputView input, int size) throws IOException {
			setWritePosition(pointer);
			write(input, size);
		}

		/**
		 * Appends a pointer and a record. The record is read from a DataInputView (this will be the staging area).
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param input The DataInputView to read the record from
		 * @param size The size of the record
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long appendPointerAndCopyRecord(long pointer, DataInputView input, int size) throws IOException {
			setWritePosition(lastPosition);
			final long oldLastPosition = lastPosition;
			writeLong(pointer);
			write(input, size);
			lastPosition += 8 + size;
			return oldLastPosition;
		}

		/**
		 * Appends an END_OF_LIST pointer and a record.
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long appendNilPointerAndRecord(T record) throws IOException {
			setWritePosition(lastPosition);
			final long oldLastPosition = lastPosition;
			final long oldPositionInSegment = getCurrentPositionInSegment();
			final long oldSegmentIndex = currentSegmentIndex;
			writeLong(END_OF_LIST);
			serializer.serialize(record, this);
			lastPosition += getCurrentPositionInSegment() - oldPositionInSegment +
				getSegmentSize() * (currentSegmentIndex - oldSegmentIndex);
			return oldLastPosition;
		}
	}


	public class StagingOutputView extends AbstractPagedOutputView {

		private final ArrayList<MemorySegment> segments;

		private final int segmentSizeBits;

		private int currentSegmentIndex;


		public StagingOutputView(ArrayList<MemorySegment> segments, int segmentSize)
		{
			super(segmentSize, 0);
			this.segmentSizeBits = MathUtils.log2strict(segmentSize);
			this.segments = segments;
		}


		public void reset() {
			seekOutput(segments.get(0), 0);
			currentSegmentIndex = 0;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
			if (++this.currentSegmentIndex == this.segments.size()) {
				if (freeMemory.isEmpty()) {
					throw new EOFException();
				} else {
					this.segments.add(allocateSegment());
				}
			}
			return this.segments.get(this.currentSegmentIndex);
		}

		public long getWritePosition() {
			return (((long) this.currentSegmentIndex) << this.segmentSizeBits) + getCurrentPositionInSegment();
		}
	}

}
