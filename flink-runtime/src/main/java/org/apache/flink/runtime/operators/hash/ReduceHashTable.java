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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.util.MathUtils;
import org.apache.flink.util.Collector;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * todo: comment on hash table structure
 *
 */

//todo: majd kell olyan teszt, hogy ossze-vissa valtozik a recordmeret (csokken is)

//todo: olyan teszt is kell, amikor nem adok neki eleg memoriat

//todo: vegig kene gondolni, hogy mikor dobunk exception-t, es mikor return value-val jelezzuk a memoria megteltet

//todo: compaction, ha kifogytunk a memoriabol

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

	/**
	 * The next pointer of a link will have this value, if it is not part of the linked list.
	 * (This can happen because the record was moved due to a size change.)
	 * Note: the record that is in the link should still be readable, in order to be possible to determine
	 * the size of the place.
	 */
	private static final long ABANDONED_RECORD = -2;

	/**
	 * This value means that the prevElemPtr is "pointing to the bucket", and not into the record segments.
	 */
	private static final long INVALID_PREV_POINTER = -3;

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
	private final ArrayList<MemorySegment> freeMemorySegments;

	private final int numAllMemorySegments;

	/**
	 * The size of the provided memoroy segments.
	 */
	private final int segmentSize, segmentSizeBits;

	/**
	 * These will contain the buckets.
	 * The buckets contain a pointer to the linked lists containing the actual records.
	 */
	private MemorySegment[] bucketSegments;

	private static final int bucketSize = 8, bucketSizeBits = 3;

	private int numBuckets; //todo: vigyazat, nehogy valami baj legyen abbol, hogy ez int! (azert int, hogy gyorsabb legyen a maradekos osztas)
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

	private long numElements = 0;

	private T reuse;

	private final HashTableProber<T> prober;


	public ReduceHashTable(TypeSerializer<T> serializer, TypeComparator<T> comparator, ReduceFunction<T> reducer,
						List<MemorySegment> memory, Collector<T> outputCollector, boolean objectReuseEnabled) {
		this.serializer = serializer;
		this.comparator = comparator;
		this.reducer = reducer;
		this.numAllMemorySegments = memory.size();
		this.freeMemorySegments = new ArrayList<>(memory);
		this.outputCollector = outputCollector;
		this.objectReuseEnabled = objectReuseEnabled;

		// some sanity checks first
		if (freeMemorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
			throw new IllegalArgumentException("Too few memory segments provided. ReduceHashTable needs at least " +
				MIN_NUM_MEMORY_SEGMENTS + " memory segments.");
		}

		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.segmentSize = freeMemorySegments.get(0).size();
		if ( (this.segmentSize & this.segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}
		this.segmentSizeBits = MathUtils.log2strict(this.segmentSize);

		this.numBucketsPerSegment = segmentSize / bucketSize;
		this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
		this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;

		//todo: Maybe calculate fraction from record size (and maybe make size a power of 2, to have faster divisions?)
		resetBucketSegments(calcInitialNumBucketSegments());

		ArrayList<MemorySegment> recordSegments = new ArrayList<>();
		recordSegments.add(allocateSegment()); //todo: esetleg mashogy megoldani, hogy ne durranjon el a konstruktor
		recordSegmentsInView = new RandomAccessInputView(recordSegments, this.segmentSize);
		recordSegmentsOutView = new AppendableRandomAccessOutputView(recordSegments, this.segmentSize);

		stagingSegments = new ArrayList<>();
		stagingSegments.add(allocateSegment());
		stagingSegmentsInView = new RandomAccessInputView(stagingSegments, this.segmentSize);
		stagingSegmentsOutView = new StagingOutputView(stagingSegments, this.segmentSize);

		reuse = serializer.createInstance();
		prober = new HashTableProber<>(comparator, new SameTypePairComparator<>(comparator));
	}

	private int calcInitialNumBucketSegments() {
		int recordLength = serializer.getLength();
		double fraction;
		if (recordLength == -1) {
			// It seems that resizing is quite efficient, so we can err here on the too few bucket segments side.
			// Even with small records, we lose only ~15% speed.
			fraction = 0.1;
		} else {
			fraction = 8.0 / (16 + recordLength);
		}
		return Math.max(1, (int)(numAllMemorySegments * fraction));
	}

	private void resetBucketSegments(int numBucketSegments) {
		if (numBucketSegments < 1) {
			throw new RuntimeException("Bug in ReduceHashTable");
		}

		if (bucketSegments != null) {
			freeMemorySegments.addAll(Arrays.asList(bucketSegments));
		}

		if (numBuckets % numBucketsPerSegment != 0) {
			throw new RuntimeException("Bug in ReduceHashTable");
		}
		bucketSegments = new MemorySegment[numBucketSegments];
		for(int i = 0; i < bucketSegments.length; i++) {
			bucketSegments[i] = allocateSegment();
			// Init all pointers in all buckets to END_OF_LIST
			for(int j = 0; j < this.numBucketsPerSegment; j++) {
				bucketSegments[i].putLong(j << bucketSizeBits, END_OF_LIST);
			}
		}
		numBuckets = numBucketSegments * numBucketsPerSegment;
	}

	private MemorySegment allocateSegment() {
		int s = this.freeMemorySegments.size();
		if (s > 0) {
			return this.freeMemorySegments.remove(s - 1);
		} else {
			return null;
		}
	}

	//todo: comment, es vegiggondolni, hogy kell-e egyaltalan ez; most csak azert raktam be, hogy performance a comparison fair legyen,
	// mert a HashTablePerformanceComparison-ben egymas utan jonnek a kulcsok
	private static int hash(int code) {
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		code = (code ^ 0xb55a4f09) ^ (code >>> 16);
		return code >= 0 ? code : -(code + 1);

//		return Math.abs(code);
	}

	/**
	 * Searches the hash table for the record with matching key, and updates it (makes one reduce step) if found,
	 * otherwise inserts a new entry.
	 *
	 * (If there are multiple entries with the same key, then it will update one of them.)
	 *
	 * @param record The record to be processed.
	 * @return A boolean that signals whether the operation succeded, or we have run out of memory.
	 */
	public boolean processRecordWithReduce(T record) throws Exception {
		T match = prober.getMatchFor(record, reuse);
		if (match == null) {
			return prober.insertAfterNoMatch(record);
		} else {
			// do the reduce step
			T res = reducer.reduce(match, record);

			// We have given this.reuse to the reducer UDF, so create new one if object reuse is disabled
			if (!objectReuseEnabled) {
				this.reuse = serializer.createInstance();
			}

			try {
				prober.updateMatch(res);
			} catch (IOException ex) {
				// We have run out of memory
				return false;
			}
			return true;
		}
	}

	/**
	 * Searches the hash table for a record with the given key.
	 * If it is found, then it is overridden with the specified record.
	 * Otherwise, the specified record is inserted.
	 * @param record The record to insert or to replace with.
	 * @throws IOException
     */
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
	 * @throws IOException
     */
	public void insert(T record) throws IOException {
		final int hashCode = hash(comparator.hash(record));
		final int bucket = hashCode % numBuckets;
		final int bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
		final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
		final int bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment
		final long firstPointer = bucketSegment.getLong(bucketOffset);

		final long newFirstPointer = recordSegmentsOutView.appendPointerAndRecord(firstPointer, record);
		bucketSegment.putLong(bucketOffset, newFirstPointer);

		numElements++;
		resizeTableIfNecessary();
	}

	private void resizeTableIfNecessary() {
		if (numElements > numBuckets) {
			final long newNumBucketSegments = 2L * bucketSegments.length;
			if (newNumBucketSegments < Integer.MAX_VALUE &&
				newNumBucketSegments - bucketSegments.length < freeMemorySegments.size() &&
				newNumBucketSegments < numAllMemorySegments / 2)
			resizeTable((int)newNumBucketSegments);
		}
	}

	/**
	 * Doubles the number of buckets.
	 */
	private void resizeTable(int newNumBucketSegments) {
		resetBucketSegments(newNumBucketSegments);
		rebuild();
	}

	/**
	 * This function reinitializes the bucket segments,
	 * reads all records from the record segments (sequentially, without using the pointers or the buckets),
	 * and rebuilds the hash table.
	 */
	private void rebuild() {
		try {
			recordSegmentsInView.setReadPosition(0);
			final long oldAppendPosition = recordSegmentsOutView.getAppendPosition();
			recordSegmentsOutView.resetAppendPosition();
			recordSegmentsOutView.setWritePosition(0);
			while (recordSegmentsInView.getReadPosition() < oldAppendPosition) {
				final boolean isAbandoned = recordSegmentsInView.readLong() == ABANDONED_RECORD;
				T record = serializer.deserialize(reuse, recordSegmentsInView);

				if (!isAbandoned) {
					// Insert into table

					final int hashCode = hash(comparator.hash(record));
					final int bucket = hashCode % numBuckets;
					final int bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
					final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
					final int bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment
					final long firstPointer = bucketSegment.getLong(bucketOffset);

					long ptrToAppended = recordSegmentsOutView.noSeekAppendPointerAndRecord(firstPointer, record);
					bucketSegment.putLong(bucketOffset, ptrToAppended);
				}
			}
			recordSegmentsOutView.freeSegmentsAfterAppendPosition();
		} catch (IOException ex) {
			throw new RuntimeException("Bug in ReduceHashTable");
		}
	}

	/**
	 * Emits all aggregates currently held by the table to the collector, and resets the table.
	 */
	public void emit() throws IOException {
		recordSegmentsInView.setReadPosition(0);
		final long oldAppendPosition = recordSegmentsOutView.getAppendPosition();
		while (recordSegmentsInView.getReadPosition() < oldAppendPosition) {
			final boolean isAbandoned = recordSegmentsInView.readLong() == ABANDONED_RECORD;
			T record = serializer.deserialize(reuse, recordSegmentsInView);
			if (!isAbandoned) {
				outputCollector.collect(record);
				if (!objectReuseEnabled) { //todo: vegiggondolni, hogy ez ide kell-e
					reuse = serializer.createInstance();
				}
			}
		}

		// reset the table

		recordSegmentsOutView.giveBackSegments(); // Note: recordSegmentsInView uses the same segments

		resetBucketSegments(bucketSegments.length);

		freeMemorySegments.addAll(stagingSegments);
		stagingSegments.clear();
		stagingSegments.add(allocateSegment());
	}


	/**
	 * An OutputView that
	 *  - can append a record
	 *  - can write a record to an arbitrary position (WARNING: the new record must not be larger than the old one)
	 *  - can be rewritten by calling resetAppendPosition
	 *  - takes memory from ReduceHashTable.freeMemorySegments on append
	 * WARNING: Do not call the write* methods of AbstractPagedOutputView directly when the write position is at the end,
	 * because appendPosition has to be modified when appending data.
	 */
	private final class AppendableRandomAccessOutputView extends AbstractPagedOutputView
	{
		private final ArrayList<MemorySegment> segments; //WARNING: recordSegmentsInView also has a ref to this object

		private final int segmentSizeBits;
		private final int segmentSizeMask;

		private int currentSegmentIndex;

		private long appendPosition = 0;


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
			if (position > appendPosition) {
				throw new IndexOutOfBoundsException();
			}

			final int segmentIndex = (int) (position >>> this.segmentSizeBits);
			final int offset = (int) (position & this.segmentSizeMask);

			// If position == appendPosition and the last buffer is full,
			// then we will be seeking to the beginning of a new segment
			if (segmentIndex == segments.size()) {
				addSegment();
			}

			this.currentSegmentIndex = segmentIndex;
			seekOutput(this.segments.get(segmentIndex), offset);
		}

		/**
		 * Moves all its memory segments to freeMemorySegments.
		 * Warning: this will be in an unwritable state after this call: you have to
		 * call setWritePosition before writing again.
		 */
		public void giveBackSegments() {
			freeMemorySegments.addAll(segments);
			segments.clear();

			resetAppendPosition();
		}

		/**
		 * Sets appendPosition and the write position to 0, so that appending starts
		 * overwriting elements from the beginning. (This is used in rebuild.)
		 * Note: if data was written to the area after the current appendPosition
		 * (before a call to resetAppendPosition), it should still be readable.
		 */
		public void resetAppendPosition() {
			appendPosition = 0;

			// this is just for safety (making sure that we fail immediately
			// if a write happens without calling setWritePosition)
			this.currentSegmentIndex = -1;
			seekOutput(null, -1);
		}

		public void freeSegmentsAfterAppendPosition() {
			final int appendSegmentIndex = (int)(appendPosition >>> this.segmentSizeBits);
			while (segments.size() > appendSegmentIndex + 1) {
				freeMemorySegments.add(segments.get(segments.size() - 1));
				segments.remove(segments.size() - 1);
			}
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
		 * @param recordSize The size of the record
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long appendPointerAndCopyRecord(long pointer, DataInputView input, int recordSize) throws IOException {
			setWritePosition(appendPosition);
			final long oldLastPosition = appendPosition;
			writeLong(pointer);
			write(input, recordSize);
			appendPosition += 8 + recordSize;
			return oldLastPosition;
		}

		/**
		 * Appends a pointer and a record.
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long appendPointerAndRecord(long pointer, T record) throws IOException {
			setWritePosition(appendPosition);
			return noSeekAppendPointerAndRecord(pointer, record);
		}

		/**
		 * Appends a pointer and a record. This function only works, if the write position is at the end,
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long noSeekAppendPointerAndRecord(long pointer, T record) throws IOException {
			final long oldLastPosition = appendPosition;
			final long oldPositionInSegment = getCurrentPositionInSegment();
			final long oldSegmentIndex = currentSegmentIndex;
			writeLong(pointer);
			serializer.serialize(record, this);
			appendPosition += getCurrentPositionInSegment() - oldPositionInSegment +
				getSegmentSize() * (currentSegmentIndex - oldSegmentIndex);
			return oldLastPosition;
		}

		public long getAppendPosition() {
			return appendPosition;
		}
	}


	private final class StagingOutputView extends AbstractPagedOutputView {

		private final ArrayList<MemorySegment> segments;

		private final int segmentSizeBits;

		private int currentSegmentIndex;


		public StagingOutputView(ArrayList<MemorySegment> segments, int segmentSize)
		{
			super(segmentSize, 0);
			this.segmentSizeBits = MathUtils.log2strict(segmentSize);
			this.segments = segments;
		}


		//todo: comment, illetve esetleg clear-t override-olni ehelyett
		public void reset() {
			seekOutput(segments.get(0), 0);
			currentSegmentIndex = 0;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
			if (++this.currentSegmentIndex == this.segments.size()) {
				if (freeMemorySegments.isEmpty()) {
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


	public <PT> HashTableProber<PT> getProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
		return new HashTableProber<>(probeTypeComparator, pairComparator);
	}

	public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T>{

		public HashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
			super(probeTypeComparator, pairComparator);
		}

		private int bucketSegmentIndex;
		private int bucketOffset;
		private long curElemPtr;
		private long prevElemPtr;
		private long nextPtr;

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
			final int bucket = hashCode % numBuckets;
			bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
			final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
			bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment

			curElemPtr = bucketSegment.getLong(bucketOffset);

			pairComparator.setReference(record);

			T currentRecordInList = targetForMatch;

			prevElemPtr = INVALID_PREV_POINTER;
			try {
				while (curElemPtr != END_OF_LIST) {
					recordSegmentsInView.setReadPosition(curElemPtr);
					nextPtr = recordSegmentsInView.readLong();

					currentRecordInList = serializer.deserialize(currentRecordInList, recordSegmentsInView);
					if (pairComparator.equalToReference(currentRecordInList)) {
						// we found an element with a matching key, and not just a hash collision
						return currentRecordInList;
					}

					prevElemPtr = curElemPtr;
					curElemPtr = nextPtr;
				}
			} catch (IOException ex) {
				throw new RuntimeException("Bug in ReduceHashTable", ex);
			}
			return null;
		}

		/**
		 * This method can be called after getMatchFor returned a match.
		 * It will overwrite the record that was found by getMatchFor.
		 * Warning: The new record should have the same key as the old!
		 * @param newRecord The record to override the old record with.
		 * @throws IOException
         */
		@Override
		public void updateMatch(T newRecord) throws IOException {
			if (curElemPtr == END_OF_LIST) {
				throw new RuntimeException("updateMatch was called after getMatchFor returned no match");
			}

			// determine the new size
			stagingSegmentsOutView.reset();
			serializer.serialize(newRecord, stagingSegmentsOutView);
			final int newRecordSize = (int)stagingSegmentsOutView.getWritePosition();
			stagingSegmentsInView.setReadPosition(0);

			// Determine the size of the place of the old record.
			final int oldRecordSize = (int)(recordSegmentsInView.getReadPosition() - (curElemPtr + RECORD_OFFSET_IN_LINK));

			if (newRecordSize == oldRecordSize) {
				// overwrite record at its original place
				recordSegmentsOutView.overwriteRecordAt(curElemPtr + RECORD_OFFSET_IN_LINK, stagingSegmentsInView, newRecordSize);
			} else {
				// new record has a different size than the old one, append new at end of recordSegments.
				// Note: we have to do this, even if the new record is smaller, because otherwise we wouldn't know the size of this
				// place during the compaction, and wouldn't know where does the next record start.

				//todo: ha megcsinalom a compaction-t, akkor itt kell majd valami jelzes a regi pointer helyere, hogy ez egy ures hely!
				// az ures hely meretet onnan tudom, hogy deserializalom, ami ott volt

				final long pointerToAppended =
					recordSegmentsOutView.appendPointerAndCopyRecord(nextPtr, stagingSegmentsInView, newRecordSize);

				// modify the pointer in the previous link
				if (prevElemPtr == INVALID_PREV_POINTER) {
					// list had only one element, so prev is in the bucketSegments
					bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
				} else {
					recordSegmentsOutView.overwriteLongAt(prevElemPtr, pointerToAppended);
				}

				recordSegmentsOutView.overwriteLongAt(curElemPtr, ABANDONED_RECORD);
			}
		}

		/**
		 * This method can be called after getMatchFor returned null.
		 * It inserts the given record to the hash table.
		 * Important: The given record should have the same key as the record that was given to getMatchFor!
		 */
		public boolean insertAfterNoMatch(T record) {
			// create new link
			long pointerToAppended;
			try {
				pointerToAppended = recordSegmentsOutView.appendPointerAndRecord(END_OF_LIST ,record);
			} catch (IOException ex) {
				return false; // we have run out of memory
			}

			// add new link to the end of the list
			if (prevElemPtr == INVALID_PREV_POINTER) {
				// list was empty
				bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
			} else {
				// update the pointer of the last element of the list.
				try {
					recordSegmentsOutView.overwriteLongAt(prevElemPtr, pointerToAppended);
				} catch (IOException ex) {
					throw new RuntimeException("Bug in ReduceHashTable");
				}
			}

			numElements++;
			resizeTableIfNecessary();

			return true;
		}
	}
}
