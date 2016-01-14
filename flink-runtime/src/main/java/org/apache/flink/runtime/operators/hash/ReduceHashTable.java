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
import java.util.List;

/**
 * todo: comment on hash table structure
 *
 * (beleirni, hogy ha csokkent eg yrecord merete, akkor elvesztjuk az infot az eredeti meretrol, vagyis ha visszano,
 * akkor uj helyet fogunk foglalni)
 */

@SuppressWarnings("ForLoopReplaceableByForEach")
public class ReduceHashTable<T> {

	/**
	 * The minimum number of memory segments the hash join needs to be supplied with in order to work.
	 */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 3;

	/**
	 * The next pointer of the last link in the linked lists will have this as next pointer.
	 */
	private static final long END_OF_LIST = 1L<<62; // warning: because of the sign bit trickery, this can't be 0 or negative

	/**
	 * This value means that the prevPointer is "pointing to the bucket", and not into the record segments.
 	 */
	private static final long INVALID_PREV_POINTER = -2;

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

	private static final int bucketSize = 8, bucketSizeBits = 3;

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

	private HashTableProber<T> prober;


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
		bucketSegments = new MemorySegment[freeMemory.size() / 2];
		initBucketSegments(bucketSegments);
		this.numBuckets = bucketSegments.length * this.numBucketsPerSegment;

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

	private void initBucketSegments(MemorySegment[] segments) {
		for(int i = 0; i < segments.length; i++) {
			segments[i] = allocateSegment();
			// Init all pointers in all buckets to END_OF_LIST
			for(int j = 0; j < this.numBucketsPerSegment; j++) {
				segments[i].putLong(j << bucketSizeBits, END_OF_LIST);
			}
		}
	}

	private MemorySegment allocateSegment() {
		int s = this.freeMemory.size();
		if (s > 0) {
			return this.freeMemory.remove(s - 1);
		} else {
			return null;
		}
	}

	//todo: comment, es vegiggondolni, hogy kell-e egyaltalan ez; most csak azert raktam be, hogy performance comparsion fair legyen,
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
	 * If it is found, then it is overridden by the specified record.
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
	}

	/**
	 * Doubles the table size (the number of buckets)
	 */
	private void resizeTable() throws IOException {
		// We allocate a new array of bucket segments, that has the same size as the old,
		// move some of the records to the new segments, and then concatenate the two arrays.

		// vagy lehet, hogy ezt olyan vegigmenessel jobb lenne, mint amit a compact csinal?
		//	 nem tudom, hogy gyorsabb lenne-e, de talan tobb code reuse lehetne ugy

		MemorySegment[] newBucketSegments = new MemorySegment[this.bucketSegments.length];
		initBucketSegments(newBucketSegments);

		this.numBuckets *= 2;

		T record = reuse;
		for (int i = 0; i < bucketSegments.length; i++) {
			MemorySegment seg = bucketSegments[i];
			for (int j = 0; j < numBucketsPerSegment; j++) {
				long prevElemPointer = INVALID_PREV_POINTER;
				final int bucketOffset = j << bucketSizeBits;
				long curElemPointer = seg.getLong(bucketOffset);
				boolean hadSizeGrowth = false, prevHadSizeGrowth = false;
				while (curElemPointer != END_OF_LIST) {
					recordSegmentsInView.setReadPosition(curElemPointer);
					long nextElemPointer = recordSegmentsInView.readLong();
					prevHadSizeGrowth = hadSizeGrowth;
					hadSizeGrowth = nextElemPointer < 0;
					nextElemPointer = Math.abs(nextElemPointer);
					record = serializer.deserialize(record, recordSegmentsInView);

					final int hashCode = hash(this.comparator.hash(record));
					final int bucket = hashCode % this.numBuckets; // (numBuckets is the new number of buckets here)
					final int bucketSegmentIndex = bucket >>> this.numBucketsPerSegmentBits; // which segment should contain the bucket
					if (bucketSegmentIndex != j) {
						// Move link to the linked list of the new bucket.
						// Note, that since we doubled the number of buckets, the target index into newBucketSegments
						// is the same as the old index into bucketSegments.

						// Remove from old linked list
						if (prevElemPointer == INVALID_PREV_POINTER) {
							bucketSegments[bucketSegmentIndex].putLong(bucketOffset, nextElemPointer);
						} else {
							recordSegmentsOutView.overwriteLongAt(prevElemPointer, prevHadSizeGrowth ? -nextElemPointer : nextElemPointer);
						}

						// Add to the beginning of the other list
						final MemorySegment targetBucketSegment = newBucketSegments[i];
						final long oldFirstElemOfTargetListPointer = targetBucketSegment.getLong(bucketOffset);
						targetBucketSegment.putLong(bucketOffset, curElemPointer);
						recordSegmentsOutView.overwriteLongAt(curElemPointer, hadSizeGrowth ? -oldFirstElemOfTargetListPointer : oldFirstElemOfTargetListPointer);
					}

					prevElemPointer = curElemPointer;
					curElemPointer = nextElemPointer;
				}
			}
		}
	}

	/**
	 * Emits all aggregates currently held by the table to the collector, and resets the table.
	 */
	public void emit() throws IOException {

		//todo: ezt is a trukkos bejarassal kene, amit a compaction-nel csinalunk

		// go through all buckets and all linked lists, and emit all current partial aggregates
		for (int i = 0; i < bucketSegments.length; i++) {
			MemorySegment seg = bucketSegments[i];
			for (int j = 0; j < numBucketsPerSegment; j++) {
				long curElemPointer = seg.getLong(j << bucketSizeBits);
				while (curElemPointer != END_OF_LIST) {
					recordSegmentsInView.setReadPosition(curElemPointer);
					curElemPointer = Math.abs(recordSegmentsInView.readLong());
					reuse = serializer.deserialize(reuse, recordSegmentsInView);
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

		bucketSegments = new MemorySegment[freeMemory.size() / 2];
		initBucketSegments(bucketSegments);
		this.numBuckets = bucketSegments.length * this.numBucketsPerSegment;

		stagingSegments.add(allocateSegment());
	}


	/**
	 * An OutputView that
	 *  - can write a record to an arbitrary position (WARNING: the new record must not be larger than the old one)
	 *  - can append a record (with a specified pointer before it)
	 *  - takes memory from ReduceHashTable.freeMemory on append
	 * WARNING: Do not call the write* methods of AbstractPagedOutputView directly, because lastPosition has to be
	 * modified when appending data.
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
		 * @param recordSize The size of the record
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long appendPointerAndCopyRecordAndSkip(long pointer, DataInputView input, int recordSize, int skipSize) throws IOException {
			setWritePosition(lastPosition);
			final long oldLastPosition = lastPosition;
			writeLong(pointer);
			write(input, recordSize);
			skipBytesToWrite(skipSize);
			lastPosition += 8 + recordSize + skipSize;
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
			setWritePosition(lastPosition);
			final long oldLastPosition = lastPosition;
			final long oldPositionInSegment = getCurrentPositionInSegment();
			final long oldSegmentIndex = currentSegmentIndex;
			writeLong(pointer);
			serializer.serialize(record, this);
			lastPosition += getCurrentPositionInSegment() - oldPositionInSegment +
				getSegmentSize() * (currentSegmentIndex - oldSegmentIndex);
			return oldLastPosition;
		}

		/**
		 * Appends an END_OF_LIST pointer and a record.
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException
		 */
		public long appendNilPointerAndRecord(T record) throws IOException {
			return appendPointerAndRecord(END_OF_LIST, record);
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


		//todo: comment, illetve esetleg clear-t override-olni ehelyett
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


	public <PT> HashTableProber<PT> getProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
		return new HashTableProber<>(probeTypeComparator, pairComparator);
	}

	public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T>{

		public HashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
			super(probeTypeComparator, pairComparator);
		}

		private long prevPointer;
		private boolean prevHadSizeGrowth;
		private long nextPointer;
		private int bucketSegmentIndex;
		private int bucketOffset;
		private boolean hadSizeGrowth;
		private long currentPointer;

		//todo: comment
		//beleirni, hogy ha tobb azonos kulcsu van, akkor egyet ad csak vissza
		/**
		 *
		 * @param record
		 * @param targetForMatch
         * @return
         */
		@Override
		public T getMatchFor(PT record, T targetForMatch) {
			final int hashCode = hash(probeTypeComparator.hash(record));
			final int bucket = hashCode % numBuckets;
			bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
			final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
			bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment

			currentPointer = bucketSegment.getLong(bucketOffset);

			pairComparator.setReference(record);

			T currentRecordInList = targetForMatch;

			prevPointer = INVALID_PREV_POINTER;
			try {
				while (currentPointer != END_OF_LIST) {
					recordSegmentsInView.setReadPosition(currentPointer);
					nextPointer = recordSegmentsInView.readLong();
					// the sign bit of nextPointer stores whether we had a record size change at this key before
					prevHadSizeGrowth = hadSizeGrowth;
					hadSizeGrowth = nextPointer < 0;
					nextPointer = Math.abs(nextPointer);

					currentRecordInList = serializer.deserialize(currentRecordInList, recordSegmentsInView);
					if (pairComparator.equalToReference(currentRecordInList)) {
						// we found an element with a matching key, and not just a hash collision
						return currentRecordInList;
					}

					prevPointer = currentPointer;
					currentPointer = nextPointer;
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
			if (currentPointer == END_OF_LIST) {
				throw new RuntimeException("updateMatch was called after getMatchFor returned no match");
			}

			// determine the new size
			stagingSegmentsOutView.reset();
			serializer.serialize(newRecord, stagingSegmentsOutView);
			final int newRecordSize = (int)stagingSegmentsOutView.getWritePosition();
			stagingSegmentsInView.setReadPosition(0);

			// Determine the size of the place of the old record.
			// Note, that if we had a size growth before, then the size will have been rounded up to the nearest
			// power of 2.
			final int oldRecordSize = (int)(recordSegmentsInView.getReadPosition() - (currentPointer + RECORD_OFFSET_IN_LINK));
			final int oldRecordPlaceSize = hadSizeGrowth ? MathUtils.roundUpToPowerOf2(oldRecordSize) : oldRecordSize;

			if (newRecordSize <= oldRecordPlaceSize) {
				// overwrite record at its original place
				recordSegmentsOutView.overwriteRecordAt(currentPointer + RECORD_OFFSET_IN_LINK, stagingSegmentsInView, newRecordSize);
				// note: sign of next pointer in previous link is OK, no need to modify it
			} else {
				// new record doesn't fit in place of old, append new at end of recordSegments.

				// append new record and then skip bytes to round up the size of the place to the nearest power of 2
				final long pointerToAppended =
					recordSegmentsOutView.appendPointerAndCopyRecordAndSkip(-Math.abs(nextPointer), stagingSegmentsInView,
						newRecordSize, MathUtils.roundUpToPowerOf2(newRecordSize) - newRecordSize);

				// modify the pointer in the previous link
				if (prevPointer == INVALID_PREV_POINTER) {
					// list had only one element, so prev is in the bucketSegments
					bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
				} else {
					recordSegmentsOutView.overwriteLongAt(prevPointer, prevHadSizeGrowth ? -pointerToAppended : pointerToAppended);
				}
			}
		}

		/**
		 * This method can be called after getMatchFor returned null.
		 * It inserts the given record to the hash table.
		 * Important: The given record should have the same key as the record that was given to getMatchFor!
		 */
		public boolean insertAfterNoMatch(T record) throws IOException {
			if (currentPointer == END_OF_LIST) {
				throw new RuntimeException("insertAfterNoMatch was called after getMatchFor returned no match");
			}

			//todo: szamolni a load factort, es resizeTable

			// create new link
			long pointerToAppended;
			try {
				pointerToAppended = recordSegmentsOutView.appendNilPointerAndRecord(record);
			} catch (EOFException ex) {
				return false; // we have run out of memory
			}

			// add new link to the end of the list
			if (prevPointer == INVALID_PREV_POINTER) {
				// list was empty
				bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
			} else {
				// update the pointer of the last element of the list.
				// (we need to use hadSizeGrowth instead of prevHadSizeGrowth)
				recordSegmentsOutView.overwriteLongAt(prevPointer, hadSizeGrowth ? -pointerToAppended : pointerToAppended);
			}

			return true;
		}
	}
}
