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
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This hash table supports updating elements, and it also has processRecordWithReduce,
 * which makes one reduce step with the given record.
 *
 * The memory is divided into three areas:
 *  - Bucket area: they contain bucket heads:
 *    an 8 byte pointer to the first link of a linked list in the record area
 *  - Record area: this contains the actual data in linked list elements. A linked list element starts
 *    with an 8 byte pointer to the next element, and then the record follows.
 *  - Staging area: This is a small, temporary storage area for writing updated records. This is needed,
 *    because before serializing a record, there is no way to know in advance how large will it be.
 *    Therefore, we can't serialize directly into the record area when we are doing an update, because
 *    if it turns out to be larger then the old record, then it would override some other record
 *    that happens to be after the old one in memory. The solution is to serialize to the staging area first,
 *    and then copy it to the place of the original if it has the same size, otherwise allocate a new linked
 *    list element at the end of the record area, and mark the old one as abandoned.
 *
 *  Compaction happens by deleting everything in the bucket area, and then reinserting all elements.
 *  The reinsertion happens by forgetting the structure (the linked lists) of the record area, and reading it
 *  sequentially, and inserting all non-abandoned records, starting from the beginning of the record area.
 *  Note, that insertions never override a record that have not been read by the reinsertion sweep, because
 *  both the insertions and readings happen sequentially in the record area, and the insertions obviously
 *  never overtake the reading sweep.
 *
 *  Note: we have to abandon the old linked list element even when the updated record has a smaller size
 *  than the original, because otherwise we wouldn't know where the next record starts durign a reinsertion
 *  sweep.
 *
 *  The number of buckets depends on how large are the records. The serializer might be able to tell us this,
 *  so in this case, we will calculate the number of buckets upfront, and won't do resizes.
 *  If the serializer doesn't know the size, then we start with a small number of buckets, and do resizes as more
 *  elements are inserted than the number of buckets.
 *
 *  The number of memory segments given to the staging area is usually one, because it just needs to hold
 *  one record.
 */


//todo: majd meg kell gondolni, hogy akarjuk-e a ReducePerformanceTest-et berakni a git-be, vagy csak rakjam el magamnak mashova

//todo: a HashTablePerformanceComparison-ben mostmar ki lehetne emelni a ket AbstractMutableHashTable
//teszteleset egy fv-be, es a ket tesztbol kulonbozo parameterekkel meghivni
//(az fv-ben egy if lenne, hogy melyiket peldanyositsa)

//todo: kene egy teszt nagy recordokkal (azaz amikor pl. a staging area tobb mint egy segmentet foglal)

//todo: at kene nezni a CompactingHashTableTest-et, hatha van benne valami erdekes
	//vagy akar valahogy lehetne egyesiteni az en tesztemmel, hogy mindket osztalyt tesztelje egyszerre

@SuppressWarnings("ForLoopReplaceableByForEach")
public class ReduceHashTable<T> extends AbstractMutableHashTable<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ReduceHashTable.class);

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
	 * (This can happen because the record couldn't be u[dated in-place due to a size change.)
	 * Note: the record that is in the link should still be readable, in order to be possible to determine
	 * the size of the place.
	 * Note: the last record in the record area can't be abandoned. (EntryIterator makes use of this fact.)
	 */
	private static final long ABANDONED_RECORD = -2;

	/**
	 * This value means that the prevElemPtr is "pointing to the bucket", and not into the record segments.
	 */
	private static final long INVALID_PREV_POINTER = -3;

	private static final long RECORD_OFFSET_IN_LINK = 8;


	/** this is used by processRecordWithReduce */
	private final ReduceFunction<T> reducer;

	/** emit() sends data to outputCollector */
	private final Collector<T> outputCollector;

	private final boolean objectReuseEnabled;

	/**
	 * This initially contains all the memory we have, and then segments
	 * are taken from it by bucketSegments and recordArea.
	 */
	private final ArrayList<MemorySegment> freeMemorySegments;

	private final int numAllMemorySegments;

	private final int segmentSize;

	/**
	 * These will contain the buckets.
	 * The buckets contain a pointer to the linked lists containing the actual records.
	 */
	private MemorySegment[] bucketSegments;

	private static final int bucketSize = 8, bucketSizeBits = 3;

	private int numBuckets;
	private int numBucketsMask;
	private final int numBucketsPerSegment, numBucketsPerSegmentBits, numBucketsPerSegmentMask;

	/**
	 * The segments where the actual data is stored.
	 */
	private final RecordArea recordArea;

	/**
	 * Segments for the staging area.
	 * (It should contain at most one record at all times.)
	 */
	private final ArrayList<MemorySegment> stagingSegments;
	private final RandomAccessInputView stagingSegmentsInView;
	private final StagingOutputView stagingSegmentsOutView;

	private T reuse;

	private final HashTableProber<T> prober;

	private long numElements = 0;

	/** The number of bytes wasted by updates that couldn't overwrite the old record. */
	private long holes = 0;

	/**
	 * If the serializer knows the size of the records, then we can calculate the optimal number of buckets
	 * upfront, so we don't need resizes.
	 */
	private boolean enableResize;


	public ReduceHashTable(TypeSerializer<T> serializer, TypeComparator<T> comparator, ReduceFunction<T> reducer,
						List<MemorySegment> memory, Collector<T> outputCollector, boolean objectReuseEnabled) {
		super(serializer, comparator);
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

		// Get the size of the first memory segment and record it. All further buffers must have the same size.
		// the size must also be a power of 2
		segmentSize = freeMemorySegments.get(0).size();
		if ( (segmentSize & segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Hash Table requires buffers whose size is a power of 2.");
		}

		this.numBucketsPerSegment = segmentSize / bucketSize;
		this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
		this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;

		recordArea = new RecordArea(segmentSize);

		stagingSegments = new ArrayList<>();
		stagingSegments.add(allocateSegment());
		stagingSegmentsInView = new RandomAccessInputView(stagingSegments, segmentSize);
		stagingSegmentsOutView = new StagingOutputView(stagingSegments, segmentSize);

		prober = new HashTableProber<>(buildSideComparator, new SameTypePairComparator<>(buildSideComparator));

		enableResize = buildSideSerializer.getLength() == -1;
	}

	private void open(int numBucketSegments) {
		synchronized (stateLock) {
			if (!closed) {
				throw new IllegalStateException("currently not closed.");
			}
			closed = false;
		}

		allocateBucketSegments(numBucketSegments);

		stagingSegments.add(allocateSegment());

		reuse = buildSideSerializer.createInstance();
	}

	/**
	 * Initialize the hash table
	 */
	@Override
	public void open() {
		open(calcInitialNumBucketSegments());
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

		LOG.debug("Closing ReduceHashTable and releasing resources.");

		releaseBucketSegments();

		recordArea.giveBackSegments();

		freeMemorySegments.addAll(stagingSegments);
		stagingSegments.clear();

		numElements = 0;
		holes = 0;
	}

	@Override
	public void abort() {
		// ReduceHashTable doesn't have closed loops like CompactingHashTable.buildTableWithUniqueKey.
	}

	@Override
	public List<MemorySegment> getFreeMemory() {
		return freeMemorySegments;
	}

	private int calcInitialNumBucketSegments() {
		int recordLength = buildSideSerializer.getLength();
		double fraction;
		if (recordLength == -1) {
			// It seems that resizing is quite efficient, so we can err here on the too few bucket segments side.
			// Even with small records, we lose only ~15% speed.
			fraction = 0.1;
		} else {
			fraction = 8.0 / (16 + recordLength);
			// note: enableResize is false in this case, so no resizing will happen
		}

		int ret = Math.max(1, MathUtils.roundDownToPowerOf2((int)(numAllMemorySegments * fraction)));

		// We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return int)
		if ((long)ret * numBucketsPerSegment > Integer.MAX_VALUE) {
			ret = MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE / numBucketsPerSegment);
		}
		return ret;
	}

	private void allocateBucketSegments(int numBucketSegments) {
		if (numBucketSegments < 1) {
			throw new RuntimeException("Bug in ReduceHashTable");
		}

		bucketSegments = new MemorySegment[numBucketSegments];
		for(int i = 0; i < bucketSegments.length; i++) {
			bucketSegments[i] = allocateSegment();
			if (bucketSegments[i] == null) {
				throw new RuntimeException("Bug in ReduceHashTable: allocateBucketSegments should be " +
					"called in a way that there is enough free memory.");
			}
			// Init all pointers in all buckets to END_OF_LIST
			for(int j = 0; j < numBucketsPerSegment; j++) {
				bucketSegments[i].putLong(j << bucketSizeBits, END_OF_LIST);
			}
		}
		numBuckets = numBucketSegments * numBucketsPerSegment;
		numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
	}

	private void releaseBucketSegments() {
		freeMemorySegments.addAll(Arrays.asList(bucketSegments));
		bucketSegments = null;
	}

	private MemorySegment allocateSegment() {
		int s = freeMemorySegments.size();
		if (s > 0) {
			return freeMemorySegments.remove(s - 1);
		} else {
			return null;
		}
	}

	private static int hash(int code) {
		//todo: comment, es vegiggondolni, hogy kell-e egyaltalan ez; most csak azert raktam be, hogy a performance a comparison fair legyen,
		// mert a HashTablePerformanceComparison-ben egymas utan jonnek a kulcsok
		// A vegso valtozatban itt eleg lesz egyszeruen abszoluterteket venni, csak ahhoz akkor majd at kell irni a
		// HashTablePerformanceComparison-t valahogy.
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
	 * Searches the hash table for the record with matching key, and updates it (making one reduce step) if found,
	 * otherwise inserts a new entry.
	 *
	 * (If there are multiple entries with the same key, then it will update one of them.)
	 *
	 * @param record The record to be processed.
	 */
	public void processRecordWithReduce(T record) throws Exception {
		T match = prober.getMatchFor(record, reuse);
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
		final int bucket = hashCode & numBucketsMask;
		final int bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
		final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
		final int bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment
		final long firstPointer = bucketSegment.getLong(bucketOffset);

		try {
			final long newFirstPointer = recordArea.appendPointerAndRecord(firstPointer, record);
			bucketSegment.putLong(bucketOffset, newFirstPointer);
		} catch (EOFException ex) {
			compactOrThrow();
			insert(record);
			return;
		}

		numElements++;
		resizeTableIfNecessary();
	}

	//todo: comment
	private void resizeTableIfNecessary() throws IOException {
		if (enableResize && numElements > numBuckets) {
			final long newNumBucketSegments = 2L * bucketSegments.length;
			// Checks:
			// - we can't handle more than Integer.MAX_VALUE buckets
			// - don't take more memory than the free memory we have left
			// - the buckets shouldn't occupy more than half of all our memory
			if (newNumBucketSegments * numBucketsPerSegment < Integer.MAX_VALUE &&
				newNumBucketSegments - bucketSegments.length < freeMemorySegments.size() &&
				newNumBucketSegments < numAllMemorySegments / 2) {
				// do the resize
				rebuild(newNumBucketSegments);
			}
		}
	}

	public final class EntryIterator implements MutableObjectIterator<T> {

		private final long endPosition;

		public EntryIterator() {
			endPosition = recordArea.getAppendPosition();
			if (endPosition == 0) {
				return;
			}
			recordArea.setReadPosition(0);
		}

		@Override
		public T next(T reuse) throws IOException {
			if (endPosition != 0 && recordArea.getReadPosition() < endPosition) {
				// Loop until we find a non-abandoned record.
				// Note: the last record in the record area can't be abandoned.
				while (true) {
					final boolean isAbandoned = recordArea.readLong() == ABANDONED_RECORD;
					reuse = recordArea.readRecord(reuse);
					if (!isAbandoned) {
						return reuse;
					}
				}
			} else {
				return null;
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

	/**
	 * This function reinitializes the bucket segments,
	 * reads all records from the record segments (sequentially, without using the pointers or the buckets),
	 * and rebuilds the hash table.
	 */
	private void rebuild() throws IOException {
		rebuild(bucketSegments.length);
	}

	/** Same as above, but the number of bucket segments of the new table can be specified. */
	private void rebuild(long newNumBucketSegments) throws IOException {
		// Get new bucket segments
		releaseBucketSegments();
		allocateBucketSegments((int)newNumBucketSegments);

		T record = buildSideSerializer.createInstance();
		try {
			EntryIterator iter = getEntryIterator();
			recordArea.resetAppendPosition();
			recordArea.setWritePosition(0);
			while ((record = iter.next(record)) != null) {
				final int hashCode = hash(buildSideComparator.hash(record));
				final int bucket = hashCode & numBucketsMask;
				final int bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
				final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
				final int bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment
				final long firstPointer = bucketSegment.getLong(bucketOffset);

				long ptrToAppended = recordArea.noSeekAppendPointerAndRecord(firstPointer, record);
				bucketSegment.putLong(bucketOffset, ptrToAppended);
			}
			recordArea.freeSegmentsAfterAppendPosition();
			holes = 0;

		} catch (EOFException ex) {
			throw new RuntimeException("Bug in ReduceHashTable: we shouldn't get out of memory during a rebuild, " +
				"because we aren't allocating any new memory.");
		}
	}

	public void emitAndReset() throws IOException {
		final int oldNumBucketSegments = bucketSegments.length;
		emit();
		close();
		open(oldNumBucketSegments);
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

	/**
	 * If there is wasted space due to updates records not fitting in their old places, then do a compaction.
	 * Else, throw EOFException to indicate that memory ran out.
	 * @throws IOException
	 */
	private void compactOrThrow() throws IOException {
		if (holes > 0) {
			rebuild();
		} else {
			throw new EOFException();
		}
	}


	/**
	 * This class encapsulates the memory segments that belong to the record area. It
	 *  - can append a record
	 *  - can overwrite a record at an arbitrary position (WARNING: the new record must have the same size
	 *    as the old one)
	 *  - can be rewritten by calling resetAppendPosition
	 *  - takes memory from ReduceHashTable.freeMemorySegments on append
	 */
	private final class RecordArea
	{
		private final ArrayList<MemorySegment> segments = new ArrayList<>();

		private final OutputView outView;
		private final RandomAccessInputView inView;

		private final int segmentSizeBits;
		private final int segmentSizeMask;

		private long appendPosition = 0;


		private final class OutputView extends AbstractPagedOutputView {

			public int currentSegmentIndex;

			public OutputView(int segmentSize) {
				super(segmentSize, 0);
			}

			@Override
			protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws EOFException {
				currentSegmentIndex++;
				if (currentSegmentIndex == segments.size()) {
					addSegment();
				}
				return segments.get(currentSegmentIndex);
			}

			@Override
			public void seekOutput(MemorySegment seg, int position) {
				super.seekOutput(seg, position);
			}
		}


		public RecordArea(int segmentSize) {
			this(segmentSize, MathUtils.log2strict(segmentSize));
		}

		public RecordArea(int segmentSize, int segmentSizeBits) {

			if ((segmentSize & (segmentSize - 1)) != 0) {
				throw new IllegalArgumentException("Segment size must be a power of 2!");
			}

			this.segmentSizeBits = segmentSizeBits;
			this.segmentSizeMask = segmentSize - 1;

			outView = new OutputView(segmentSize);
			inView = new RandomAccessInputView(segments, segmentSize);
		}


		private void addSegment() throws EOFException {
			MemorySegment m = allocateSegment();
			if (m == null) {
				throw new EOFException();
			}
			segments.add(m);
		}

		/**
		 * Moves all its memory segments to freeMemorySegments.
		 * Warning: this will leave the RecordArea in an unwritable state: you have to
		 * call setWritePosition before writing again.
		 */
		public void giveBackSegments() {
			freeMemorySegments.addAll(segments);
			segments.clear();

			resetAppendPosition();
		}

		// ----------------------- Output -----------------------

		private void setWritePosition(long position) throws EOFException {
			if (position > appendPosition) {
				throw new IndexOutOfBoundsException();
			}

			final int segmentIndex = (int) (position >>> segmentSizeBits);
			final int offset = (int) (position & segmentSizeMask);

			// If position == appendPosition and the last buffer is full,
			// then we will be seeking to the beginning of a new segment
			if (segmentIndex == segments.size()) {
				addSegment();
			}

			outView.currentSegmentIndex = segmentIndex;
			outView.seekOutput(segments.get(segmentIndex), offset);
		}

		/**
		 * Sets appendPosition and the write position to 0, so that appending starts
		 * overwriting elements from the beginning. (This is used in rebuild.)
		 *
		 * Note: if data was written to the area after the current appendPosition
		 * before a call to resetAppendPosition, it should still be readable. To
		 * release the segments after the current append position, call
		 * freeSegmentsAfterAppendPosition()
		 */
		public void resetAppendPosition() {
			appendPosition = 0;

			// this is just for safety (making sure that we fail immediately
			// if a write happens without calling setWritePosition)
			outView.currentSegmentIndex = -1;
			outView.seekOutput(null, -1);
		}

		/**
		 * Releases the memory segments that are after the current append position.
		 * Note: The situation that there are segments after the current append position
		 * can arise from a call to resetAppendPosition().
		 */
		public void freeSegmentsAfterAppendPosition() {
			final int appendSegmentIndex = (int)(appendPosition >>> segmentSizeBits);
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
			outView.writeLong(value);
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
			outView.write(input, size);
		}

		/**
		 * Appends a pointer and a record. The record is read from a DataInputView (this will be the staging area).
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param input The DataInputView to read the record from
		 * @param recordSize The size of the record
		 * @return A pointer to the written data
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public long appendPointerAndCopyRecord(long pointer, DataInputView input, int recordSize) throws IOException {
			setWritePosition(appendPosition);
			final long oldLastPosition = appendPosition;
			outView.writeLong(pointer);
			outView.write(input, recordSize);
			appendPosition += 8 + recordSize;
			return oldLastPosition;
		}

		/**
		 * Appends a pointer and a record.
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public long appendPointerAndRecord(long pointer, T record) throws IOException {
			setWritePosition(appendPosition);
			return noSeekAppendPointerAndRecord(pointer, record);
		}

		/**
		 * Appends a pointer and a record. Call this function only if the write position is at the end!
		 * @param pointer The pointer to write (Note: this is NOT the position to write to!)
		 * @param record The record to write
		 * @return A pointer to the written data
		 * @throws IOException (EOFException specifically, if memory ran out)
		 */
		public long noSeekAppendPointerAndRecord(long pointer, T record) throws IOException {
			final long oldLastPosition = appendPosition;
			final long oldPositionInSegment = outView.getCurrentPositionInSegment();
			final long oldSegmentIndex = outView.currentSegmentIndex;
			outView.writeLong(pointer);
			buildSideSerializer.serialize(record, outView);
			appendPosition += outView.getCurrentPositionInSegment() - oldPositionInSegment +
				outView.getSegmentSize() * (outView.currentSegmentIndex - oldSegmentIndex);
			return oldLastPosition;
		}

		public long getAppendPosition() {
			return appendPosition;
		}

		// ----------------------- Input -----------------------

		public void setReadPosition(long position) {
			inView.setReadPosition(position);
		}

		public long getReadPosition() {
			return inView.getReadPosition();
		}

		public long readLong() throws IOException {
			return inView.readLong();
		}

		public T readRecord(T reuse) throws IOException {
			return buildSideSerializer.deserialize(reuse, inView);
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
			currentSegmentIndex++;
			if (currentSegmentIndex == segments.size()) {
				MemorySegment m = allocateSegment();
				if (m == null) {
					throw new EOFException();
				}
				segments.add(m);
			}
			return segments.get(currentSegmentIndex);
		}

		public long getWritePosition() {
			return (((long) currentSegmentIndex) << segmentSizeBits) + getCurrentPositionInSegment();
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
			final int bucket = hashCode & numBucketsMask;
			bucketSegmentIndex = bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
			final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
			bucketOffset = (bucket & numBucketsPerSegmentMask) << bucketSizeBits; // offset of the bucket in the segment

			curElemPtr = bucketSegment.getLong(bucketOffset);

			pairComparator.setReference(record);

			T currentRecordInList = targetForMatch;

			prevElemPtr = INVALID_PREV_POINTER;
			try {
				while (curElemPtr != END_OF_LIST) {
					recordArea.setReadPosition(curElemPtr);
					nextPtr = recordArea.readLong();

					currentRecordInList = recordArea.readRecord(currentRecordInList);
					if (pairComparator.equalToReference(currentRecordInList)) {
						// we found an element with a matching key, and not just a hash collision
						return currentRecordInList;
					}

					prevElemPtr = curElemPtr;
					curElemPtr = nextPtr;
				}
			} catch (IOException ex) {
				throw new RuntimeException("Error deserializing record from the hashtable: " + ex.getMessage(), ex);
			}
			return null;
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
			if (curElemPtr == END_OF_LIST) {
				throw new RuntimeException("updateMatch was called after getMatchFor returned no match");
			}

			try {
				// determine the new size
				stagingSegmentsOutView.reset();
				buildSideSerializer.serialize(newRecord, stagingSegmentsOutView);
				final int newRecordSize = (int)stagingSegmentsOutView.getWritePosition();
				stagingSegmentsInView.setReadPosition(0);

				// Determine the size of the place of the old record.
				final int oldRecordSize = (int)(recordArea.getReadPosition() - (curElemPtr + RECORD_OFFSET_IN_LINK));

				if (newRecordSize == oldRecordSize) {
					// overwrite record at its original place
					recordArea.overwriteRecordAt(curElemPtr + RECORD_OFFSET_IN_LINK, stagingSegmentsInView, newRecordSize);
				} else {
					// new record has a different size than the old one, append new at the end of the record area.
					// Note: we have to do this, even if the new record is smaller, because otherwise we wouldn't know
					// the size of this place during the compaction, and wouldn't know where does the next record start.

					final long pointerToAppended =
						recordArea.appendPointerAndCopyRecord(nextPtr, stagingSegmentsInView, newRecordSize);

					// modify the pointer in the previous link
					if (prevElemPtr == INVALID_PREV_POINTER) {
						// list had only one element, so prev is in the bucketSegments
						bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
					} else {
						recordArea.overwriteLongAt(prevElemPtr, pointerToAppended);
					}

					recordArea.overwriteLongAt(curElemPtr, ABANDONED_RECORD);

					holes += oldRecordSize;
				}
			} catch (EOFException ex) {
				compactOrThrow();
				insertOrReplaceRecord(newRecord);
			}
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
			// create new link
			long pointerToAppended;
			try {
				pointerToAppended = recordArea.appendPointerAndRecord(END_OF_LIST ,record);
			} catch (EOFException ex) {
				compactOrThrow();
				insert(record);
				return;
			}

			// add new link to the end of the list
			if (prevElemPtr == INVALID_PREV_POINTER) {
				// list was empty
				bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
			} else {
				// update the pointer of the last element of the list.
				recordArea.overwriteLongAt(prevElemPtr, pointerToAppended);
			}

			numElements++;
			resizeTableIfNecessary();
		}
	}
}
