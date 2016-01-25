package malom;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemoryUtils;

/**
 * To identify a game state, we need to know where and what pieces are on the board,
 * and what sector are we in. (The latter is needed, because the number of pieces to placed can't be seen on the board.)
 *
 * Note: always white is to move. (See comment in SectorId.java)
 *
 * Note: One could have a model of the game, where the situations after closing a mill but before taking a stone
 * are considered game states as well. For example, the game GUI follows this approach, because there it clearly
 * fits better. But here, these situations are eliminated by considering the operation of closing a mill and then
 * taking a stone as _one_ move.
 */

public class GameState implements Comparable<GameState>, Serializable, KryoSerializable, IOReadableWritable {
	private static final long serialVersionUID = 1L;

	public SectorId sid;

	// There are 24 fields on the board, but it is stored in 48 bits.
	// The first 24 bits are for the white pieces, the next 24 are the blacks.
	public long board;


	public GameState(SectorId sid, long board) {
		this.sid = sid;
		this.board = board;
	}

	public GameState() {}

	@Override
	public int compareTo(GameState o) {
		if(!sid.equals(o.sid))
			return sid.compareTo(o.sid);
		else
			return ((Long)board).compareTo(o.board);
	}

	@Override
	public boolean equals(Object o0) {
		if(o0 instanceof GameState) {
			GameState o = (GameState)o0;
			return sid.equals(o.sid) && board == o.board;
		} else {
			return false;
		}
	}


	static final long mask24 = (1<<24) - 1;
	static final long mask8 = (1<<8) - 1;

	static private String toString24(long a) {
		return
				Long.toBinaryString((a>>16) & mask8) + " : " +
				Long.toBinaryString((a>>8) & mask8) + " : " +
				Long.toBinaryString(a & mask8);
	}

	static public GameState getNull(){
		return new GameState(SectorId.getNull(), -1);
	}

	@Override
	public String toString() {
		if(board != -1) {
			//return sid.toString() + " | " + Long.toBinaryString(board & mask24) + " | " + Long.toBinaryString(board >> 24);
			return sid.toString() + " | " + toString24(board & mask24) + " | " + toString24(board >> 24) + " [" + board + "]";
		} else {
			return "Nothing";
		}
	}

	@Override
	public int hashCode() {
		return (int)(sid.hashCode() * board % (1<<31));
	}

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeByte(sid.w);
		output.writeByte(sid.b);
		output.writeByte(sid.wf);
		output.writeByte(sid.bf);
		output.writeLong(board);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		sid = new SectorId(input.readByte(), input.readByte(), input.readByte(), input.readByte());
		board = input.readLong();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeByte(sid.w);
		out.writeByte(sid.b);
		out.writeByte(sid.wf);
		out.writeByte(sid.bf);
		out.writeLong(board);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.sid.w = in.readByte();
		this.sid.b = in.readByte();
		this.sid.wf = in.readByte();
		this.sid.bf = in.readByte();
		this.board = in.readLong();
	}

	static public class GameStateSerializer extends TypeSerializer<GameState> {
		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<GameState> duplicate() {
			return this;
		}

		@Override
		public GameState createInstance() {
			return GameState.getNull();
		}

		@Override
		public GameState copy(GameState from) {
			return new GameState(new SectorId(from.sid.w, from.sid.b, from.sid.wf, from.sid.bf), from.board);
		}

		@Override
		public GameState copy(GameState from, GameState reuse) {
			reuse.sid.w = from.sid.w;
			reuse.sid.b = from.sid.b;
			reuse.sid.wf = from.sid.wf;
			reuse.sid.bf = from.sid.bf;
			reuse.board = from.board;
			return reuse;
		}

		@Override
		public int getLength() {
			return 12;
		}

		@Override
		public void serialize(GameState record, DataOutputView target) throws IOException {
			target.writeByte(record.sid.w);
			target.writeByte(record.sid.b);
			target.writeByte(record.sid.wf);
			target.writeByte(record.sid.bf);
			target.writeLong(record.board);
		}

		@Override
		public GameState deserialize(DataInputView source) throws IOException {
			byte w = source.readByte();
			byte b = source.readByte();
			byte wf = source.readByte();
			byte bf = source.readByte();
			long board = source.readLong();
			return new GameState(new SectorId(w, b, wf, bf), board);
		}

		@Override
		public GameState deserialize(GameState reuse, DataInputView source) throws IOException {
			reuse.sid.w = source.readByte();
			reuse.sid.b = source.readByte();
			reuse.sid.wf = source.readByte();
			reuse.sid.bf = source.readByte();
			reuse.board = source.readLong();
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.write(source, getLength());
		}


		@Override
		public int hashCode() {
			return 42;
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof GameStateSerializer;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof GameStateSerializer;
		}
	}

}
