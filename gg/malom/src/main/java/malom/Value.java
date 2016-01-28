package malom;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

public class Value implements Serializable {
	private static final long serialVersionUID = 1L;

	public byte value; // can be LOSS or WIN (draws are represented by counts, see ValueCount)
	public short depth;

	public Value() {}

	public Value(int value, int depth) {
		this.value = (byte)value;
		this.depth = (short)depth;
	}

	public Value(Value o) {
		this.value = o.value;
		this.depth = o.depth;
	}

	static public byte LOSS = -1, WIN = 1;

	public boolean isWin() {
		return value == WIN;
	}

	public boolean isLoss() {
		return value == LOSS;
	}

	static public Value win(int depth) {
		return new Value(WIN, depth);
	}

	static public Value loss(int depth) {
		return new Value(LOSS, depth);
	}

	public Value undoNegate() {
		return new Value(-value, depth + 1);
	}

	static public Value getNull() {
		return new Value(-100, -100);
	}

	public boolean isNull() {
		return value == -100;
	}

	@Override
	public boolean equals(Object o0) {
		if(o0 instanceof Value) {
			Value o = (Value)o0;
			return value == o.value && depth == o.depth;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "Value{" +
				"value=" + value +
				", depth=" + depth +
				'}';
	}

	static public final class ValueSerializer extends TypeSerializer<Value> {
		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<Value> duplicate() {
			return this;
		}

		@Override
		public Value createInstance() {
			return new Value();
		}

		@Override
		public Value copy(Value from) {
			return new Value(from.value, from.depth);
		}

		@Override
		public Value copy(Value from, Value reuse) {
			reuse.value = from.value;
			reuse.depth = from.depth;
			return reuse;
		}

		@Override
		public int getLength() {
			return 3;
		}

		@Override
		public void serialize(Value record, DataOutputView target) throws IOException {
			target.writeByte(record.value);
			target.writeShort(record.depth);
		}

		@Override
		public Value deserialize(DataInputView source) throws IOException {
			return new Value(source.readByte(), source.readShort());
		}

		@Override
		public Value deserialize(Value reuse, DataInputView source) throws IOException {
			reuse.value = source.readByte();
			reuse.depth = source.readShort();
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.write(source, getLength());
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof ValueSerializer;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof ValueSerializer;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}
}
