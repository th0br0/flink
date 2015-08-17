package malom;

import java.io.Serializable;

public class Value implements Serializable {
	private static final long serialVersionUID = 1L;

	public byte value;
	public short depth;

	public Value() {}

	public Value(int value, int depth) {
		this.value = (byte)value;
		this.depth = (short)depth;
	}

	static public byte LOSS = -1, DRAW = 0, WIN = 1;

	public boolean isWin() {
		return value == WIN;
	}

	public boolean isLoss() {
		return value == LOSS;
	}

	public boolean isDraw() {
		return value == DRAW;
	}

	static public Value win(int depth) {
		return new Value(WIN, depth);
	}

	static public Value loss(int depth) {
		return new Value(LOSS, depth);
	}

	static public Value draw(int depth) {
		return new Value(DRAW, depth);
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
}
