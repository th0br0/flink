package malom;

public class Config {
	static String movegenFile = "/tmp/movegen";

	static final int maxPieceCount = 9;
	static final String variantName = "std";

	static final boolean filterSym = true; // Filter by symmetries of the board (reduces state space to almost 1/16)
}
