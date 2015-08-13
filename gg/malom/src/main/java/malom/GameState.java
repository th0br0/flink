package malom;

import java.io.Serializable;

public class GameState implements Comparable<GameState>, Serializable {
	private static final long serialVersionUID = 1L;

	public SectorId sid;
	public long board; // 24 mezo van; az also 24 bit a feher korongok, a kovetkezo 24 bit a feketek

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
}
