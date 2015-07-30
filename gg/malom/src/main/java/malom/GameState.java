package malom;

public class GameState implements Comparable<GameState> {
	public SectorId sid;
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

	@Override
	public String toString() {
		return sid.toString() + " " + Long.toBinaryString(board);
	}
}
