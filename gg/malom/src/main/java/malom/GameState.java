package malom;

public class GameState implements Comparable<GameState> {
	public SectorId sid;
	public long board;

	public GameState(SectorId sid, long board) {
		this.sid = sid;
		this.board = board;
	}

	@Override
	public int compareTo(GameState o) {
		if(!sid.equals(o.sid))
			return sid.compareTo(o.sid);
		else
			return ((Long)board).compareTo(o.board);
	}
}
