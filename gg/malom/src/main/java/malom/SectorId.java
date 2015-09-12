package malom;

import java.io.Serializable;

/**
 * The state space is subdivided into sectors. A sector is identified by the numbers of stones on the board for each
 * of the players, and the numbers of stones to be placed on the board for each of the players.
 *
 * Note, that you can "negate" a game state in the following sense: switch the colors of all stones on the board,
 * switch the number of stones to be placed, and switch the player-to-move. A game state and its negation are clearly
 * equivalent states, so we can eliminate one of them from our state space. This involves "normalizing" all game states
 * that pop up during the computation. (That is, when we see an eliminated state, we change it to the corresponding
 * non-eliminated state).
 * The question that remains, is which of the two corresponding states should be eliminated. One way would be to
 * have only states where white has at least as many stones on the board as black (Gasser's way (I think)).
 * But we have chosen to eliminate all the states where black is to move.
 */
public class SectorId implements Serializable, Comparable<SectorId> {
	private static final long serialVersionUID = 1L;

	// Number of stones for white and black on the board, and number of pices to be placed by the players.
	// For example the sector of the starting position in standard Mills is 0,0,9,9.
	public byte w, b, wf, bf;

	public SectorId(byte w, byte b, byte wf, byte bf) {
		this.w = w;
		this.b = b;
		this.wf = wf;
		this.bf = bf;
	}

	public SectorId(int w, int b, int wf, int bf) {
		this((byte)w, (byte)b, (byte) wf, (byte) bf);
	}

	public SectorId(SectorId o) {
		this.w = o.w;
		this.b = o.b;
		this.wf = o.wf;
		this.bf = o.bf;
	}

	public SectorId(){}

	public void negateInPlace(){
		byte tmp = w;
		w = b;
		b = tmp;

		tmp = wf;
		wf = bf;
		bf = tmp;
	}

	public SectorId negate(){
		SectorId r = new SectorId(this);
		r.negateInPlace();
		return r;
	}

	// All states are losses here
	public boolean isLosing() {
		return w + wf < 3; /////////////////////// 3
	}

	static public SectorId getNull() {
		return new SectorId(-1, -1, -1, -1);
	}

	// ESC: Equal Stone Count
	public boolean isEsc() {
		return equals(negate());
	}

	// A sector is transient, if all moves step out from it.
	// This is the case in standard and morabaraba placement phase.
	public boolean isTransient() {
		//#if VARIANT==STANDARD || VARIANT==MORABARABA
		return !(wf==0 && bf==0);
		//#else
		//return !(w!=0 && b!=0);
		//#endif
	}

	// ESC sectors don't have a twin, because they are the negations of themselves.
	// Moreover, transient sectors don't have a twin, because you can't move back and forth between transient sectors.
	public boolean hasTwin() {
		return !isEsc() && !isTransient();
	}

//	string file_name(){
//		char b[255];
//		sprintf_s(b, "%s_%d_%d_%d_%d.sec"FNAME_SUFFIX, VARIANT_NAME, w, b, wf, bf);
//		string r=string(b);
//		return r;
//	}

	@Override
	public int compareTo(SectorId o) {
		if(w != o.w)
			return ((Byte)w).compareTo(o.w);
		else if(b != o.b)
			return ((Byte)b).compareTo(o.b);
		else if(wf != o.wf)
			return ((Byte)wf).compareTo(o.wf);
		else
			return ((Byte)bf).compareTo(o.bf);
	}

	@Override
	public boolean equals(Object o0) {
		if(o0 instanceof SectorId) {
			SectorId o = (SectorId) o0;
			return w == o.w && b == o.b && wf == o.wf && bf == o.bf;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return w | (b << 4) | (wf << 8) | (bf << 12);
	}

	public String toString(){
		return String.format("%s_%d_%d_%d_%d", Config.variantName, w, b, wf, bf);
	}
}
