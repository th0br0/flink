package malom;

import java.io.Serializable;
import java.util.Iterator;

// Iterates over all elements in a sector.
public class SectorElementIterator implements Iterator<GameState>, Serializable {
	private static final long serialVersionUID = 1L;

	final SectorId s;

	int w, bc;
	final int end;

	SectorElementIterator(SectorId s) {
		this.s = s;
		w = (1<<s.w) - 1;
		bc = (1<<s.b) - 1;
		end = 1<<(24-s.w);
	}

	@Override
	public boolean hasNext() {
		return bc < end;
	}

	@Override
	public GameState next() {
		GameState ret = new GameState(s, uncollapse(w, bc));

		// If too much time is spent here, then this can be sped up by not iterating on filtered out states
		// (this would need the Gasser inv_hash)
		w = nextChoose(w);
		if (w >= (1 << 24)) {
			w = (1 << s.w) - 1;
			bc = nextChoose(bc);
		}

		return ret;
	}

	// Get the next bitset in lexicographical order among the bitsets with the same popcount as x.
	private int nextChoose(int x){
		int c=x&-x, r=x+c;
		return (((r^x)>>2)/c)|r;
	}

	// The collapse operation is to shift each bit of w to the right by the amount of set bits in b below the current
	// bit position. Uncollapse is the inverse of this.
	// This operation is needed, so that nextChoose can iterate on only those black stone configurations, where
	// the black stones are on fields left empty by the white stones.
	private long uncollapse(int w, int b){
		int r=0;
		for(int i=1; i<1<<24; i<<=1)
			if((w&i) != 0) {
				b <<= 1;
			} else {
				r |= b & i;
			}
		return ((long)r<<24)|w;
	}
}
