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

		// If too much time is spent here, then this can be sped up by not itreating on filtered out states
		// (this would need the Gasser inv_hash)
		w = nextChoose(w);
		if (w >= (1 << 24)) {
			w = (1 << s.w) - 1;
			bc = nextChoose(bc);
		}

		return ret;
	}

	private int nextChoose(int x){
		int c=x&-x, r=x+c;
		return (((r^x)>>2)/c)|r;
	}

	private long uncollapse(int w, int b){
		int r=0;
		for(int i=1; i<1<<24; i<<=1)
			if((w&i) != 0)
				b<<=1;
			else
				r|=b&i;
		return ((long)r<<24)|w;
	}
}
