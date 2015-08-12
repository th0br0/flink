package malom;

import java.io.Serializable;
import java.util.Iterator;

// Iterates over all elements in a sector.
public class SectorElementIterator implements Iterator<GameState>, Serializable {
	private static final long serialVersionUID = 1L;

	final SectorId s;

	int w, bc;
	final int end;
	GameState next;

	SectorElementIterator(SectorId s) {
		this.s = s;
		w = (1<<s.w) - 1;
		bc = (1<<s.b) - 1;
		end = 1<<(24-s.w);
		assembleNext();
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public GameState next() {
		GameState ret = next;

		do {
			w = nextChoose(w);
			if (w >= (1 << 24)) {
				w = (1 << s.w) - 1;
				bc = nextChoose(bc);
			}
			assembleNext();
		} while(Config.filterSym && hasNext() && Symmetries.minSym48(next.board) < next.board);

		return ret;
	}

	private void assembleNext() {
		if(bc < end) {
			next = new GameState(s, uncollapse(w, bc));
		} else {
			next = null;
		}
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
