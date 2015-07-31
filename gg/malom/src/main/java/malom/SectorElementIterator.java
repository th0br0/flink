package malom;

import java.io.Serializable;
import java.util.Iterator;

// Iterates over all elements in a sector.
public class SectorElementIterator implements Iterator<GameState>, Serializable {
	private static final long serialVersionUID = 1L;

	final SectorId s;

	final int bl;
	int w, b;

	SectorElementIterator(SectorId s) {
		this.s = s;
		w = (1<<s.w) - 1;
		b = (1<<s.b) - 1;
		bl = 1<<(24-s.w);
	}

	@Override
	public boolean hasNext() {
		return w < 1<<24;
	}

	@Override
	public GameState next() {
		GameState ret = new GameState(s, uncollapse(w, b));

		b = nextChoose(b);
		if(b >= bl) {
			b = (1<<s.b) - 1;
			w = nextChoose(w);
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
