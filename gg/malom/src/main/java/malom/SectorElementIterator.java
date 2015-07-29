package malom;

import java.io.Serializable;
import java.util.Iterator;

public class SectorElementIterator implements Iterator<GameState>, Serializable {
	private static final long serialVersionUID = 1L;

	SectorId s;

	int w, b;

	SectorElementIterator(SectorId s) {
		this.s = s;
		w = (1<<s.w) - 1;
		b = (1<<s.b) - 1;
	}

	@Override
	public boolean hasNext() {
		return w < 1<<24;
	}

	@Override
	public GameState next() {
		GameState ret = new GameState(s, (long)w | ((long)b << 24));

		b = nextChoose(b);
		if(b >= 1<<24) {
			b = (1<<s.b) - 1;
			w = nextChoose(w);
		}

		return ret;
	}

	private int nextChoose(int x){
		int c=x&-x, r=x+c;
		return (((r^x)>>2)/c)|r;
	}

}
