package malom;

import java.io.Serializable;

public class SectorId implements Serializable, Comparable<SectorId> {
	private static final long serialVersionUID = 1L;

	public byte w, b, wf, bf; // feher es fekete font levo korongszamai, illetve feher es fekete altal meg felrakhato korongok szama

	public SectorId(byte w, byte b, byte wf, byte bf) {
		this.w = w;
		this.b = b;
		this.wf = wf;
		this.bf = bf;
	}

	public SectorId(int w, int b, int wf, int bf) {
		this((byte)w, (byte)b, (byte) wf, (byte) bf);
	}

	public SectorId(){}

	public void negate(){
		byte tmp = w;
		w = b;
		b =tmp;

		tmp= wf;
		wf = bf;
		bf = wf;
	}

	public boolean isLosing() {
		return w + wf < 2; /////////////////////// 3
	}

	static public SectorId getNull() {
		return new SectorId(-1, -1, -1, -1);
	}

//	boolean eks() {
//		return *this==-*this;
//	}


//	boolean transient() const {
//		#if VARIANT==STANDARD || VARIANT==MORABARABA
//		return !(wf==0 && bf==0);
//		#else
//		return !(w!=0 && b!=0);
//		#endif
//	}

//	bool twine() const {
//		return !eks() && !transient();
//	}

//	string file_name(){
//		char b[255];
//		sprintf_s(b, "%s_%d_%d_%d_%d.sec"FNAME_SUFFIX, VARIANT_NAME, w, b, wf, bf);
//		string r=string(b);
//		return r;
//	}

//	bool operator<(const id &o) const {return make_pair(make_pair(w,b),make_pair(wf,bf)) < make_pair(make_pair(o.w,o.b),make_pair(o.wf,o.bf));}
//	bool operator>(const id &o) const {return make_pair(make_pair(w,b),make_pair(wf,bf)) > make_pair(make_pair(o.w,o.b),make_pair(o.wf,o.bf));}
//
//	bool operator==(const id &o) const {return w==o.w && b==o.b && wf==o.wf && bf==o.bf;}
//	bool operator!=(const id &o) const {return !(*this==o);}

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
