package malom;

public class ValueCount {
	public short value = -1;
	public short count = -1;

	public static ValueCount value(int v){
		ValueCount r = new ValueCount();
		r.value = (short)v;
		return r;
	}

	public static ValueCount count(int c){
		ValueCount r = new ValueCount();
		r.count = (short)c;
		return r;
	}

	public boolean isValue(){
		return value != -1;
	}

	public boolean isCount(){
		return count != -1;
	}

	@Override
	public boolean equals(Object o0) {
		if(o0 instanceof ValueCount) {
			ValueCount o = (ValueCount) o0;
			if(isValue()) {
				return o.isValue() && value == o.value;
			} else if(isCount()){
				return o.isCount() && count == o.count;
			} else {
				assert false;
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return " ValueCount{" +
				"value=" + value +
				", count=" + count +
				'}';
	}
}
