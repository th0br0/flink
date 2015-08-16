package malom;

public class ValueCount {
	public Value value = Value.getNull();
	public short count = -1;

	public static ValueCount value(Value v){
		ValueCount r = new ValueCount();
		r.value = v;
		return r;
	}

	public static ValueCount count(int c){
		ValueCount r = new ValueCount();
		r.count = (short)c;
		return r;
	}

	public boolean isValue(){
		return !value.isNull();
	}

	public boolean isCount(){
		return count != -1;
	}

	@Override
	public boolean equals(Object o0) {
		if(o0 instanceof ValueCount) {
			ValueCount o = (ValueCount) o0;
			if(isValue()) {
				return o.isValue() && value.equals(o.value);
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
