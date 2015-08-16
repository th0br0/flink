package malom;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

// Verifies the result of a Retrograde run.
// Warning: If the graph is wrong (eg. induced sectors are not present), it might not catch it.
public class Verify {
	static public void verify(Graph<GameState, ValueCount, NullValue> g) {
		DataSet<Tuple4<GameState, ValueCount, ValueCount, GameState>> verif = g.groupReduceOnNeighbors(
				new NeighborsFunctionWithVertexValue<GameState, ValueCount, NullValue, Tuple4<GameState, ValueCount, ValueCount, GameState>>() {
			@Override
			public void iterateNeighbors(
					Vertex<GameState, ValueCount> vertex,
					Iterable<Tuple2<Edge<GameState, NullValue>, Vertex<GameState, ValueCount>>> neighbors,
					Collector<Tuple4<GameState, ValueCount, ValueCount, GameState>> out) throws Exception {

				ValueCount vv = vertex.getValue();

				if(vertex.getId().sid.isLosing()) {
					if(!vv.isValue() || vv.value != 0) {
						out.collect(Tuple4.of(vertex.getId(), ValueCount.value(0), vv, GameState.getNull()));
					}
				}

				boolean blocked = true, hasDraw = false, hasWin = false, hasLoss = false;
				short bestWin = Short.MAX_VALUE, bestLoss = Short.MIN_VALUE;
				GameState drawMh = null, winMh = null, lossMh = null;
				int numChd = 0, numWinChd = 0;
				for (Tuple2<Edge<GameState, NullValue>, Vertex<GameState, ValueCount>> n : neighbors) {
					blocked = false;
					numChd++;
					ValueCount cvc = n.f1.getValue();
					GameState cid = n.f1.getId();
					if(cvc.isCount()) { // child is draw
						hasDraw = true;
						drawMh = cid;
					} else if(cvc.isValue() && cvc.value % 2 == 0) { // child is loss
						hasWin = true;
						if(cvc.value + 1 < bestWin) {
							bestWin = (short)(cvc.value + 1);
							winMh = cid;
						}
					} else if(cvc.isValue() && cvc.value % 2 == 1) { // child is win
						hasLoss = true;
						if(cvc.value + 1 > bestLoss) {
							bestLoss = (short)(cvc.value + 1);
							lossMh = cid;
						}
						numWinChd++;
					} else {
						assert false;
					}
				}

				ValueCount shouldBe = null;
				GameState mh = null;
				if(blocked) {
					shouldBe = ValueCount.value(0);
				} else {
					if(hasWin) {
						shouldBe = ValueCount.value(bestWin);
						mh = winMh;
					} else if(hasDraw) {
						shouldBe = ValueCount.count(numChd - numWinChd);
						mh = drawMh;
					} else if(hasLoss) {
						shouldBe = ValueCount.value(bestLoss);
						mh = lossMh;
					} else {
						assert false;
					}
				}

				if(!vv.equals(shouldBe)) {
					out.collect(Tuple4.of(vertex.getId(), shouldBe, vv, mh));
				}
			}
		}, EdgeDirection.IN);
		verif.writeAsText("/home/gabor/malom_output/verif.txt", FileSystem.WriteMode.OVERWRITE);
	}
}
