package malom;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Retrograde implements Serializable {
	private static final long serialVersionUID = 1L;

	public static DataSet<Vertex<GameState, ValueCount>> createSectorVertices(SectorId s, ValueCount vv, ExecutionEnvironment env) {
		DataSet<GameState> gameStates = env.fromCollection(new SectorElementIterator(s), GameState.class);
		if(Config.filterSym) {
			gameStates = gameStates.filter(state -> !Symmetries.isFiltered(state.board));
		}
		return gameStates.map(new MapFunction<GameState, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> map(GameState s) throws Exception {
				return new Vertex<>(s, vv);
			}
		});
	}

	public static DataSet<Edge<GameState, NullValue>> createEdges(DataSet<Vertex<GameState, ValueCount>> vertices, List<SectorId> sectors) {
		Movegen movegen = new Movegen(sectors);
		return vertices.flatMap(new FlatMapFunction<Vertex<GameState, ValueCount>, Edge<GameState, NullValue>>() {
			@Override
			public void flatMap(Vertex<GameState, ValueCount> v, Collector<Edge<GameState, NullValue>> out) throws Exception {
				if(!Config.filterSym) {
					for (GameState parent : movegen.get_parents(v.getId())) {
						out.collect(new Edge<>(v.getId(), parent, new NullValue()));
					}
				} else {
					assert !Symmetries.isFiltered(v.getId().board);
					Set<Long> parentBoards = new HashSet<>();
					for (GameState parent : movegen.get_parents(v.getId())) {
						parent.board = Symmetries.minSym48(parent.board);
						if(!parentBoards.contains(parent.board)) {
							out.collect(new Edge<>(v.getId(), parent, new NullValue()));
							parentBoards.add(parent.board);
						}
					}
				}
			}
		});
	}

	public static Graph<GameState, ValueCount, NullValue> countChdAndInitBlocked(
			Graph<GameState, ValueCount, NullValue> g,
			SectorId mainSec1, SectorId mainSec2,
			ExecutionEnvironment env) {
		// Set losses to 0, and
		// set others to count(deg)
		return Graph.fromDataSet(g.getVertices().join(g.inDegrees()).where(0).equalTo(0).with(new JoinFunction<Vertex<GameState, ValueCount>, Tuple2<GameState, Long>, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> join(Vertex<GameState, ValueCount> vertex, Tuple2<GameState, Long> deg0) throws Exception {
				long deg = deg0.f1;
				GameState v = vertex.f0;
				if (!v.sid.equals(mainSec1) && !v.sid.equals(mainSec2)) {
					return vertex;
				} else {
					if (deg == 0) { // state is blocked
						return new Vertex<>(v, ValueCount.value(Value.loss(0)));
					} else { // to be computed by the iteration (set to count for now)
						return new Vertex<>(v, ValueCount.count((int) deg));
					}
				}
			}
		}), g.getEdges(), env);
	}

	public static Graph<GameState, ValueCount, NullValue> iterate(Graph<GameState, ValueCount, NullValue> g, ExecutionEnvironment env) {

		VertexCentricConfiguration config = new VertexCentricConfiguration();
		//config.setOptDegrees(true);
		//config.setSolutionSetUnmanagedMemory(true);

		return g.runVertexCentricIteration(new VertexUpdateFunction<GameState, ValueCount, Value>() {
			@Override
			public void updateVertex(Vertex<GameState, ValueCount> vertex, MessageIterator<Value> inMessages) throws Exception {
				boolean newValueSet = false;
				ValueCount vv = vertex.getValue();
				for (Value msg : inMessages) {
					assert getSuperstepNumber() == msg.depth + 1;
					if (vv.isCount()) { //count-ba terjesztunk
						if (msg.isWin()) { //nyeresbol terjesztunk
							short newCount = (short) (vv.count - 1);
							if (newCount == 0) { //elfogyott a count
								vv = ValueCount.value(msg.undoNegate());
								newValueSet = true;
							} else { //nem fogyott el a count
								vv = ValueCount.count(newCount);
								newValueSet = true;
							}
						} else { //vesztesbol terjesztunk
							vv = ValueCount.value(msg.undoNegate());
							newValueSet = true;
						}
					} else { //val-ba terjesztunk
						assert vv.value.isWin();
						assert vv.value.depth <= msg.depth + 1; // (az ultra-nal itt meg valami magic van, ld. a c++ kodban)
					}
				}
				if(newValueSet) {
					setNewVertexValue(vv);
				}
			}
		}, new MessagingFunction<GameState, ValueCount, Value, NullValue>() {
			@Override
			public void sendMessages(Vertex<GameState, ValueCount> vertex) throws Exception {
				if (vertex.getValue().isValue()) {
					sendMessageToAllNeighbors(vertex.getValue().value);
					assert vertex.getValue().value.depth + 1 == getSuperstepNumber();
				}
			}
		}, 1000, config);
	}
}
