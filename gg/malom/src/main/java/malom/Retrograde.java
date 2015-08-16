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
import java.util.Set;

public class Retrograde implements Serializable {
	private static final long serialVersionUID = 1L;

	ArrayList<SectorId> sectors;
	transient ExecutionEnvironment env;

	Movegen movegen;

	public Retrograde(ArrayList<SectorId> sectors, ExecutionEnvironment env) {
		this.sectors = sectors;
		this.env = env;
	}

	public Graph<GameState, ValueCount, NullValue> run() {

		movegen = new Movegen(sectors); // (Will generate edges with targets only inside these sectors)

		// Create the vertices by unioning all sectors
		DataSet<GameState> gameStates = null;
		for(SectorId s: sectors) {
			DataSet<GameState> currentSectorGameStates = env.fromCollection(new SectorElementIterator(s), GameState.class);
			if(gameStates == null) {
				gameStates = currentSectorGameStates;
			} else {
				gameStates = gameStates.union(currentSectorGameStates);
			}
		}
		if(Config.filterSym) {
			gameStates = gameStates.filter(state -> !Symmetries.isFiltered(state.board));
		}
		//DataSet<Vertex<GameState, ValueCount>> vertices = gameStates.map(s -> new Vertex<>(s, ValueCount.count(-1)));
		DataSet<Vertex<GameState, ValueCount>> vertices = gameStates.map(new MapFunction<GameState, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> map(GameState s) throws Exception {
				return new Vertex<>(s, ValueCount.count(-1));
			}
		});


		// Create the edges
		DataSet<Edge<GameState, NullValue>> edges =
				vertices.flatMap(new FlatMapFunction<Vertex<GameState, ValueCount>, Edge<GameState, NullValue>>() {
					@Override
					public void flatMap(Vertex<GameState, ValueCount> v, Collector<Edge<GameState, NullValue>> out) throws Exception {
						if(!Config.filterSym) {
							for (GameState parent : movegen.get_parents(v.getId())) {
								out.collect(new Edge<>(v.getId(), parent, new NullValue()));
							}
						} else {
							assert !Symmetries.isFiltered(v.getId().board);
							Set<Long> parentBoards = new HashSet<Long>();
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

		Graph<GameState, ValueCount, NullValue> g = Graph.fromDataSet(vertices, edges, env);

		
		// Set losses to 0, and
		// set others to count(deg)
		g = Graph.fromDataSet(g.getVertices().join(g.inDegrees()).where(0).equalTo(0).with(new JoinFunction<Vertex<GameState, ValueCount>, Tuple2<GameState, Long>, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> join(Vertex<GameState, ValueCount> v0, Tuple2<GameState, Long> deg0) throws Exception {
				long deg = deg0.f1;
				GameState v = v0.f0;
				if (v.sid.isLosing()) {
					return new Vertex<>(v, ValueCount.value(Value.loss(0)));
				} else {
					if (deg == 0) { // state is blocked
						return new Vertex<>(v, ValueCount.value(Value.loss(0)));
					} else { // to be computed by the iteration (set to count for now)
						return new Vertex<>(v, ValueCount.count((int) deg));
					}
				}
			}
		}), edges, env);


		// The iteration

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
