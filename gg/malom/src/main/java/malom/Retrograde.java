package malom;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunction;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.ReduceNeighborsFunction;
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

		// Create the vertices by unioning together all sectors
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


		// Set blocked states to 0, and
		// set those states to 1, where I can win by closing a mill
		g = Graph.fromDataSet(g.getVertices().join(g.inDegrees()).where(0).equalTo(0).with(new JoinFunction<Vertex<GameState,ValueCount>, Tuple2<GameState,Long>, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> join(Vertex<GameState, ValueCount> v0, Tuple2<GameState, Long> deg0) throws Exception {
				long deg = deg0.f1;
				GameState v = v0.f0;
				if(deg == 0) {
					return new Vertex<>(v, ValueCount.value(0));
				} else {
					if (v.sid.b + v.sid.bf <= 3 && movegen.can_close_mill(v)) {
						return new Vertex<>(v, ValueCount.value(1));
					} else {
						return new Vertex<>(v, ValueCount.count((int)deg));
					}
				}
			}
		}), edges, env);

		// Set those states to 1, where I can win by moving to a blocked (0 valued) state
		g = Graph.fromDataSet(g.groupReduceOnNeighbors(
				new NeighborsFunctionWithVertexValue<GameState, ValueCount, NullValue, Vertex<GameState, ValueCount>>() {
			@Override
			public void iterateNeighbors(Vertex<GameState, ValueCount> vertex, Iterable<Tuple2<Edge<GameState, NullValue>, Vertex<GameState, ValueCount>>> neighbors, Collector<Vertex<GameState, ValueCount>> out) throws Exception {
				for (Tuple2<Edge<GameState, NullValue>, Vertex<GameState, ValueCount>> n: neighbors) {
					if(n.f1.getValue().isValue() && n.f1.getValue().value == 0) {
						out.collect(new Vertex<>(vertex.getId(), ValueCount.value(1)));
						return;
					}
				}
				out.collect(vertex);
			}
		}, EdgeDirection.IN), edges, env);


		// The iteration

		VertexCentricConfiguration config = new VertexCentricConfiguration();
		//config.setOptDegrees(true);
		//config.setSolutionSetUnmanagedMemory(true);

		return g.runVertexCentricIteration(new VertexUpdateFunction<GameState, ValueCount, Short>() {
			@Override
			public void updateVertex(Vertex<GameState, ValueCount> vertex, MessageIterator<Short> inMessages) throws Exception {
				for (Short msg : inMessages) {
					assert getSuperstepNumber() == msg;
					if (vertex.getValue().isCount()) { //count-ba terjesztunk
						if (msg % 2 == 1) { //nyeresbol terjesztunk
							short newCount = (short) (vertex.getValue().count - 1);
							if (newCount == 0) { //elfogyott a count
								setNewVertexValue(ValueCount.value(msg + 1));
							} else { //nem fogyott el a count
								setNewVertexValue(ValueCount.count(newCount));
							}
						} else { //vesztesbol terjesztunk
							setNewVertexValue(ValueCount.value(msg + 1)); // This also handles the blocked self-msg case
						}
					} else { //val-ba terjesztunk
						assert vertex.getValue().value % 2 == 1;
						assert vertex.getValue().value <= msg + 1; // (az ultra-nal itt meg valami magic van, ld. a c++ kodban)
					}
				}
			}
		}, new MessagingFunction<GameState, ValueCount, Short, NullValue>() {
			@Override
			public void sendMessages(Vertex<GameState, ValueCount> vertex) throws Exception {
				if (vertex.getValue().isValue()) {
					if(vertex.getValue().value != 0) {
						sendMessageToAllNeighbors(vertex.getValue().value);
					} else {
						assert getSuperstepNumber() == 1;
					}
				}
			}
		}, 1000, config);
	}
}
