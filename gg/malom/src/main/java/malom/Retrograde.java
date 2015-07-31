package malom;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
		movegen = new Movegen(sectors); //Generates edges with targets only inside these sectors

		DataSet<Vertex<GameState, ValueCount>> vertices =
				createGameStates()
						.map(new InitGameStateValue());

		DataSet<Edge<GameState, NullValue>> edges =
				vertices.flatMap(new FlatMapFunction<Vertex<GameState, ValueCount>, Edge<GameState, NullValue>>() {
					@Override
					public void flatMap(Vertex<GameState, ValueCount> v, Collector<Edge<GameState, NullValue>> out) throws Exception {
						for (GameState parent : movegen.get_parents(v.getId())) {
							out.collect(new Edge<GameState, NullValue>(v.getId(), parent, new NullValue()));
						}
					}
				});

		Graph<GameState, ValueCount, NullValue> g = Graph.fromDataSet(vertices, edges, env);

//		//
//		try {
//			g.getVertices().print();
//		} catch (Exception e) {
//			throw new RuntimeException();
//		}
//		//

		VertexCentricConfiguration config = new VertexCentricConfiguration();
		config.setOptDegrees(true);
		//config.setSolutionSetUnmanagedMemory(true);

		return g.runVertexCentricIteration(new VertexUpdateFunction<GameState, ValueCount, Short>() {
			@Override
			public void updateVertex(Vertex<GameState, ValueCount> vertex, MessageIterator<Short> inMessages) throws Exception {
				//kesobbitodo: use getSuperstepNumber() for asserts, val-ba terjesztunk assertek athozasa
				if (vertex.getValue().isCount()) { //count-ba terjesztunk
					for (Short msg : inMessages) {
						if (msg % 2 == 1) { //nyeresbol terjesztunk
							short newCount = (short) (vertex.getValue().count + 1);
							if (newCount == getInDegree()) { //elfogyott a count
								setNewVertexValue(ValueCount.value(msg + 1));
							} else { //nem fogyott el a count
								setNewVertexValue(ValueCount.count(newCount));
							}
						} else { //vesztesbol terjesztunk
							setNewVertexValue(ValueCount.value(msg + 1)); // This also handles the blocked self-msg case
						}
					}
				}
			}
		}, new MessagingFunction<GameState, ValueCount, Short, NullValue>() {
			@Override
			public void sendMessages(Vertex<GameState, ValueCount> vertex) throws Exception {
				if (vertex.getValue().isValue()) {
					sendMessageToAllNeighbors(vertex.getValue().value);
				} else {
					if(getInDegree() == 0) { // state is blocked
						sendMessageTo(vertex.getId(), (short)-1);
					}
				}
			}
		//}, 1000, new VertexCentricConfiguration().setOptDegrees(false)); ///////////////////////////////////true
		}, 1000, config);
	}

	private DataSet<GameState> createGameStates() {
		DataSet<GameState> gameStates = null;
		for(SectorId s: sectors) {
			DataSet<GameState> currentSectorGameStates = env.fromCollection(new SectorElementIterator(s), GameState.class);
			if(gameStates == null) {
				gameStates = currentSectorGameStates;
			} else {
				gameStates = gameStates.union(currentSectorGameStates);
			}
		}
		return gameStates;
	}

	class InitGameStateValue implements MapFunction<GameState, Vertex<GameState, ValueCount>>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public Vertex<GameState, ValueCount> map(GameState a) throws Exception {
			if (a.sid.b + a.sid.bf <= 3 && movegen.can_close_mill(a)) { // win condition
				return new Vertex<GameState, ValueCount>(a, ValueCount.value(1));
			} else {
				return new Vertex<GameState, ValueCount>(a, ValueCount.count(0)); //vigyazat! forditva megy!
			}
		}
	}


}
