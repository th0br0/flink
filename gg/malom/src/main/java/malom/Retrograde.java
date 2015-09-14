package malom;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
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
import java.util.*;

public class Retrograde implements Serializable {
	private static final long serialVersionUID = 1L;

	ExecutionEnvironment env;

	private Map<SectorId, DataSet<Vertex<GameState, ValueCount>>> results = new HashMap<>();


	public Retrograde(ExecutionEnvironment env) {
		this.env = env;
	}

	/**
	 * Compute the game-theoretical solution for all states of the work unit of the given sector.
	 * Recursively calls itself for sectors that the work unit directly depends on, and uses memoization (the results map).
	 * (Note: we don't actually compute anything here when the method is called; we are just creating Flink DataSets.)
	 *
	 * The computation will go one work unit at a time. A work unit is either one non-twin sector (see sectorId.hasTwin), or
	 * two sectors that are twins of each other. (Assuming a more general sector graph, the work units should be the
	 * strongly connected components. This is the case here: the only loops are self-edges and back-and-forth edges between
	 * twin nodes.)
	 */
	public DataSet<Vertex<GameState, ValueCount>> solve(SectorId u) {
		if(u.isLosing()) {
			results.put(u, createSectorVertices(u, ValueCount.value(Value.loss(0))));
		} else {

			// The main sectors are the one or two sectors of the work unit.
			// The child sectors are those sectors, that the main sectors directly depend on.
			// The sector family is all these together.

			Set<SectorId> mainSectors = new HashSet<>(Arrays.asList(u, u.negate()));
			Set<SectorId> chdSectors = new HashSet<>();
			for(SectorId mainSec: mainSectors) {
				for(SectorId chdSec: SectorGraph.graphFunc(mainSec)) {
					if(!mainSectors.contains(chdSec)) {
						chdSectors.add(chdSec);
					}
				}
			}
			assert chdSectors.size() > 0;
			List<SectorId> sectorFamily = new ArrayList<>();
			sectorFamily.addAll(mainSectors);
			sectorFamily.addAll(chdSectors);

			// The vertices DataSet will consist of all the vertices of the sector family.
			// - vertices of child sectors will have been already solved
			// - vertices of main sectors will be initialized to undefined

			DataSet<Vertex<GameState, ValueCount>> vertices = null;

			for(SectorId chdSector: chdSectors) {
				DataSet<Vertex<GameState, ValueCount>> currentSectorVertices = solve(chdSector);
				if(vertices == null) {
					vertices = currentSectorVertices;
				} else {
					vertices = vertices.union(currentSectorVertices);
				}
			}

			// Add the main sectors
			vertices = vertices.union(createSectorVertices(u, ValueCount.count(-1)));
			if(u.hasTwin()) {
				vertices = vertices.union(createSectorVertices(u.negate(), ValueCount.count(-1)));
			}

			// Create the edges
			DataSet<Edge<GameState, NullValue>> edges = createEdges(vertices, sectorFamily, mainSectors);

			Graph<GameState, ValueCount, NullValue> g = Graph.fromDataSet(vertices, edges, env);

			// Initialize main sectors (we do this here, because we need the degrees)
			g = init(g, u, u.hasTwin() ? u.negate() : null, env);

			// The essence of the computation
			g = Retrograde.iterate(g);

			// We need a result DataSet for each of the main sectors.
			results.put(u, g.getVertices().filter(new FilterToOneSector(u)));
			if(u.hasTwin()) {
				results.put(u.negate(), g.getVertices().filter(new FilterToOneSector(u.negate())));
			}

			// Write the result files
			results.get(u).writeAsText(Config.resultOutPath(u), FileSystem.WriteMode.OVERWRITE);
			if(u.hasTwin()) {
				results.get(u.negate()).writeAsText(Config.resultOutPath(u.negate()), FileSystem.WriteMode.OVERWRITE);
			}

			// Compute verify and write it
			Verify.verify(g, u, u.hasTwin() ? u.negate() : null);
		}
		return results.get(u);
	}

	public DataSet<Vertex<GameState, ValueCount>> createSectorVertices(SectorId s, ValueCount vv) {
		DataSet<GameState> gameStates = env.fromCollection(new SectorElementIterator(s), GameState.class);
		if(Config.filterSym) {
			gameStates = gameStates.filter(state -> !Symmetries.isFiltered(state.board));
		}
		return gameStates.map(new MapFunction<GameState, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> map(GameState s) throws Exception {
				return new Vertex<>(s, vv);
			}
		}).name("Vertices " + s);
	}

	public DataSet<Edge<GameState, NullValue>> createEdges(DataSet<Vertex<GameState, ValueCount>> vertices, Collection<SectorId> sectors, Collection<SectorId> mainSectors) {
		Movegen movegen = new Movegen(sectors, mainSectors);
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
		}).name("Edges");
	}

	/**
	 * Initialize all states in the sector family:
	 *	- child sectors:
	 *		- modify the depth of losses and wins to 0
	 * 	- main sectors:
	 *		- states where I can't make a move are losses in 0
	 *		- other states are initialized to count(inDegree)
	 *
	 *  @param g		A graph of a sector family, where vertices of the main sectors are not initialized
	 *  @param mainSec1	One of the main sectors
	 *  @param mainSec2 The other main sector (null, if not twin)
	 *  @return		g transformed, by initializing the vertices of the main sectors.
 	 */
	public Graph<GameState, ValueCount, NullValue> init(
			Graph<GameState, ValueCount, NullValue> g,
			SectorId mainSec1, SectorId mainSec2,
			ExecutionEnvironment env) {
		// Set losses to 0, and
		// set others to count(deg)
		return Graph.fromDataSet(g.getVertices().join(g.inDegrees()).where(0).equalTo(0).with(new JoinFunction<Vertex<GameState, ValueCount>, Tuple2<GameState, Long>, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> join(Vertex<GameState, ValueCount> vertex, Tuple2<GameState, Long> deg0) throws Exception {
				long deg = deg0.f1;
				GameState state = vertex.getId();
				ValueCount value = vertex.getValue();
				if (!state.sid.equals(mainSec1) && (mainSec2 == null || !state.sid.equals(mainSec2))) {
					// vertex in child sector
					if(value.isCount()) {
						return vertex;
					} else {
						value.value.depth = 0;
						return new Vertex<>(state, value);
					}
				} else {
					// vertex in main sector
					if (deg == 0) { // state is blocked
						return new Vertex<>(state, ValueCount.value(Value.loss(0)));
					} else { // to be computed by the iteration (set to count for now)
						// (Note: the first iteration will take child sectors into account.)
						return new Vertex<>(state, ValueCount.count((int) deg));
					}
				}
			}
		}), g.getEdges(), env);
	}

	/**
	 * The core of the computation. We iterate by the order of increasing depth (cf. the assert with getSuperstepNumber).
	 * Each vertex sends msg at most once, when its final value (loss or win) is determined. (Vertices in child sectors
	 * do this in the first iteration.)
	 * Draw states never send a msg.
	 *
	 * @param g		A graph of a sector family, where the child sectors have already been solved, and the main sectors
	 *              have been initialized by init.
	 * @return		g transformed, so that the vertices of the main sectors are solved (have their final values).
	 */
	public static Graph<GameState, ValueCount, NullValue> iterate(Graph<GameState, ValueCount, NullValue> g) {

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
								// We have seen all successors, set final value. (will be a loss, because msg is a win)
								vv = ValueCount.value(msg.undoNegate());
								newValueSet = true;
							} else { //nem fogyott el a count
								// One less successor to wait for
								vv = ValueCount.count(newCount);
								newValueSet = true;
							}
						} else { //vesztesbol terjesztunk
							// We can move to a loss, so we have a win.
							vv = ValueCount.value(msg.undoNegate());
							newValueSet = true;
						}
					} else { //val-ba terjesztunk
						// This can't be a loss. We set a state to loss only after seeing all its successors, so
						// why haven't we seen the successor that this msg is coming from?
						assert vv.value.isWin();
						// We shouldn't have found a better value for a state that we already think we know the final value of.
						assert vv.value.depth <= msg.depth + 1;
					}
				}
				if (newValueSet) {
					setNewVertexValue(vv);
				}
			}
		}, new MessagingFunction<GameState, ValueCount, Value, NullValue>() {
			@Override
			public void sendMessages(Vertex<GameState, ValueCount> vertex) throws Exception {
				// Note: if we were coding the iteration manually (without Gelly),
				// we could stuff the below "if" into a filter to be done before joining the workset with the edges.
				if (vertex.getValue().isValue()) {
					sendMessageToAllNeighbors(vertex.getValue().value);
					assert vertex.getValue().value.depth + 1 == getSuperstepNumber();
				}
			}
		}, 1000, config);
	}


	public class FilterToOneSector implements FilterFunction<Vertex<GameState, ValueCount>> {

		SectorId s;

		public FilterToOneSector(SectorId s) {
			this.s = s;
		}

		@Override
		public boolean filter(Vertex<GameState, ValueCount> value) throws Exception {
			return value.getId().sid.equals(s);
		}
	}
}
