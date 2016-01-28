package malom;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RetrogradeWithoutGelly implements Serializable {
	private static final long serialVersionUID = 1L;

	ExecutionEnvironment env;

	private Map<SectorId, DataSet<Tuple2<GameState, ValueCount>>> results = new HashMap<>();


	public RetrogradeWithoutGelly(ExecutionEnvironment env) {
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
	public DataSet<Tuple2<GameState, ValueCount>> solve(SectorId u) {
		if(u.isLosing()) {
			results.put(u, createSectorVertices(u, ValueCount.value(Value.loss(0))));
		} else {

			// (See comment in createSectorFamily)
			Set<SectorId> mainSectors, chdSectors;
			List<SectorId> sectorFamily;
			SectorGraph.createSectorFamily(u, mainSectors = new HashSet<>(), chdSectors = new HashSet<>(), sectorFamily = new ArrayList<>());

			Movegen movegen = new Movegen(sectorFamily, mainSectors);

			// The vertices DataSet will consist of all the vertices of the sector family.
			// - vertices of child sectors will have been already solved
			// - vertices of main sectors will be initialized to undefined

			DataSet<Tuple2<GameState, ValueCount>> vertices = null;

			for(SectorId chdSector: chdSectors) {
				DataSet<Tuple2<GameState, ValueCount>> currentSectorVertices = solve(chdSector);
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


			// Initialize main sectors (we do this here, because we need the degrees)
			vertices = init(vertices, u, u.hasTwin() ? u.negate() : null, movegen);

			// The essence of the computation
			vertices = RetrogradeWithoutGelly.iterate(vertices, movegen);

			// We need a result DataSet for each of the main sectors.
			results.put(u, vertices.filter(new FilterToOneSector(u)));
			if(u.hasTwin()) {
				results.put(u.negate(), vertices.filter(new FilterToOneSector(u.negate())));
			}

			// Write the result files
			results.get(u).writeAsText(Config.resultOutPath(u), FileSystem.WriteMode.OVERWRITE);
			if(u.hasTwin()) {
				results.get(u.negate()).writeAsText(Config.resultOutPath(u.negate()), FileSystem.WriteMode.OVERWRITE);
			}

			// Compute verify and write it
			//todo
			//Verify.verify(g, u, u.hasTwin() ? u.negate() : null);
		}
		return results.get(u);
	}

	public DataSet<Tuple2<GameState, ValueCount>> createSectorVertices(SectorId s, ValueCount initValue) {
		DataSet<GameState> gameStates = env.fromCollection(new SectorElementIterator(s), GameState.class).name("Vertices " + s);
		if(Config.filterSym) {
			gameStates = gameStates.filter(state -> !Symmetries.isFiltered(state.board));
		}
		return gameStates.map(new MapFunction<GameState, Tuple2<GameState, ValueCount>>() {
			@Override
			public Tuple2<GameState, ValueCount> map(GameState state) throws Exception {
				return Tuple2.of(state, initValue);
			}
		}).name(s.toString() + " (init)");
	}

	static Iterable<GameState> generateEdges(GameState state, Movegen movegen) {
		Set<Long> parentBoards = new HashSet<>();
		List<GameState> ret = new ArrayList<GameState>();
		if (!Config.filterSym) {
			for (GameState parent : movegen.get_parents(state)) {
				parentBoards.add(parent.board);
			}
		} else {
			assert !Symmetries.isFiltered(state.board);
			for (GameState parent : movegen.get_parents(state)) {
				parent.board = Symmetries.minSym48(parent.board);
				if (!parentBoards.contains(parent.board)) {
					parentBoards.add(parent.board);
					ret.add(parent);
				}
			}
		}
		return ret;
	}

	private static final class SumReducer<K> implements ReduceFunction<Tuple2<K, Short>> {
		@Override
		public Tuple2<K, Short> reduce(Tuple2<K, Short> a, Tuple2<K, Short> b) throws Exception {
			if (!a.f0.equals(b.f0)) {
				throw new RuntimeException("SumReducer was called with two record that have differing keys.");
			}
			a.f1 = (short)(a.f1 + b.f1);
			return a;
		}
	}

	/**
	 * Calculate in-degrees, and initialize all states in the sector family:
	 *	- child sectors:
	 *		- modify the depth of losses and wins to 0
	 * 	- main sectors:
	 *		- states where I can't make a move are losses in 0
	 *		- other states are initialized to count(inDegree)
	 *
	 *  @param vertices	The vertices of a sector family, where vertices of the main sectors are not initialized
	 *  @param mainSec1	One of the main sectors
	 *  @param mainSec2 The other main sector (null, if not twin)
	 *  @return			vertices transformed, by initializing the vertices of the main sectors.
 	 */
	public DataSet<Tuple2<GameState, ValueCount>> init(
			DataSet<Tuple2<GameState, ValueCount>> vertices,
			SectorId mainSec1, SectorId mainSec2, Movegen movegen) {

		DataSet<Tuple2<GameState, Short>> inDegrees = vertices
				.flatMap(new FlatMapFunction<Tuple2<GameState,ValueCount>, Tuple2<GameState, GameState>>() {
					@Override
					public void flatMap(Tuple2<GameState, ValueCount> v, Collector<Tuple2<GameState, GameState>> out) throws Exception {
						for(GameState target: generateEdges(v.f0, movegen)) {
							out.collect(Tuple2.of(v.f0, target));
						}
					}
				}).name("edges") // TODO: check that this is not materialized
				.map(new MapFunction<Tuple2<GameState, GameState>, Tuple2<GameState, Short>>() {
					@Override
					public Tuple2<GameState, Short> map(Tuple2<GameState, GameState> t) throws Exception {
						return Tuple2.of(t.f1, (short) 1);
					}
				})
				//.groupBy(0).sum(1)
				.groupBy(0).reduce(new SumReducer<>())
				.coGroup(vertices).where(0).equalTo(0).with(new CoGroupFunction<Tuple2<GameState, Short>, Tuple2<GameState, ValueCount>, Tuple2<GameState, Short>>() {
					@Override
					public void coGroup(Iterable<Tuple2<GameState, Short>> first, Iterable<Tuple2<GameState, ValueCount>> second, Collector<Tuple2<GameState, Short>> out) throws Exception {
						// First is a degree coming from above, second should be considered 0 degree
						Iterator<Tuple2<GameState, Short>> it1 = first.iterator();
						Iterator<Tuple2<GameState, ValueCount>> it2 = second.iterator();
						if (it1.hasNext()) {
							out.collect(it1.next());
						} else {
							out.collect(Tuple2.of(it2.next().f0, (short) 0));
						}
					}
				}).name("in-degrees");


		return vertices.join(inDegrees).where(0).equalTo(0).with(new JoinFunction<Tuple2<GameState, ValueCount>, Tuple2<GameState, Short>, Tuple2<GameState, ValueCount>>() {
			@Override
			public Tuple2<GameState, ValueCount> join(Tuple2<GameState, ValueCount> vertex, Tuple2<GameState, Short> deg0) throws Exception {
				short deg = deg0.f1;
				GameState state = vertex.f0;
				ValueCount value = vertex.f1;
				if (!state.sid.equals(mainSec1) && (mainSec2 == null || !state.sid.equals(mainSec2))) {
					// vertex in child sector
					if (value.isCount()) {
						return vertex;
					} else {
						value.value.depth = 0;
						return Tuple2.of(state, value);
					}
				} else {
					// vertex in main sector
					if (deg == 0) { // state is blocked
						return Tuple2.of(state, ValueCount.value(Value.loss(0)));
					} else { // to be computed by the iteration (set to count for now)
						// (Note: the first iteration will take child sectors into account.)
						return Tuple2.of(state, ValueCount.count(deg));
					}
				}
			}
		}).name("init");
	}

	/**
	 * The core of the computation. We iterate by the order of increasing depth (cf. the assert with getSuperstepNumber).
	 * Each vertex sends msg at most once, when its final value (loss or win) is determined. (Vertices in child sectors
	 * do this in the first iteration.)
	 * Draw states never send a msg.
	 *
	 * @param vertices	The vertices of a sector family, where the child sectors have already been solved, and the main sectors
	 *					have been initialized by init.
	 * @return			vertices transformed, so that the main sectors are solved (have their final values).
	 */
	public static DataSet<Tuple2<GameState, ValueCount>> iterate(DataSet<Tuple2<GameState, ValueCount>> vertices, Movegen movegen) {

		DataSet<Tuple2<GameState, ValueCount>> initialWorkset = vertices.filter(new FilterFunction<Tuple2<GameState, ValueCount>>() {
			@Override
			public boolean filter(Tuple2<GameState, ValueCount> v) throws Exception {
				return v.f1.isValue();
			}
		});

		DeltaIteration<Tuple2<GameState, ValueCount>, Tuple2<GameState, ValueCount>> iteration = vertices.iterateDelta(initialWorkset, 1000, (int) 0);

		DataSet<Tuple2<GameState, ValueCount>> solutionSetDelta =
				iteration.getWorkset()
				.flatMap(new RichFlatMapFunction<Tuple2<GameState, ValueCount>, Tuple3<GameState, Value, Short>>() {
					@Override
					public void flatMap(Tuple2<GameState, ValueCount> v, Collector<Tuple3<GameState, Value, Short>> out) throws Exception {
						assert v.f1.isValue();
						assert v.f1.value.depth + 1 == ((IterationRuntimeContext) this.getRuntimeContext()).getSuperstepNumber();
						for (GameState target : generateEdges(v.f0, movegen)) {
							out.collect(Tuple3.of(target, v.f1.value, (short) 1));
						}
					}
				})
				.groupBy(0)
				.reduce(new ReduceFunction<Tuple3<GameState, Value, Short>>() {
					@Override
					public Tuple3<GameState, Value, Short> reduce(Tuple3<GameState, Value, Short> a, Tuple3<GameState, Value, Short> b) throws Exception {
						assert a.f0.equals(b.f0);
						if (a.f1.isLoss()) {
							return a;
						}
						if (b.f1.isLoss()) {
							return b;
						}
						// both of them are wins
						return Tuple3.of(a.f0, a.f1, (short) (a.f2 + b.f2));
					}
				})
				.join(iteration.getSolutionSet()).where(0).equalTo(0).with(new RichFlatJoinFunction<Tuple3<GameState, Value, Short>, Tuple2<GameState, ValueCount>, Tuple2<GameState, ValueCount>>() {

					@Override
					public void join(Tuple3<GameState, Value, Short> update0, Tuple2<GameState, ValueCount> oldVertex, Collector<Tuple2<GameState, ValueCount>> out) throws Exception {
						GameState state = oldVertex.f0;
						ValueCount oldVal = oldVertex.f1;
						Value updateVal = update0.f1;
						Short updateNumWins = update0.f2;

						if (oldVal.isValue()) {
							return;
						}

						if (updateVal.isLoss()) {
							out.collect(Tuple2.of(state, ValueCount.value(Value.win(updateVal.depth + 1))));
						} else {
							short newCount = (short) (oldVal.count - updateNumWins);
							assert newCount >= 0;
							if (newCount == 0) {
								out.collect(Tuple2.of(state, ValueCount.value(Value.loss(updateVal.depth + 1))));
							} else {
								out.collect(Tuple2.of(state, ValueCount.count(oldVal.count - updateNumWins)));
							}
						}
					}
				}).name("solutionSetDelta");

		DataSet<Tuple2<GameState, ValueCount>> newWorkset = solutionSetDelta.filter(new FilterFunction<Tuple2<GameState, ValueCount>>() {
			@Override
			public boolean filter(Tuple2<GameState, ValueCount> v) throws Exception {
				return v.f1.isValue();
			}
		}).name("newWorkset");

		return iteration.closeWith(solutionSetDelta, newWorkset);
	}


	public class FilterToOneSector implements FilterFunction<Tuple2<GameState, ValueCount>> {

		SectorId s;

		public FilterToOneSector(SectorId s) {
			this.s = s;
		}

		@Override
		public boolean filter(Tuple2<GameState, ValueCount> value) throws Exception {
			return value.f0.sid.equals(s);
		}
	}
}
