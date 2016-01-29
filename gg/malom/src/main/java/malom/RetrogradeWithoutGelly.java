package malom;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
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

	private Map<SectorId, DataSet<Tuple2<GameState, ValueCount>>> results = new HashMap<>();

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
	public DataSet<Tuple2<GameState, ValueCount>> solve(SectorId u, ExecutionEnvironment env) {
		if(u.isLosing()) {
			results.put(u, RetrogradeCommon.createSectorVertices(u, ValueCount.value(Value.loss(0)), env));
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
				DataSet<Tuple2<GameState, ValueCount>> currentSectorVertices = solve(chdSector, env);
				if(vertices == null) {
					vertices = currentSectorVertices;
				} else {
					vertices = vertices.union(currentSectorVertices);
				}
			}

			// Add the main sectors
			vertices = vertices.union(RetrogradeCommon.createSectorVertices(u, ValueCount.count(-1), env));
			if(u.hasTwin()) {
				vertices = vertices.union(RetrogradeCommon.createSectorVertices(u.negate(), ValueCount.count(-1), env));
			}


			// Initialize main sectors (we do this here, because we need the degrees)
			vertices = init(vertices, u, u.hasTwin() ? u.negate() : null, movegen);

			// The essence of the computation
			vertices = RetrogradeCommon.iterate(vertices, movegen);

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

	private static final class SumReducer<K> implements ReduceFunction<Tuple2<K, Short>> {
		@Override
		public Tuple2<K, Short> reduce(Tuple2<K, Short> a, Tuple2<K, Short> b) throws Exception {
			if (!a.f0.equals(b.f0)) {
				throw new RuntimeException("SumReducer was called with two records that have differing keys.");
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
	public static DataSet<Tuple2<GameState, ValueCount>> init(
			DataSet<Tuple2<GameState, ValueCount>> vertices,
			SectorId mainSec1, SectorId mainSec2, Movegen movegen) {

		DataSet<Tuple2<GameState, Short>> inDegrees = vertices
				.flatMap(new FlatMapFunction<Tuple2<GameState,ValueCount>, Tuple2<GameState, GameState>>() {
					@Override
					public void flatMap(Tuple2<GameState, ValueCount> v, Collector<Tuple2<GameState, GameState>> out) throws Exception {
						for(GameState target: RetrogradeCommon.generateEdges(v.f0, movegen)) {
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
				}).name("in-degrees"); // todo: this could be an outer join instead of coGroup


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

	public static final class FilterToOneSector implements FilterFunction<Tuple2<GameState, ValueCount>> {

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
