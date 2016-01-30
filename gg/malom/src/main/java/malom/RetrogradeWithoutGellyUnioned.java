package malom;

import org.apache.flink.api.common.functions.CoGroupFunction;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RetrogradeWithoutGellyUnioned implements Serializable {
	private static final long serialVersionUID = 1L;

	public static void solve(SectorId u, ExecutionEnvironment env) {
		Set<SectorId> allSectors = new HashSet<>();
		SectorGraph.dfs(u, allSectors);

		DataSet<Tuple2<GameState, ValueCount>> vertices = null;
		for (SectorId s: allSectors) {
			DataSet<Tuple2<GameState, ValueCount>> current = RetrogradeCommon.createSectorVertices(s, s.isLosing() ? ValueCount.value(Value.loss(0)) : ValueCount.getNull(), env);
			if (vertices == null) {
				vertices = current;
			} else {
				vertices = vertices.union(current);
			}
		}

		Movegen movegen = new Movegen(allSectors, allSectors);

		vertices = init(vertices, movegen);

		RetrogradeCommon.iterate(vertices, movegen)
			.writeAsText(Config.resultOutPathUnioned(u), FileSystem.WriteMode.OVERWRITE);
	}

	/**
	 * Calculate in-degrees, and then initialize states that are not yet initialized:
	 *  - states where I can't make a move are losses in 0
	 *  - other states are initialized to count(inDegree)
	 *
	 *  @param vertices	The vertices
	 *  @return			vertices initialized
	 */
	public static DataSet<Tuple2<GameState, ValueCount>> init(
		DataSet<Tuple2<GameState, ValueCount>> vertices, Movegen movegen) {

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
				if (value.isNull()) {
					// vertex in main sector
					if (deg == 0) { // state is blocked
						return Tuple2.of(state, ValueCount.value(Value.loss(0)));
					} else { // to be computed by the iteration (set to count for now)
						return Tuple2.of(state, ValueCount.count(deg));
					}
				} else {
					return vertex;
				}
			}
		}).name("init");
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
}
