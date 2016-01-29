package malom;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RetrogradeCommon {

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

	public static DataSet<Tuple2<GameState, ValueCount>> createSectorVertices(SectorId s, ValueCount initValue, ExecutionEnvironment env) {
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
						for (GameState target : RetrogradeCommon.generateEdges(v.f0, movegen)) {
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
}
