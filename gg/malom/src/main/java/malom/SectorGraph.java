package malom;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SectorGraph {

	ExecutionEnvironment env;

	public SectorGraph(ExecutionEnvironment env) {
		this.env = env;
	}

	public DataSet<Vertex<GameState, ValueCount>> solve(SectorId u) {
		if(u.isLosing()) {
			results.put(u, Retrograde.createSectorVertices(u, ValueCount.value(Value.loss(0)), env));
		} else {
			Set<SectorId> mainSectors = new HashSet<>(Arrays.asList(u, u.negate()));
			Set<SectorId> chdSectors = new HashSet<>();
			for(SectorId mainSec: mainSectors) {
				chdSectors.addAll(graphFunc(mainSec));
			}
			List<SectorId> sectorFamily = new ArrayList<>();
			sectorFamily.addAll(mainSectors);
			sectorFamily.addAll(chdSectors);

			DataSet<Vertex<GameState, ValueCount>> vertices = null;
			for(SectorId chdSector: chdSectors) {
				DataSet<Vertex<GameState, ValueCount>> currentSectorVertices = solve(chdSector);
				if(vertices == null) {
					vertices = currentSectorVertices;
				} else {
					vertices = vertices.union(currentSectorVertices);
				}
			}

			vertices = vertices.union(Retrograde.createSectorVertices(u, ValueCount.count(-1), env));
			if(u.isTwin()) {
				vertices = vertices.union(Retrograde.createSectorVertices(u.negate(), ValueCount.count(-1), env));
			}

			DataSet<Edge<GameState, NullValue>> edges = Retrograde.createEdges(vertices, sectorFamily);

			Graph<GameState, ValueCount, NullValue> g = Graph.fromDataSet(vertices, edges, env);

			g = Retrograde.countChdAndInitBlocked(g, u, u.negate(), env);

			g = Retrograde.iterate(g, env);

			results.put(u, g.getVertices().filter(new FilterFunction<Vertex<GameState, ValueCount>>() {
				@Override
				public boolean filter(Vertex<GameState, ValueCount> v) throws Exception {
					return v.getId().sid.equals(u);
				}
			}));
			if(u.isTwin()) {
				results.put(u.negate(), g.getVertices().filter(new FilterFunction<Vertex<GameState, ValueCount>>() {
					@Override
					public boolean filter(Vertex<GameState, ValueCount> v) throws Exception {
						return v.getId().sid.equals(u.negate());
					}
				}));
			}
		}
		return results.get(u);
	}

	private Map<SectorId, DataSet<Vertex<GameState, ValueCount>>> results = new HashMap<>();


	private List<SectorId> graphFunc(SectorId u) {
		List<SectorId> r0 = graphFuncInner(u);
		List<SectorId> r = new ArrayList<>();
		for (SectorId x : r0) {
			x.negateInPlace();
			if(!u.equals(x)){
				r.add(x);
			}
		}
		return r;
	}

	private List<SectorId> graphFuncInner(SectorId u) {
		List<SectorId> v = new ArrayList<>();
		v.add(new SectorId(u));
		v.add(new SectorId(u));

		if (u.wf != 0) {
			v.get(0).wf--;
			v.get(0).w++;

			v.get(1).wf--;
			v.get(1).w++;
			v.get(1).b--;
		} else {
			v.get(1).b--;
		}

		return v;
	}

}
