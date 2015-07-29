package malom;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class Solver {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ArrayList<SectorId> sectors = new ArrayList<SectorId>();
		sectors.add(new SectorId(3, 3, 0, 0));
		Retrograde retr33 = new Retrograde(sectors, env);
		Graph<GameState, ValueCount, NullValue> res33 = retr33.run();
		res33.getVertices().print();
	}
}
