package malom;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;

public class Solver {

	public static void main(String[] args) throws Exception {

//		/////
//		GameState s1 = new GameState(new SectorId(1,1,0,0), 123L);
//		GameState s2 = new GameState(new SectorId(1,1,0,0), 123L);
//		System.out.println(s1.compareTo(s2));
//		System.out.println(s1.equals(s2));
//		if(0==0) return;
//		/////





		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		///////double backChannelMemory = this.config.getDouble(ITERATION_HEAD_SOLUTION_SET_MEMORY, 0);
		///////System.out.println(env.getConfig().getGlobalJobParameters().toMap());

		ArrayList<SectorId> sectors = new ArrayList<SectorId>();
		sectors.add(new SectorId(3, 3, 0, 0)); ////////////////////////////////////////////
		//sectors.add(new SectorId(1, 1, 0, 0)); ////////////////////////////////////////////
		//sectors.add(new SectorId(2, 3, 0, 0)); sectors.add(new SectorId(3, 2, 0, 0)); ////////////////////////////////////////////


		Retrograde retr33 = new Retrograde(sectors, env);
		Graph<GameState, ValueCount, NullValue> res33 = retr33.run();
		//res33.getVertices().print();
		System.out.println(res33.getVertices().count());
	}
}
