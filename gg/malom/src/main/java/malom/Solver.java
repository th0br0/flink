package malom;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

//-Xmx8g -Xms8g

public class Solver {

	public static void main(String[] args) throws Exception {

		System.out.println("VIGYAZAT! adjmasks atirva!");
		//System.out.println("VIGYAZAT! lose condition atirva!");


		Config.outPath = args[0];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(1);
		//env.getConfig().enableObjectReuse(); // seems to have only negligible impact

		PojoTypeInfo.registerCustomSerializer(GameState.class, GameState.GameStateSerializer.class);
		PojoTypeInfo.registerCustomSerializer(ValueCount.class, ValueCount.ValueCountSerializer.class);
		PojoTypeInfo.registerCustomSerializer(Value.class, Value.ValueSerializer.class);

		//Retrograde retrograde = new Retrograde(env);
		RetrogradeWithoutGelly retrograde = new RetrogradeWithoutGelly(env);
		//retrograde.solve(new SectorId(3,4,0,0));
		//sectorGraph.solve(new SectorId(5,5,0,0));
		retrograde.solve(new SectorId(Integer.parseInt(args[1]),Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4])));

		long start = System.currentTimeMillis();

		env.execute();
		//System.out.println(env.getExecutionPlan());

		long end = System.currentTimeMillis();
		System.out.println("time: " + (end - start) + "ms");
	}
}
