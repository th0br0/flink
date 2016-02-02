package malom;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;


/**
 * Run with the first argument being an (already created) output dir, and then the next four arguments should be 3 3 0 0
 * If you increase memory given to the JVM (-Xmx1g), performance degrades.
 * You can see that the problem is the GC, with eg. Java Mission Control.
 */

public class Solver {

	public static void main(String[] args) throws Exception {

		//System.out.println("VIGYAZAT! adjmasks atirva!");
		//System.out.println("VIGYAZAT! lose condition atirva!");


		Config.outPath = args[0];

		SectorId rootSector = new SectorId(Integer.parseInt(args[1]),Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]));


		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		//env.setParallelism(1);
		//env.getConfig().enableObjectReuse(); // seems to have only negligible impact

		PojoTypeInfo.registerCustomSerializer(GameState.class, GameState.GameStateSerializer.class);
		PojoTypeInfo.registerCustomSerializer(ValueCount.class, ValueCount.ValueCountSerializer.class);
		PojoTypeInfo.registerCustomSerializer(Value.class, Value.ValueSerializer.class);



		//Retrograde retrograde = new Retrograde(env);

//		RetrogradeWithoutGelly retrograde = new RetrogradeWithoutGelly();
//		retrograde.solve(rootSector, env);

		RetrogradeWithoutGellyUnioned.solve(rootSector, env);


		long start = System.currentTimeMillis();

		env.execute();
		//System.out.println(env.getExecutionPlan());

		long end = System.currentTimeMillis();
		System.out.println("time: " + (end - start) + "ms");
	}
}
