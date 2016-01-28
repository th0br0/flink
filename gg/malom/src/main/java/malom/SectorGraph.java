package malom;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This class represents the subdivision of the state space into sectors.
 * The concept of "sector graph" means a graph where the vertices are the sectors, and there is a directed edge from
 * u sector to v sector, if there is a game state in u from where you can make a movoe to a game state in v.
 */
public class SectorGraph {

	public static void createSectorFamily(SectorId u, Set<SectorId> mainSectors, Set<SectorId> chdSectors, List<SectorId> sectorFamily) {
		// The main sectors are the one or two sectors of the work unit.
		// The child sectors are those sectors, that the main sectors directly depend on.
		// The sector family is all these together.

		mainSectors.add(u);
		mainSectors.add(u.negate());

		for(SectorId mainSec: mainSectors) {
			for(SectorId chdSec: SectorGraph.graphFunc(mainSec)) {
				if(!mainSectors.contains(chdSec)) {
					chdSectors.add(chdSec);
				}
			}
		}
		assert chdSectors.size() > 0;
		sectorFamily.addAll(mainSectors);
		sectorFamily.addAll(chdSectors);
	}

	// Returns the outgoing edges from a sector
	public static List<SectorId> graphFunc(SectorId u) {
		List<SectorId> r0 = graphFunc0(u);
		List<SectorId> r = new ArrayList<>();
		for (SectorId x : r0) {
			// TODO: filtering (see std_filtered in Sporol.java)
			if(x.b >= 0) {
				x.negateInPlace(); // Game states are negated after each move. (see comment in SectorId.java)
				r.add(x);
			}
		}
		return r;
	}

	private static List<SectorId> graphFunc0(SectorId u) {
		// In the standard variant, there are two outgoing edges from each node: we either close a mill, or not.
		List<SectorId> v = new ArrayList<>();
		v.add(new SectorId(u));
		v.add(new SectorId(u));

		if (u.wf != 0) { // Can we place a stone?
			// Not taking a stone.
			v.get(0).wf--;
			v.get(0).w++;

			// Taking a stone
			v.get(1).wf--;
			v.get(1).w++;
			v.get(1).b--;
		} else { // Moving a stone
			v.get(1).b--; // Taking a stone
			// (sectorId doesn't chang when we don't take a stone) (negation will be handled by graphFunc)
		}

		return v;
	}

}
