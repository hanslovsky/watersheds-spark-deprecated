package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RemapToOriginalIndices implements PairFunction< Tuple2< Long, Tuple2< Tuple2< Long, MergeBlocOut >, long[] > >, Long, RemappedData >
{

	private final Broadcast< int[] > parentsBC;

	private final int setCount;

	public RemapToOriginalIndices( final Broadcast< int[] > parentsBC, final int setCount )
	{
		super();
		this.parentsBC = parentsBC;
		this.setCount = setCount;
	}

	@Override
	public Tuple2< Long, RemappedData > call( final Tuple2< Long, Tuple2< Tuple2< Long, MergeBlocOut >, long[] > > t ) throws Exception
	{
		final MergeBlocOut out = t._2()._1()._2();
		final int root = t._2()._1()._1().intValue();
		final long[] map = t._2()._2();

		// map back edges
		Util.remapEdges( new Edge( out.g.edges() ), out, map );

		// map back counts
		final TLongLongHashMap countsInBlock = Util.remapCounts( out, map );

		final int[] p = parentsBC.getValue();
		final DisjointSets djBlock = new DisjointSets( p, new int[ p.length ], setCount );
		// map back outsideNodes
		final TLongLongHashMap outsideNodes = Util.remapOutsideNodes( out, djBlock, root, map );

		// map back borderNodes
		final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
		final TLongLongHashMap borderNodeRoots = new TLongLongHashMap();
		Util.remapBorderNodes( out, map, djBlock, root, borderNodes, borderNodeRoots );

		return new Tuple2< Long, RemappedData >( ( long ) root, new RemappedData( out.g.edges(), countsInBlock, outsideNodes, borderNodes, borderNodeRoots ) );
	}

}