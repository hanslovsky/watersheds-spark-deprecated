package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RemapToOriginalIndices implements PairFunction< Tuple2< Long, Tuple2< Long, MergeBlocOut > >, Long, RemappedData >
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
	public Tuple2< Long, RemappedData > call( final Tuple2< Long, Tuple2< Long, MergeBlocOut > > t ) throws Exception
	{
		final MergeBlocOut out = t._2()._2();
		final int root = t._2()._1().intValue();
		final long[] map = out.indexNodeMapping;

		final int[] p = parentsBC.getValue();
		final DisjointSets djBlock = new DisjointSets( p, new int[ p.length ], setCount );

		final TDoubleArrayList edges = new TDoubleArrayList( out.edges );

		// map back edges
		Util.remapEdges( new Edge( edges ), out, map );

		// map back counts
		final TLongLongHashMap countsInBlock = Util.remapCounts( out, map, djBlock, root );

		// map back outsideNodes
		final TLongLongHashMap outsideNodes = Util.remapOutsideNodes( out, djBlock, root, map );

		final TLongArrayList merges = new TLongArrayList();
		for ( int i = 0; i < out.merges.size(); i += 4 )
		{
			merges.add( map[ ( int ) out.merges.get( i ) ] );
			merges.add( map[ ( int ) out.merges.get( i + 1 ) ] );
		}

		return new Tuple2< Long, RemappedData >( ( long ) root, new RemappedData( edges, countsInBlock, outsideNodes, merges ) );
	}

}