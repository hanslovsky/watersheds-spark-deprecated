package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class RemapToOriginalIndices implements PairFunction< Tuple2< Long, Tuple2< Long, MergeBlocOut > >, Long, Tuple2< Long, RemappedData > >
{

	@Override
	public Tuple2< Long, Tuple2< Long, RemappedData > > call( final Tuple2< Long, Tuple2< Long, MergeBlocOut > > t ) throws Exception
	{
		final MergeBlocOut out = t._2()._2();
		final long[] map = out.indexNodeMapping;

		final TDoubleArrayList edges = new TDoubleArrayList( out.edges );

		// map back edges
		Util.remapEdges( new Edge( edges ), out, map );

		// map back counts
		final TLongLongHashMap countsInBlock = Util.remapCounts( out, map );

		// map back outsideNodes
		final TLongLongHashMap outsideNodes = Util.remapOutsideNodes( out, map );

		// map back borderNodeMappings
		final TLongLongHashMap borderNodeMappings = new TLongLongHashMap();
		final DisjointSetsHashMap borderNodeUnionFind = new DisjointSetsHashMap( t._2()._2().borderNodeMappings, new TLongLongHashMap(), 0 );
		for ( final long key : t._2()._2().borderNodeMappings.keys() )
			borderNodeMappings.put( map[ ( int ) key ], map[ ( int ) borderNodeUnionFind.findRoot( key ) ] );

		final TLongArrayList merges = new TLongArrayList();
		for ( int i = 0; i < out.merges.size(); i += 4 )
		{
			merges.add( map[ ( int ) out.merges.get( i ) ] );
			merges.add( map[ ( int ) out.merges.get( i + 1 ) ] );
		}

		return new Tuple2<>( t._1(), new Tuple2<>( t._2()._1(), new RemappedData( edges, countsInBlock, outsideNodes, merges, borderNodeMappings ) ) );
	}

}