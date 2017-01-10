package de.hanslovsky.watersheds.rewrite;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;

public class UndirectedGraphArrayBasedTest
{

	public static void main( final String[] args )
	{
		final TLongLongHashMap parents = new TLongLongHashMap();

		final TDoubleArrayList edges = new TDoubleArrayList();
		final Edge e = new Edge( edges );
		e.add( 1.0, 2.0, 230, 235, 1 );
		e.add( 2.0, 3.0, 235, 272, 1 );
		e.add( 4.0, 1.0, 235, 236, 1 );
		e.add( 3.0, 2.9, 230, 272, 1 );

		final long[] indexNodeMapping = new long[] {
				230, 235, 236, 272
		};

		final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();
		for ( int i = 0; i < indexNodeMapping.length; ++i )
			nodeIndexMapping.put( indexNodeMapping[i], i );

		final TDoubleArrayList mappedEdges = new TDoubleArrayList();
		final Edge mappedE = new Edge( mappedEdges );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			mappedE.add( e.weight(), e.affinity(), nodeIndexMapping.get( e.from() ), nodeIndexMapping.get( e.to() ), e.multiplicity() );
		}

		final UndirectedGraphArrayBased g = new UndirectedGraphArrayBased( indexNodeMapping.length, mappedEdges );

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			System.out.println( i + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
		}
		for ( int i = 0; i < g.nodeEdgeMap().length; ++i )
			System.out.println( i + " " + g.nodeEdgeMap()[i] );

		final DisjointSets dj = new DisjointSets( indexNodeMapping.length );
		System.out.println();
		{
			final Edge edge = new Edge( mappedEdges );
			edge.setIndex( 1 );
			final int r1 = dj.findRoot( ( int ) edge.from() );
			final int r2 = dj.findRoot( ( int ) edge.to() );
			final int n = dj.join( r1, r2 );
			System.out.println( r1 + " " + r2 + " " + n );
			g.contract( edge, n, new EdgeMerger.MAX_AFFINITY_MERGER() ).clear();
		}
		System.out.println();

		for ( int i = 0; i < mappedE.size(); ++i )
		{
			mappedE.setIndex( i );
			System.out.println( i + " " + mappedE.weight() + " " + mappedE.affinity() + " " + mappedE.from() + " (" + indexNodeMapping[ ( int ) mappedE.from() ] + ") "
					+ mappedE.to() + " (" + indexNodeMapping[ ( int ) mappedE.to() ] + ") " + mappedE.multiplicity() );
		}
		for ( int i = 0; i < g.nodeEdgeMap().length; ++i )
			System.out.println( i + " " + g.nodeEdgeMap()[i] );

	}

}
