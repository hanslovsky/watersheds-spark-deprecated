package de.hanslovsky.watersheds.graph;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;

public class UndirectedGraphTest
{

	public static void main( final String[] args )
	{

		final TLongLongHashMap parents = new TLongLongHashMap();
		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, new TLongLongHashMap(), 0 );

		final TDoubleArrayList edges = new TDoubleArrayList();
		final Edge e = new Edge( edges );
		e.add( 1.0, 2.0, 230, 235, 1 );
		e.add( 2.0, 3.0, 235, 272, 1 );
		e.add( 4.0, 1.0, 235, 236, 2 );
		dj.findRoot( 230 );
		dj.findRoot( 235 );
		dj.findRoot( 236 );
		dj.findRoot( 272 );

		final UndirectedGraph g = new UndirectedGraph( edges, MergeBloc.DEFAULT_EDGE_MERGER );

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );;
			System.out.println( i + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
		}
		for ( final TLongObjectIterator< TLongIntHashMap > it = g.nodeEdgeMap().iterator(); it.hasNext(); )
		{
			it.advance();
			System.out.println( it.key() + " " + it.value() );
		}

		System.out.println();
		{
			final long r1 = dj.findRoot( 235 );
			final long r2 = dj.findRoot( 272 );
			final long n = dj.join( r1, r2 );
			System.out.println( r1 + " " + r2 + " " + n );
			g.contract( 1, n, new TLongLongHashMap(), new RegionMerging.CountOverSquaredSize() );
		}
		System.out.println();

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );;
			System.out.println( i + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
		}
		for ( final TLongObjectIterator< TLongIntHashMap > it = g.nodeEdgeMap().iterator(); it.hasNext(); )
		{
			it.advance();
			System.out.println( it.key() + " " + it.value() );
		}

		System.out.println();
		{
			final long r1 = dj.findRoot( 235 );
			final long r2 = dj.findRoot( 230 );
			final long n = dj.join( r1, r2 );
			System.out.println( r1 + " " + r2 + " " + n );
			g.contract( 4, n, new TLongLongHashMap(), new RegionMerging.CountOverSquaredSize() );
		}
		System.out.println();

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );;
			System.out.println( i + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
		}
		for ( final TLongObjectIterator< TLongIntHashMap > it = g.nodeEdgeMap().iterator(); it.hasNext(); )
		{
			it.advance();
			System.out.println( it.key() + " " + it.value() );
		}

		System.out.println( parents );

	}

}
