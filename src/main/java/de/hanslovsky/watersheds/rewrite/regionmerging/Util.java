package de.hanslovsky.watersheds.rewrite.regionmerging;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;

public class Util
{

	public static TDoubleArrayList mapEdges( final TDoubleArrayList edges, final TLongIntHashMap nodeIndexMapping )
	{

		final TDoubleArrayList mappedEdges = new TDoubleArrayList();
		final Edge e = new Edge( edges );
		final Edge m = new Edge( mappedEdges );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			m.add( e.weight(), e.affinity(), nodeIndexMapping.get( e.from() ), nodeIndexMapping.get( e.to() ), e.multiplicity() );
		}

		return mappedEdges;
	}

	public static long[] mapCounts( final TLongLongHashMap counts, final TLongIntHashMap nodeIndexMapping )
	{
		final long[] mappedCounts = new long[ counts.size() ];

		for ( final TLongLongIterator it = counts.iterator(); it.hasNext(); )
		{
			it.advance();
			assert nodeIndexMapping.contains( it.key() );
			mappedCounts[ nodeIndexMapping.get( it.key() ) ] = it.value();
		}

		return mappedCounts;
	}

	public static TIntLongHashMap mapOutsideNodes( final TLongLongHashMap outsideNodes, final TLongIntHashMap nodeIndexMapping )
	{
		final TIntLongHashMap mappedOutsideNodes = new TIntLongHashMap();
		for ( final TLongLongIterator it = outsideNodes.iterator(); it.hasNext(); )
		{
			it.advance();
			assert nodeIndexMapping.contains( it.key() );
			mappedOutsideNodes.put( nodeIndexMapping.get( it.key() ), it.value() );
		}
		return mappedOutsideNodes;
	}

	public static long[] inverse( final TLongIntHashMap nodeIndexMapping )
	{
		final long[] indexNodeMapping = new long[ nodeIndexMapping.size() ];
		for ( final TLongIntIterator it = nodeIndexMapping.iterator(); it.hasNext(); )
		{
			it.advance();
			indexNodeMapping[ it.value() ] = it.key();
		}
		return indexNodeMapping;
	}

	public static TIntObjectHashMap< TLongHashSet > mapBorderNodes( final TLongObjectHashMap< TLongHashSet > borderNodes, final TLongIntHashMap nodeIndexMapping )
	{
		final TIntObjectHashMap< TLongHashSet > mappedOutsideNodes = new TIntObjectHashMap<>();
		for ( final TLongObjectIterator<TLongHashSet> it = borderNodes.iterator(); it.hasNext(); )
		{
			it.advance();
			assert nodeIndexMapping.contains( it.key() );
			mappedOutsideNodes.put( nodeIndexMapping.get( it.key() ), it.value() );
		}
		return mappedOutsideNodes;
	}

	public static void remapEdges( final Edge e, final MergeBlocOut out, final long[] map )
	{
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( out.outsideNodes.contains( ( int ) e.from() ) || out.outsideNodes.contains( ( int ) e.to() ) )
				e.weight( Double.NaN );
			e.from( map[ out.dj.findRoot( ( int ) e.from() ) ] );
			e.to( map[ out.dj.findRoot( ( int ) e.to() ) ] );
		}
	}

	public static TLongLongHashMap remapCounts( final MergeBlocOut out, final long[] map, final DisjointSets djBlock, final long root )
	{
		final TLongLongHashMap countsInBlock = new TLongLongHashMap();
		for ( int i = 0; i < out.counts.length; ++i )
		{
			final int r = out.dj.findRoot( i );
			if ( r == i )
				if ( !out.outsideNodes.contains( i ) || djBlock.findRoot( (int)out.outsideNodes.get( i ) ) != root )
					countsInBlock.put( map[ r ], out.counts[ r ] );

		}
		return countsInBlock;
	}

	public static TLongLongHashMap remapOutsideNodes( final MergeBlocOut out, final DisjointSets djBlock, final int root, final long[] map )
	{
		final TLongLongHashMap outsideNodes = new TLongLongHashMap();
		for ( final TIntLongIterator it = out.outsideNodes.iterator(); it.hasNext(); )
		{
			it.advance();
			final int r = djBlock.findRoot( ( int ) it.value() );
			// TODO is second check right?
			if ( r != root && out.dj.findRoot( it.key() ) == it.key() )
				outsideNodes.put( map[ it.key() ], r );
		}
		return outsideNodes;
	}

//	public static void remapBorderNodes( final MergeBlocOut out, final long[] map, final DisjointSets djBlock, final long blockRoot, final TLongObjectHashMap< TLongHashSet > borderNodes, final TLongLongHashMap borderNodeRoots )
//	{
//		for ( final TIntObjectIterator< TLongHashSet > it = out.borderNodes.iterator(); it.hasNext(); )
//		{
//			it.advance();
//			final TLongHashSet neighboringBlocks = it.value();
//
//			final long r = map[ out.dj.findRoot( it.key() ) ];
//			borderNodeRoots.put( map[ it.key() ], r );
//
//			if ( !borderNodes.contains( r ) )
//				borderNodes.put( r, new TLongHashSet() );
//			final TLongHashSet hs = borderNodes.get( r );
//			for ( final TLongIterator neighborIt = neighboringBlocks.iterator(); neighborIt.hasNext(); )
//			{
//				final int br = djBlock.findRoot( ( int ) neighborIt.next() );
//				if ( br != blockRoot )
//					hs.add( br );
//			}
//
//		}
//	}

}
