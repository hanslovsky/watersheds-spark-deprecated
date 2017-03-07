package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.hanslovsky.watersheds.rewrite.graph.edge.Edge;
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

public class Util
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

	public static TDoubleArrayList mapEdges( final TDoubleArrayList edges, final TLongIntHashMap nodeIndexMapping, final int edgeDataSize )
	{

		final TDoubleArrayList mappedEdges = new TDoubleArrayList();
		final Edge e = new Edge( edges, edgeDataSize );
		final Edge m = new Edge( mappedEdges, edgeDataSize );
		LOG.trace( "Mapping to zero based: " + e.size() + " edges." );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final long f = e.from();
			final long t = e.to();
			// TODO Why is this necessary?
			if ( !nodeIndexMapping.contains( f ) || !nodeIndexMapping.contains( t ) )
			{
				LOG.trace( "NodeIndexMapping does not contain " + f + " or " + t + " " + nodeIndexMapping.toString() );
				e.setObsolete();
			}
			else
			{
				final int idx = m.add( e );
				m.setIndex( idx );
				m.from( nodeIndexMapping.get( f ) );
				m.to( nodeIndexMapping.get( t ) );
				LOG.trace( "Adding edge " + m + " from " + e );
			}
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
				e.setStale();
			e.from( map[ out.dj.findRoot( ( int ) e.from() ) ] );
			e.to( map[ out.dj.findRoot( ( int ) e.to() ) ] );
		}
	}

	public static TLongLongHashMap remapCounts( final MergeBlocOut out, final long[] map )
	{
		final TLongLongHashMap countsInBlock = new TLongLongHashMap();
		for ( int i = 0; i < out.counts.length; ++i )
		{
			final int r = out.dj.findRoot( i );
			if ( r == i )
				if ( !out.outsideNodes.contains( i ) )
					countsInBlock.put( map[ r ], out.counts[ r ] );

		}
		return countsInBlock;
	}

	public static TLongLongHashMap remapOutsideNodes( final MergeBlocOut out, final long[] map )
	{
		final TLongLongHashMap outsideNodes = new TLongLongHashMap();
		for ( final TIntLongIterator it = out.outsideNodes.iterator(); it.hasNext(); )
		{
			it.advance();
			outsideNodes.put( map[ it.key() ], it.value() );
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
