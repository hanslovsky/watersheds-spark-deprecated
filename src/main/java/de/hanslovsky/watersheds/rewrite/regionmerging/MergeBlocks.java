package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class MergeBlocks
{

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

	public static JavaPairRDD< Long, RegionMergingInput > mergeRemappedData( final JavaPairRDD< Long, RemappedData > noRoot, final DisjointSets dj )
	{
		final JavaPairRDD< Long, RemappedData > withUpdatedBorderNodes = OutsideNodeCountRequest.request( noRoot );

		final JavaPairRDD< Long, RemappedData > withRootBlock = withUpdatedBorderNodes.mapToPair( t -> new Tuple2<>( ( long ) dj.findRoot( t._1().intValue() ), t._2() ) );

		final JavaPairRDD< Long, RemappedData > withCorrectOutsideNodes = withRootBlock.mapToPair( t -> {
			final long root = t._1();
			final TLongLongHashMap outsideNodes = new TLongLongHashMap();
			for ( final TLongLongIterator it = t._2().outsideNodes.iterator(); it.hasNext(); )
			{
				it.advance();
				final int r = dj.findRoot( ( int ) it.value() );
				if ( r != root )
					outsideNodes.put( it.key(), r );
			}

			return new Tuple2<>( root, new RemappedData( t._2().edges, t._2().counts, outsideNodes, t._2().merges, t._2.borderNodeMappings ) );

		} );

		final JavaPairRDD< Long, ArrayList< RemappedData > > aggregated = withCorrectOutsideNodes
				.aggregateByKey(
						new ArrayList<>(),
						( v1, v2 ) -> {
							v1.add( v2 );
							return v1;
						},
						( v1, v2 ) -> {
							v1.addAll( v2 );
							return v1;
						} );

		final JavaPairRDD< Long, OriginalLabelData > reduced = aggregated.mapValues( new ReduceBlock() );

		return reduced
				.mapValues( data -> {
					final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();
					final TLongLongIterator it = data.counts.iterator();
					for ( int i = 0; it.hasNext(); ++i )
					{
						it.advance();
						nodeIndexMapping.put( it.key(), i );
					}
					return new RegionMergingInput( nodeIndexMapping.size(), nodeIndexMapping, data.counts, data.outsideNodes, data.edges );
				} )
		;
	}

	public static JavaPairRDD< Long, RegionMergingInput > mergeSmallBlocks( final JavaPairRDD< Long, RegionMergingInput > rdd, final DisjointSets dj, final int minNodesPerBlock )
	{
		final List< Tuple2< Long, Long > > joins = rdd
				.mapToPair( t -> {
					final long self = t._1();
					final RegionMergingInput rmi = t._2();
					final long size = rmi.counts.size() - rmi.outsideNodes.size();

					if ( size >= minNodesPerBlock )
						return new Tuple2<>( t._1(), t._1() );
					else
					{

						final Edge e = new Edge( rmi.edges );
						double bestWeight = Double.MAX_VALUE;
						long bestNeighbor = self;
						for ( int i = 0; i < e.size(); ++i )
						{
							e.setIndex( i );
							final double w = e.weight();
							final long from = e.from();
							final long to = e.to();
							if ( rmi.outsideNodes.contains( from ) && w < bestWeight )
							{
								bestWeight = w;
								bestNeighbor = rmi.outsideNodes.get( from );
							}
							else if ( rmi.outsideNodes.contains( to ) && w < bestWeight )
							{
								bestWeight = w;
								bestNeighbor = rmi.outsideNodes.get( to );
							}
						}
						return new Tuple2<>( self, bestNeighbor );

					}
				} )
				.collect();

		for ( final Tuple2< Long, Long > j : joins )
		{
			final int r1 = dj.findRoot( j._1().intValue() );
			final int r2 = dj.findRoot( j._2().intValue() );
			if ( r1 != r2 )
				dj.join( r1, r2 );
		}

		return rdd
				.mapToPair( t -> {
					final RegionMergingInput rmi = t._2();
					final int newRoot = dj.findRoot( t._1().intValue() );
					final TLongLongHashMap outsideNodes = new TLongLongHashMap();
					for ( final TLongLongIterator oIt = rmi.outsideNodes.iterator(); oIt.hasNext(); )
					{
						oIt.advance();
						final int r = dj.findRoot( ( int ) oIt.value() );
						if ( r != newRoot )
							outsideNodes.put( oIt.key(), r );
					}

					return new Tuple2<>( ( long ) newRoot, new RegionMergingInput( rmi.nNodes, rmi.nodeIndexMapping, rmi.counts, outsideNodes, rmi.edges ) );
				} )
				.reduceByKey( ( rmi1, rmi2 ) -> {
					final int nNodes = rmi1.nNodes + rmi2.nNodes;
					rmi1.counts.putAll( rmi2.counts );
					rmi1.outsideNodes.putAll( rmi2.outsideNodes );
					rmi1.edges.addAll( rmi2.edges );

					return new RegionMergingInput( nNodes, null, rmi1.counts, rmi1.outsideNodes, rmi1.edges );
				} )
				.mapValues( rmi -> {
					final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();

					final TLongIterator cIt = rmi.counts.keySet().iterator();
					for ( int i = 0; cIt.hasNext(); ++i )
						nodeIndexMapping.put( cIt.next(), i );

					return new RegionMergingInput( rmi.counts.size(), nodeIndexMapping, rmi.counts, rmi.outsideNodes, rmi.edges );
				} );

	}

}
