package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.BlockDivision;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RegionMergingArrayBased
{

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );

	static
	{
		LOG.setLevel( Level.TRACE );
	}

	public static interface Visitor
	{

		void visit( final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges, DisjointSets parents );

	}

	private final EdgeMerger edgeMerger;

	private final EdgeWeight edgeWeight;


	public RegionMergingArrayBased( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight )
	{
		super();
		this.edgeMerger = edgeMerger;
		this.edgeWeight = edgeWeight;
	}

	public JavaPairRDD< Long, RegionMergingInput > run(
			final JavaSparkContext sc,
			final JavaPairRDD< Long, RegionMergingInput > in,
			final double maxThreshold,
			final Visitor visitor,
			final long nOriginalBlocks,
			final double tolerance,
			final double regionRatio,
			final DisjointSets dj )
	{

		JavaPairRDD< Long, RegionMergingInput > rdd = in.mapValues( t -> t );

		final int nBlocks = ( int ) nOriginalBlocks;// rdd.count();

//		final int[] parents = new int[ nBlocks ];
//		for ( int i = 0; i < parents.length; ++i )
//			parents[i] = i;
//		final DisjointSets dj = new DisjointSets( parents, new int[ nBlocks ], nBlocks );

		int iteration = 0;

		for ( boolean hasChanged = true; hasChanged; )
		{

			LOG.info( "Region merging iteration " + iteration++ );
			final ArrayList< Object > unpersistList = new ArrayList<>();
			unpersistList.add( rdd );

			if ( LOG.getLevel().isGreaterOrEqual( Level.TRACE ) )
			{
				rdd.map( t -> {
					final Long k = t._1();
					final Edge e = new Edge( t._2().edges );
					final StringBuilder sb = new StringBuilder( "Logging initial edges for block " + k );
					for ( int i = 0; i < e.size(); ++i )
					{
						e.setIndex( i );
						sb.append( "\n" ).append( e );
					}
					LOG.trace( sb.toString() );
					return k;
				} ).count();

				rdd.map( t -> {
					final Long k = t._1();
					final StringBuilder sb = new StringBuilder( "Logging local ids to global ids for block " + k );
					final long[] reverse = new long[ t._2().nodeIndexMapping.size() ];
					for ( final TLongIntIterator it = t._2().nodeIndexMapping.iterator(); it.hasNext(); )
					{
						it.advance();
						reverse[ it.value() ] = it.key();
					}
					sb.append( "\n" ).append( t._2().nodeIndexMapping );
					sb.append( "\n" ).append( Arrays.toString( reverse ) );
					LOG.trace( sb.toString() );
					return k;
				} ).count();
			}

			LOG.debug( "Current block roots: " + Arrays.toString( IntStream.range( 0, ( int ) nOriginalBlocks ).map( i -> dj.findRoot( i ) ).toArray() ) );

			final JavaPairRDD< Long, Tuple2< RegionMergingInput, Double > > ensuredWeights = rdd.mapValues( new EnsureWeights( edgeWeight ) );
			ensuredWeights.cache();
			unpersistList.add( ensuredWeights );

			final JavaPairRDD< Long, MergeBlocIn > zeroBased = ensuredWeights.mapValues( t -> t._1() ).mapValues( new ToZeroBasedIndexing( sc.broadcast( edgeMerger ) ) );
			zeroBased.cache();
			unpersistList.add( zeroBased );
			zeroBased.count();

//			final int nRegions = zeroBased.map( t -> t._2().counts.length - t._2().outsideNodes.size() / MergeBlocArrayBased.MERGERS_ENTRY_SIZE ).reduce( ( i1, i2 ) -> i1 + i2 );

//			LOG.info( "Currently " + nRegions + " (" + ensuredWeights.count() + ") regions (blocks) remaining." );

			final long remainingBlocks = ensuredWeights.count();

			// TODO why is filter necessary?
			final JavaRDD< Double > filtered = ensuredWeights.map( t -> t._2()._2() ).filter( d -> d >= 0 ).cache();
			unpersistList.add( filtered );
//
			final double minimalMaximumWeight = filtered.count() == 0 ? maxThreshold : filtered.treeReduce( ( d1, d2 ) -> Math.min( d1, d2 ) );

			final double threshold = remainingBlocks == 1 ? maxThreshold : Math.min( maxThreshold, tolerance * minimalMaximumWeight );

			LOG.info( "Merging everything up to " + threshold + " (" + maxThreshold + ")" );

			final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges = zeroBased
					.mapToPair( new MergeBlocArrayBased( edgeMerger, edgeWeight, threshold, regionRatio ) ).cache();
			unpersistList.add( mergedEdges );
			mergedEdges.count();

			final long nMerges = mergedEdges.mapValues( o -> ( long ) o._2().merges.size() / 4 ).values().treeReduce( ( l1, l2 ) -> l1 + l2 );

			LOG.info( "Contracted " + nMerges + " edges." );

			hasChanged = mergedEdges.values().filter( t -> t._2().hasChanged ).count() > 0;
			if ( !hasChanged )
				break;

			LOG.info( "Visiting" );
			visitor.visit( mergedEdges, dj );
			LOG.info( "Done visiting" );

			// Update counts of outside nodes

			final List< Tuple2< Long, Long > > joins = mergedEdges.map( t -> new Tuple2<>( t._1(), t._2()._1() ) ).collect();

			for ( final Tuple2< Long, Long > join : joins )
			{
				final int r1 = dj.findRoot( join._1().intValue() );
				final int r2 = dj.findRoot( join._2().intValue() );
				if ( r1 != r2 )
					dj.join( r1, r2 );
			}

			for ( int i = 0; i < nBlocks; ++i )
				dj.findRoot( i );

			final int setCount = dj.setCount();

//			final Broadcast< int[] > parentsBC = sc.broadcast( parents );

			final JavaPairRDD< Long, Tuple2< Long, RemappedData > > remappedData = mergedEdges
					.mapValues( input -> {
						final Edge e = new Edge( input._2().edges );
						final DisjointSets map = input._2().dj;
						final long[] counts = input._2().counts;
						for ( int i = 0; i < e.size(); ++i )
						{
							e.setIndex( i );
							final int from = ( int ) e.from();
							final int to = ( int ) e.to();

							final int rFrom = map.findRoot( from );
							final int rTo = map.findRoot( to );

							if ( rFrom != from )
								counts[ from ] = 0;

							if ( rTo != to )
								counts[ to ] = 0;

							e.from( rFrom );
							e.to( rTo );

							if ( rFrom == rTo )
								e.weight( -1.0 );

						}
						return input;
					} )
					.mapValues( new RemapToOriginalIndices() );
			remappedData.cache();
			unpersistList.add( remappedData );
			remappedData.count();
			LOG.info( "Done remapping." );

			final JavaPairRDD< Long, RemappedData > noRoot = remappedData.mapValues( t -> t._2() );

			rdd = MergeBlocks.mergeRemappedData( noRoot, dj ).cache();
			rdd.count();

			LOG.info( "" );

			for ( final Object o : unpersistList )
				if ( o instanceof JavaPairRDD )
					((JavaPairRDD)o).unpersist();
				else if ( o instanceof JavaRDD )
					( ( JavaRDD ) o ).unpersist();

		}

		return rdd;


	}

	public static JavaPairRDD< Long, RegionMergingInput > fromBlockDivision( final JavaPairRDD< Long, BlockDivision > rdd ) {
		return rdd.mapToPair( t -> {
			final BlockDivision bd = t._2();
			final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();
			final TLongIterator cIt = bd.counts.keySet().iterator();
			for ( int i = 0; cIt.hasNext(); ++i )
				nodeIndexMapping.put( cIt.next(), i );
			return new Tuple2<>( t._1(), new RegionMergingInput( bd.counts.size(), nodeIndexMapping, bd.counts, bd.outsideNodes, bd.edges ) );
		});
	}

	public static class GenerateNodeIndexMapping implements Function< RegionMergingInput, RegionMergingInput >
	{

		@Override
		public RegionMergingInput call( final RegionMergingInput rmi ) throws Exception
		{
			return rmi;
		}

	}


}
