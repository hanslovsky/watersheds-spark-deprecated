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

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import de.hanslovsky.watersheds.rewrite.mergebloc.RootEdges;
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
		LOG.setLevel( Level.INFO );
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

		int iteration = 0;

		for ( boolean hasChanged = true; hasChanged; ++iteration )
		{

			LOG.info( "Region merging iteration " + iteration );
			final ArrayList< Object > unpersistList = new ArrayList<>();
			unpersistList.add( rdd );

			logInitialStateAtIteration( rdd, iteration );

			LOG.debug( "Current block roots: " + Arrays.toString( IntStream.range( 0, ( int ) nOriginalBlocks ).map( i -> dj.findRoot( i ) ).toArray() ) );

			final JavaPairRDD< Long, Tuple2< RegionMergingInput, Double > > ensuredWeights = rdd.mapValues( new EnsureWeights( edgeWeight ) );
			ensuredWeights.cache();
			unpersistList.add( ensuredWeights );

			final JavaPairRDD< Long, MergeBlocIn > zeroBased = ensuredWeights.mapValues( t -> t._1() ).mapValues( new ToZeroBasedIndexing( sc.broadcast( edgeMerger ) ) );
			zeroBased.cache();
			unpersistList.add( zeroBased );
			zeroBased.count();

			LOG.info( "Currently " + getNumRegions( zeroBased.values() ) + " (" + ensuredWeights.count() + ") regions (blocks) remaining." );

			// TODO why is filter necessary?
			final double threshold = getCurrentThreshold( ensuredWeights.values().map( t -> t._2() ), maxThreshold, tolerance, ensuredWeights.count() == 1 );

			LOG.info( "Merging everything up to " + threshold + " (" + maxThreshold + ")" );

			final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges = zeroBased.mapToPair( new MergeBlocArrayBased( edgeMerger, edgeWeight, threshold, regionRatio ) ).cache();
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
			findCommonBlockRoots( mergedEdges.mapValues( t -> t._1() ), dj, nOriginalBlocks );

			final JavaPairRDD< Long, Tuple2< Long, RemappedData > > remappedData = mergedEdges
					.mapValues( new RootEdges() )
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
					( ( JavaPairRDD< ?, ? > ) o ).unpersist();
				else if ( o instanceof JavaRDD )
					( ( JavaRDD< ? > ) o ).unpersist();

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

	public static < K > void logInitialStateAtIteration( final JavaPairRDD< K, RegionMergingInput > rdd, final int iteration )
	{
		if ( LOG.getLevel().isGreaterOrEqual( Level.TRACE ) )
		{
			LOG.trace( "Logging initial state at iteration " + iteration );
			rdd.map( t -> {
				final K k = t._1();
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
				final K k = t._1();
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
	}

	public static long getNumRegions( final JavaRDD< MergeBlocIn > rdd )
	{
		final int nRegions = rdd.map( t -> t.counts.length - t.outsideNodes.size() / MergeBlocArrayBased.MERGERS_ENTRY_SIZE ).reduce( ( i1, i2 ) -> i1 + i2 );
		return nRegions;
	}

	public static void findCommonBlockRoots( final JavaPairRDD< Long, Long > assignments, final DisjointSets dj, final long nBlocks )
	{
		final List< Tuple2< Long, Long > > joins = assignments.collect();

		for ( final Tuple2< Long, Long > join : joins )
		{
			final int r1 = dj.findRoot( join._1().intValue() );
			final int r2 = dj.findRoot( join._2().intValue() );
			if ( r1 != r2 )
				dj.join( r1, r2 );
		}

		for ( int i = 0; i < nBlocks; ++i )
			dj.findRoot( i );
	}

	public static double getCurrentThreshold( final JavaRDD< Double > weights, final double maxThreshold, final double tolerance, final boolean useMaxThreshold )
	{
		if ( useMaxThreshold )
			return maxThreshold;

		final JavaRDD< Double > filtered = weights.filter( d -> d >= 0 );
		//
		final double minimalMaximumWeight = filtered.count() == 0 ? maxThreshold : filtered.treeReduce( ( d1, d2 ) -> Math.min( d1, d2 ) );

		return Math.min( maxThreshold, tolerance * minimalMaximumWeight );
	}


}
