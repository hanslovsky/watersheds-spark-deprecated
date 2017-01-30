package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.BlockDivision;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RegionMergingArrayBased
{
	public static interface Visitor
	{

		void visit( final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges, int[] parents );

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
			final double tolerance )
	{

		JavaPairRDD< Long, RegionMergingInput > rdd = in.mapValues( t -> t );

		final int nBlocks = ( int ) nOriginalBlocks;// rdd.count();

		final int[] parents = new int[ nBlocks ];
		for ( int i = 0; i < parents.length; ++i )
			parents[i] = i;
		final DisjointSets dj = new DisjointSets( parents, new int[ nBlocks ], nBlocks );

		for ( boolean hasChanged = true; hasChanged; )
		{
			final ArrayList< Object > unpersistList = new ArrayList<>();
			final JavaPairRDD< Long, Tuple2< RegionMergingInput, Double > > ensuredWeights = rdd.mapValues( new EnsureWeights( edgeWeight ) );
			ensuredWeights.cache();
			unpersistList.add( ensuredWeights );

			final JavaPairRDD< Long, MergeBlocIn > zeroBased = ensuredWeights.mapValues( t -> t._1() ).mapValues( new ToZeroBasedIndexing( sc.broadcast( edgeMerger ) ) );
			zeroBased.cache();
			unpersistList.add( zeroBased );

			final int nRegions = zeroBased.map( t -> t._2().counts.length - t._2().outsideNodes.size() ).reduce( ( i1, i2 ) -> i1 + i2 );

			System.out.println( "Currently " + ensuredWeights.count() + " blocks remaining." );

			final long remainingBlocks = ensuredWeights.count();

			// why is filter necessary?
			final JavaRDD< Double > filtered = ensuredWeights.map( t -> t._2()._2() ).filter( d -> d > 0 ).cache();
			unpersistList.add( filtered );

			if ( filtered.count() == 0 )
				break;

			final double minimalMaximumWeight = filtered.treeReduce( ( d1, d2 ) -> Math.min( d1, d2 ) );

			final double threshold = remainingBlocks == 1 ? maxThreshold : Math.min( maxThreshold, tolerance * minimalMaximumWeight );

			System.out.println( "Merging everything up to " + threshold + " (" + maxThreshold + ")" );

			final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges = zeroBased
					.mapToPair( new MergeBlocArrayBased( edgeMerger, edgeWeight, threshold ) ).cache();
			unpersistList.add( mergedEdges );

			hasChanged = mergedEdges.values().filter( t -> t._2().hasChanged ).count() > 0;
			if ( !hasChanged )
				break;

			System.out.println( "Visiting" );
			visitor.visit( mergedEdges, parents );
			System.out.println( "Done visiting" );

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

			final Broadcast< int[] > parentsBC = sc.broadcast( parents );

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
			System.out.println( "Done remapping." );

			final JavaPairRDD< Long, RemappedData > noRoot = remappedData.mapValues( t -> t._2() );

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

			rdd = reduced
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
					.mapValues( new GenerateNodeIndexMapping() );

//			final JavaPairRDD< Long, MergeBlocIn > bti = backToInput
////					.mapToPair( new GenerateNodeIndexMapping<>() )
//					.mapToPair( new ToZeroBasedIndexing<>( sc.broadcast( edgeMerger ) ) );
//			bti.persist( zeroBased.getStorageLevel() );
//			zeroBased.unpersist();
//			zeroBased = bti;

//			if ( zeroBased.count() == 1 )
//				break;
			System.out.println();

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
