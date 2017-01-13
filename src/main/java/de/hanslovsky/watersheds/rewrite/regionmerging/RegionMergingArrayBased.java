package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.BlockDivision;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongIntHashMap;
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

	public JavaPairRDD< Long, MergeBlocIn > run( final JavaSparkContext sc, final JavaPairRDD< Long, RegionMergingInput > rdd, final double maxThreshold, final Visitor visitor, final long nOriginalBlocks, final double tolerance )
	{

		JavaPairRDD< Long, MergeBlocIn > zeroBased = rdd.mapToPair( new ToZeroBasedIndexing<>( sc.broadcast( edgeMerger ) ) );
		System.out.println( "Starting with " + zeroBased.count() + " blocks." );

		final JavaPairRDD< Long, long[] > mapping = rdd.mapToPair( t -> {
			return new Tuple2<>( t._1(), Util.inverse( t._2().nodeIndexMapping ) );
		});

		final int nBlocks = ( int ) nOriginalBlocks;// rdd.count();

		final int[] parents = new int[ nBlocks ];
		for ( int i = 0; i < parents.length; ++i )
			parents[i] = i;
		final DisjointSets dj = new DisjointSets( parents, new int[ nBlocks ], nBlocks );

		for ( boolean hasChanged = true; hasChanged; )
		{

//			if ( zeroBased.cache().count() > 0 ) {
//				final List< Tuple2< Long, MergeBlocIn > > zb = zeroBased.filter( t -> {
//					for ( int i = 0; i < t._2().indexNodeMapping.length; ++i )
//						if ( t._2().indexNodeMapping[ i ] == 7085 )
//							return true;
//					return false;
//				}).collect();
//				for ( final Tuple2< Long, MergeBlocIn > z : zb ) {
//					final long[] indexNodeMapping = z._2().indexNodeMapping;
//					final Edge e = new Edge( z._2().g.edges() );
//					for ( int i = 0; i < e.size(); ++i ) {
//						e.setIndex( i );
//						if ( indexNodeMapping[ ( int ) e.from() ] == 7085 || indexNodeMapping[ ( int ) e.to() ] == 7085 )
//							System.out.println( "Input? " + z._1() + " " + e + " " + indexNodeMapping[ (int)e.from() ] + " " + indexNodeMapping[ (int)e.to() ]  );
//					}
//				}
//			}

			final JavaPairRDD< Long, Tuple2< MergeBlocIn, Double > > ensuredWeights = zeroBased.mapToPair( new EnsureWeights( edgeWeight ) ).cache();

			final long remainingBlocks = ensuredWeights.count();

			// why is filter necessary?
			final JavaRDD< Double > filtered = ensuredWeights.map( t -> t._2()._2() ).filter( d -> d > 0 ).cache();

			if ( filtered.count() == 0 )
				break;

			final double minimalMaximumWeight = filtered.treeReduce( ( d1, d2 ) -> Math.min( d1, d2 ) );

			final double threshold = remainingBlocks == 1 ? maxThreshold : Math.min( maxThreshold, tolerance * minimalMaximumWeight );

			System.out.println( "Merging everything up to " + threshold + " (" + maxThreshold + ")" );

			final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges = ensuredWeights
					.mapToPair( t -> new Tuple2<>( t._1(), t._2()._1() ) )
					.mapToPair( new MergeBlocArrayBased( edgeMerger, edgeWeight, threshold ) ).cache();

			hasChanged = mergedEdges.values().filter( t -> t._2().hasChanged ).count() > 0;
			if ( !hasChanged )
				break;

			System.out.println( "Visiting" );
			visitor.visit( mergedEdges, parents );
			System.out.println( "Done visiting" );

			final long[] counts = new long[ nBlocks ];

			final List< Tuple2< Long, Long > > joins = mergedEdges.map( t -> new Tuple2<>( t._1(), t._2()._1() ) ).collect();

			for ( final Tuple2< Long, Long > join : joins )
			{
				final int r1 = dj.findRoot( join._1().intValue() );
				final int r2 = dj.findRoot( join._2().intValue() );
				if ( r1 != r2 )
					dj.join( r1, r2 );
			}

			for ( int i = 0; i < nBlocks; ++i )
				++counts[ dj.findRoot( i ) ];

			final int setCount = dj.setCount();

			final Broadcast< int[] > parentsBC = sc.broadcast( parents );

			final JavaPairRDD< Long, RemappedData > remappedData = mergedEdges
					.mapToPair( new FindRootBlock( parentsBC, setCount ) )
					.mapToPair( new RemapToOriginalIndices( parentsBC, setCount ) );


			final JavaPairRDD< Long, ArrayList< RemappedData > > aggregated = remappedData
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

			final JavaPairRDD< Long, OriginalLabelData > reduced = aggregated
					.mapToPair( new ReduceBlock() )
//					.mapToPair( t -> {
//
//						final OriginalLabelData old = t._2();
//						final TDoubleArrayList edges = new TDoubleArrayList();
//						final Edge oldE = new Edge( old.edges );
//						final Edge e = new Edge( edges );
//						for ( int i = 0; i < oldE.size(); ++i )
//						{
//							oldE.setIndex( i );
//							if ( oldE.weight() >= 0.0 )
//								e.add( oldE.weight(), oldE.affinity(), oldE.from(), oldE.to(), oldE.multiplicity() );
//						}
//						return new Tuple2<>( t._1(), new OriginalLabelData( edges, old.counts, old.outsideNodes ) );
//					} );
					;

//			for ( final Tuple2< Long, OriginalLabelData > blub : reduced.cache().filter( t -> t._2().counts.contains( 7085 ) ).collect() )
//			{
//				final Edge edge7085 = new Edge( blub._2().edges );
//				System.out.println( blub._1() );
//				for ( int i = 0; i < edge7085.size(); ++i )
//				{
//					edge7085.setIndex( i );
//					if ( edge7085.from() == 7085 || edge7085.to() == 7085 )
//						System.out.println( edge7085 );
//				}
//				System.out.println();
//			}

			final JavaPairRDD< Long, RegionMergingInput > backToInput = reduced.mapToPair( t -> {
				final Long key = t._1();
				final OriginalLabelData data = t._2();

				final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();
				final TLongLongIterator it = data.counts.iterator();
				for ( int i = 0; it.hasNext(); ++i )
				{
					it.advance();
					nodeIndexMapping.put( it.key(), i );
				}

				return new Tuple2<>( key, new RegionMergingInput( nodeIndexMapping.size(), nodeIndexMapping, data.counts, data.outsideNodes, data.edges) );
			} );

			zeroBased = backToInput
//					.mapToPair( new GenerateNodeIndexMapping<>() )
					.mapToPair( new ToZeroBasedIndexing<>( sc.broadcast( edgeMerger ) ) );

			System.out.println( zeroBased.count() + " blocks remaining" );
			System.out.println();
			System.out.println();
//			if ( zeroBased.count() == 1 )
//				break;
		}

		return zeroBased;


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

	public static class GenerateNodeIndexMapping< K > implements PairFunction< Tuple2< K, RegionMergingInput >, K, RegionMergingInput >
	{

		@Override
		public Tuple2< K, RegionMergingInput > call( final Tuple2< K, RegionMergingInput > t ) throws Exception
		{
			final RegionMergingInput rmi = t._2();

			return new Tuple2<>( t._1(), rmi );
		}

	}


}
