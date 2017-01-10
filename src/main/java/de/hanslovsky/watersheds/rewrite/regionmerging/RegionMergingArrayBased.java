package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.MergerService;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RegionMergingArrayBased
{

	public static class OriginalLabelData
	{

		public final TDoubleArrayList edges;

		public final TLongLongHashMap counts;

		public final TLongLongHashMap outsideNodes;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public OriginalLabelData( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TLongObjectHashMap< TLongHashSet > borderNodes )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.outsideNodes = outsideNodes;
			this.borderNodes = borderNodes;
		}

	}

	public static class RegionMergingInput implements Serializable
	{

		public final int nNodes;

		public final TLongIntHashMap nodeIndexMapping;

		public final TLongLongHashMap counts;

		public final TLongLongHashMap outsideNodes;

		public final TDoubleArrayList edges;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public RegionMergingInput( final int nNodes, final TLongIntHashMap nodeIndexMapping, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TDoubleArrayList edges, final TLongObjectHashMap< TLongHashSet > borderNodes )
		{
			super();
			this.nNodes = nNodes;
			this.nodeIndexMapping = nodeIndexMapping;
			this.counts = counts;
			this.outsideNodes = outsideNodes;
			this.edges = edges;
			this.borderNodes = borderNodes;
		}


	}

	public static class RemappedData implements Serializable
	{

		public final TDoubleArrayList edges;

		public final TLongLongHashMap counts;

		public final TLongLongHashMap outsideNodes;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public final TLongLongHashMap borderNodeAssignments;

		public RemappedData( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TLongObjectHashMap< TLongHashSet > borderNodes, final TLongLongHashMap borderNodeAssignments )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.outsideNodes = outsideNodes;
			this.borderNodes = borderNodes;
			this.borderNodeAssignments = borderNodeAssignments;
		}

	}

	private final EdgeMerger edgeMerger;

	private final EdgeWeight edgeWeight;

	private final MergerService mergerService;

	public RegionMergingArrayBased( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight, final MergerService mergerService )
	{
		super();
		this.edgeMerger = edgeMerger;
		this.edgeWeight = edgeWeight;
		this.mergerService = mergerService;
	}

	public void run( final JavaSparkContext sc, final JavaPairRDD< Long, RegionMergingInput > rdd, final double threshold )
	{

		JavaPairRDD< Long, MergeBlocIn > zeroBased = rdd.mapToPair( new ToZeroBasedIndexing<>() );

		final JavaPairRDD< Long, long[] > mapping = rdd.mapToPair( t -> {
			return new Tuple2<>( t._1(), Util.inverse( t._2().nodeIndexMapping ) );
		});

		final int nBlocks = ( int ) rdd.count();

		for ( boolean hasChanged = true; hasChanged; )
		{

			final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges = zeroBased.mapToPair( new MergeBlocArrayBased( edgeMerger, edgeWeight, mergerService, threshold ) ).cache();

			hasChanged = mergedEdges.values().filter( t -> t._2().hasChanged ).count() > 0;

			final int[] parents = new int[ nBlocks ];
			final DisjointSets dj = new DisjointSets( parents, new int[ nBlocks ], nBlocks );
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
					.join( mapping )
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

			final JavaPairRDD< Long, OriginalLabelData > reduced = aggregated.mapToPair( new ReduceBlock() );

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

				return new Tuple2<>( key, new RegionMergingInput( nodeIndexMapping.size(), nodeIndexMapping, data.counts, data.outsideNodes, data.edges, data.borderNodes ) );
			} );

			zeroBased = backToInput.mapToPair( new ToZeroBasedIndexing<>() );




			break;
		}


	}


}
