package de.hanslovsky.watersheds.regionmerging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import de.hanslovsky.watersheds.HashableLongArray;
import de.hanslovsky.watersheds.graph.Edge;
import de.hanslovsky.watersheds.graph.EdgeMerger;
import de.hanslovsky.watersheds.graph.Function;
import de.hanslovsky.watersheds.graph.IdService;
import de.hanslovsky.watersheds.graph.MergeBloc;
import de.hanslovsky.watersheds.graph.RegionMerging;
import de.hanslovsky.watersheds.graph.UndirectedGraph;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.array.ArrayRandomAccess;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.CompositeView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class GetInternalEdgesAndSplits< K > implements
PairFunction< Tuple2< K, Tuple3< long[], float[], TLongLongHashMap > >, K, GetInternalEdgesAndSplits.IntraBlockOutput >
{

	public static class IntraBlockOutput
	{

		public final long[] labels;

		public final float[] affinities;

		public final TLongLongHashMap counts;

		public final UndirectedGraph g;

		public final TLongLongHashMap nodeBlockAssignment;

		public final TIntArrayList splitEdges;

		public final long[] blockIds;

		public IntraBlockOutput( final long[] labels, final float[] affinities, final TLongLongHashMap counts, final UndirectedGraph g, final TLongLongHashMap nodeBlockAssignment, final TIntArrayList splitEdges, final long[] blockIds )
		{
			super();
			this.labels = labels;
			this.affinities = affinities;
			this.counts = counts;
			this.g = g;
			this.nodeBlockAssignment = nodeBlockAssignment;
			this.splitEdges = splitEdges;
			this.blockIds = blockIds;
		}



	}

	private final long[] blockDim;

	private final EdgeMerger edgeMerger;

	private final Function func;

	private final EdgeCheck edgeCheck;

	private final IdService idService;

	public GetInternalEdgesAndSplits(
			final long[] blockDim,
			final EdgeMerger edgeMerger,
			final Function func,
			final EdgeCheck edgeCheck,
			final IdService idService )
	{
		super();
		this.blockDim = blockDim;
		this.edgeMerger = edgeMerger;
		this.func = func;
		this.edgeCheck = edgeCheck;
		this.idService = idService;
	}

	@Override
	public Tuple2< K, GetInternalEdgesAndSplits.IntraBlockOutput > call( final Tuple2< K, Tuple3< long[], float[], TLongLongHashMap > > t ) throws Exception
	{
		final long[] blockDim = this.blockDim;// .getValue();
		final long[] extendedBlockDim = PrepareRegionMergingCutBlocks.padDimensions( blockDim, 1 );
		final long[] extendedAffinitiesBlockDim = PrepareRegionMergingCutBlocks.getAffinityDims( extendedBlockDim );
		final long[] offset = new long[ blockDim.length ];
		Arrays.fill( offset, 1 );

		final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( t._2()._1(), extendedBlockDim );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affinities =
				Views.collapseReal( ArrayImgs.floats( t._2()._2(), extendedAffinitiesBlockDim ) );

		final IntervalView< LongType > innerLabels = Views.offsetInterval( labels, offset, blockDim );
		final IntervalView< RealComposite< FloatType > > innerAffinities = Views.offsetInterval( affinities, offset, blockDim );

		final TDoubleArrayList edges = new TDoubleArrayList();
		final TDoubleArrayList edgesDummy = new TDoubleArrayList();
		final Edge e = new Edge( edges );
		final Edge dummy = new Edge( edgesDummy );
		dummy.add( Double.NaN, 0.0, 0, 0, 1 );
		final TLongLongHashMap counts = t._2()._3();
		final UndirectedGraph g = new UndirectedGraph( edges, edgeMerger );
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = g.nodeEdgeMap();

		final TLongLongHashMap parents = new TLongLongHashMap();
		PrepareRegionMergingCutBlocks.addEdges( innerLabels, innerAffinities, blockDim, g, nodeEdgeMap, e, dummy, edgeMerger, parents );
		final int nIntraBlockEdges = e.size();

		final TLongLongHashMap ranks = new TLongLongHashMap();
		for ( final TLongIterator it = parents.keySet().iterator(); it.hasNext(); )
			ranks.put( it.next(), 0 );
		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, ranks, parents.size() );
		final TIntArrayList splitEdges = new TIntArrayList();
		for ( int i = 0; i < nIntraBlockEdges; ++i )
		{
			e.setIndex( i );
			e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			if ( edgeCheck.isGoodEdge( e ) )
				dj.join( dj.findRoot( e.from() ), dj.findRoot( e.to() ) );
			else
				splitEdges.add( i );
		}

		for ( final TIntIterator it = splitEdges.iterator(); it.hasNext(); )
		{
			e.setIndex( it.next() );
			dj.findRoot( e.from() );
			dj.findRoot( e.to() );
		}

		final TLongLongHashMap rootToGlobalId = new TLongLongHashMap();
		final TLongLongHashMap nodeBlockAssignment = assignNodesToBlocks( idService, dj, parents, rootToGlobalId );// new

		for ( final TLongIterator vIt = rootToGlobalId.valueCollection().iterator(); vIt.hasNext(); )
			if ( vIt.next() < 0 ) {
				System.out.println( t._1() instanceof HashableLongArray ? Arrays.toString( ( ( HashableLongArray ) t._1() ).getData() ) : t._1() + " " + "vIt < 0!" );
				System.out.println( rootToGlobalId );
				System.exit( -1 );
			}
		final GetInternalEdgesAndSplits.IntraBlockOutput output = new IntraBlockOutput( t._2()._1(), t._2()._2(), t._2()._3(), g, nodeBlockAssignment, splitEdges, rootToGlobalId.values() );


		return new Tuple2<>( t._1, output );
	}

	public static TLongLongHashMap assignNodesToBlocks(
			final IdService idService,
			final DisjointSetsHashMap dj,
			final TLongLongHashMap parents,
			final TLongLongHashMap rootToGlobalIdMapping )
	{
		final TLongLongHashMap assignment = new TLongLongHashMap();

		long id = idService.requestIds( dj.setCount() );
		if ( id < 0 )
		{
			System.out.println( "HOW CAN ID BE SMALLER THAN ZERO? " + id );
			System.exit( -9001 );
		}

		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
			final long r = it.value();
			final long block;
			if ( !rootToGlobalIdMapping.contains( r ) )
			{
				block = id++;
				rootToGlobalIdMapping.put( r, block );
			}
			else
				block = rootToGlobalIdMapping.get( r );
			assignment.put( it.key(), block );
		}

		return assignment;
	}

	public static void main( final String[] args ) throws Exception
	{
		final long[] blockDim = new long[] { 3, 3 };
		final long[] paddedBlockDim = PrepareRegionMergingCutBlocks.padDimensions( blockDim, 1 );
		final long[] paddedAffinityDim = PrepareRegionMergingCutBlocks.getAffinityDims( paddedBlockDim );

		final long[] l = new long[ ( int ) Intervals.numElements( paddedBlockDim ) ];
		final float[] a = new float[ ( int ) Intervals.numElements( paddedAffinityDim ) ];

		final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( l, paddedBlockDim );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affinities = Views.collapseReal( ArrayImgs.floats( a, paddedAffinityDim ) );

		final TLongLongHashMap counts = new TLongLongHashMap();

		for ( int i = 0; i < l.length; ++i )
		{
			l[ i ] = i + 1;
			counts.put( i + 1, 1 );
		}
		Arrays.fill( a, 1.0f );
		final CompositeView< FloatType, RealComposite< FloatType > >.CompositeRandomAccess aRa = affinities.randomAccess();
		aRa.setPosition( new long[] { 1, 1 } );
		aRa.get().get( 0 ).set( 0.0f );
		aRa.fwd( 0 );
		aRa.get().get( 1 ).set( 0.0f );
		aRa.setPosition( new long[] { 2, 2 } );
		aRa.get().get( 0 ).set( 0.0f );
		aRa.fwd( 0 );
		aRa.get().get( 1 ).set( 0.0f );

		final ArrayRandomAccess< LongType > lRa = labels.randomAccess();
		lRa.setPosition( new long[] { 1, 1 } );
		System.out.println( lRa.get() );
		lRa.fwd( 0 );
		System.out.println( lRa.get() );

		final AtomicLong startId = new AtomicLong( l.length );
		final GetInternalEdgesAndSplits< Long > splits = new GetInternalEdgesAndSplits<>( blockDim, MergeBloc.DEFAULT_EDGE_MERGER, new RegionMerging.CountOverSquaredSize(), ( e ) -> e.affinity() > 0, ( n ) -> {
			return startId.getAndIncrement();
		} );

		final Tuple2< Long, IntraBlockOutput > res = splits.call( new Tuple2<>( 1l, new Tuple3<>( l, a, counts ) ) );
		final Edge e = new Edge( res._2().g.edges() );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			System.out.println( e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() );
		}

		System.out.println( res._2().splitEdges );
		for ( int i = 0; i < res._2().splitEdges.size(); ++i )
		{
			e.setIndex( res._2().splitEdges.get( i ) );
			System.out.println( e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() );
		}

		System.out.println( Arrays.toString( res._2().blockIds ) );

		System.out.println( res._2().counts );

		System.out.println( res._2().nodeBlockAssignment );


	}

}