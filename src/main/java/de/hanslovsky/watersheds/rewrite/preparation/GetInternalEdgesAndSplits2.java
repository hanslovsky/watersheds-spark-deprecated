package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeCreator;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.DisjointSetsHashMap;
import de.hanslovsky.watersheds.rewrite.util.EdgeCheck;
import de.hanslovsky.watersheds.rewrite.util.IdService;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
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

/**
 *
 * @author Philipp Hanslovsky
 *
 * @param <K>
 */

public class GetInternalEdgesAndSplits2< K > implements
PairFunction< Tuple2< K, Tuple3< long[], float[], TLongLongHashMap > >, K, GetInternalEdgesAndSplits2.IntraBlockOutput >
{

	public static class IntraBlockOutput implements Serializable
	{

		public final long[] labels;

		public final float[] affinities;

		public final TLongLongHashMap counts;

		public final TLongLongHashMap nodeBlockAssignment;

		public final TIntArrayList splitEdges;

		public final long[] blockIds;

		public final TDoubleArrayList edges;

		public final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap;

		public IntraBlockOutput(
				final long[] labels,
				final float[] affinities,
				final TLongLongHashMap counts,
				final TLongLongHashMap nodeBlockAssignment,
				final TIntArrayList splitEdges,
				final long[] blockIds,
				final TDoubleArrayList edges,
				final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap )
		{
			super();
			this.labels = labels;
			this.affinities = affinities;
			this.counts = counts;
			this.nodeBlockAssignment = nodeBlockAssignment;
			this.splitEdges = splitEdges;
			this.blockIds = blockIds;
			this.edges = edges;
			this.nodeEdgeMap = nodeEdgeMap;
		}



	}

	private final long[] blockDim;

	private final EdgeCreator edgeCreator;

	private final EdgeMerger edgeMerger;

	private final EdgeWeight edgeWeight;

	private final EdgeCheck edgeCheck;

	private final IdService idService;

	public GetInternalEdgesAndSplits2(
			final long[] blockDim,
			final EdgeCreator edgeCreator,
			final EdgeMerger edgeMerger,
			final EdgeWeight func,
			final EdgeCheck edgeCheck,
			final IdService idService )
	{
		super();
		this.blockDim = blockDim;
		this.edgeCreator = edgeCreator;
		this.edgeMerger = edgeMerger;
		this.edgeWeight = func;
		this.edgeCheck = edgeCheck;
		this.idService = idService;
	}

	@Override
	public Tuple2< K, GetInternalEdgesAndSplits2.IntraBlockOutput > call( final Tuple2< K, Tuple3< long[], float[], TLongLongHashMap > > t ) throws Exception
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
		final Edge e = new Edge( edges, edgeMerger.dataSize() );
		final Edge dummy = new Edge( edgesDummy, edgeMerger.dataSize() );
		dummy.add( Double.NaN, 0.0, 0, 0, 1 );
		final TLongLongHashMap counts = t._2()._3();
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = new TLongObjectHashMap<>();

		final TLongLongHashMap parents = new TLongLongHashMap();
		PrepareRegionMergingCutBlocks.addEdges( innerLabels, innerAffinities, blockDim, nodeEdgeMap, e, dummy, edgeCreator, edgeMerger, parents );
		final int nIntraBlockEdges = e.size();


		final TLongLongHashMap ranks = new TLongLongHashMap();
		for ( final TLongIterator it = parents.keySet().iterator(); it.hasNext(); )
			ranks.put( it.next(), 0 );
		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, ranks, parents.size() );
		final TIntArrayList splitEdges = new TIntArrayList();
		for ( int i = 0; i < nIntraBlockEdges; ++i )
		{
			e.setIndex( i );
			e.weight( edgeWeight.weight( e, counts.get( e.from() ), counts.get( e.to() ) ) );
			if ( edgeCheck.isGoodEdge( e ) )
				dj.join( dj.findRoot( e.from() ), dj.findRoot( e.to() ) );
			else
			{
				splitEdges.add( i );
				dj.findRoot( e.from() );
				dj.findRoot( e.to() );
			}
		}



		for ( final TIntIterator it = splitEdges.iterator(); it.hasNext(); )
		{
			e.setIndex( it.next() );
			dj.findRoot( e.from() );
			dj.findRoot( e.to() );

		}

		final TLongLongHashMap rootToLocalId = new TLongLongHashMap();
		final long startId = 0;
		final TLongLongHashMap nodeBlockAssignmentLocal = assignNodesToBlocks( ( numIds ) -> startId, dj, parents, rootToLocalId );

		final TLongObjectHashMap< TLongArrayList > blockNodeMap = new TLongObjectHashMap<>();

		for ( final TLongLongIterator it = nodeBlockAssignmentLocal.iterator(); it.hasNext(); )
		{
			it.advance();
			final long b = it.value();
			if ( !blockNodeMap.contains( b ) )
				blockNodeMap.put( b, new TLongArrayList() );
			blockNodeMap.get( b ).add( it.key() );
		}

		final TLongDoubleHashMap bestWeight = new TLongDoubleHashMap();
		final TLongLongHashMap bestBlock = new TLongLongHashMap();
		for ( final TLongObjectIterator< TLongArrayList > it = blockNodeMap.iterator(); it.hasNext(); )
		{
			it.advance();
			final TLongArrayList l = it.value();
			if ( l.size() < 1 ) // expose this!
			{
				final long k = it.key();
				bestWeight.put( k, Double.MAX_VALUE );
				bestBlock.put( k, k );
			}
		}

		for ( final TIntIterator it = splitEdges.iterator(); it.hasNext(); )
		{
			e.setIndex( it.next() );
			final long fromBlock = nodeBlockAssignmentLocal.get( e.from() );
			final long toBlock = nodeBlockAssignmentLocal.get( e.to() );

			if ( fromBlock == toBlock )
				continue;

			final double w = e.weight();

			if ( bestWeight.contains( fromBlock ) && w < bestWeight.get( fromBlock ) )
			{
				bestWeight.put( fromBlock, w );
				bestBlock.put( fromBlock, toBlock );
			}
			else if ( bestWeight.contains( toBlock ) && w < bestWeight.get( toBlock ) )
			{
				bestWeight.put( toBlock, w );
				bestBlock.put( toBlock, fromBlock );
			}
		}

		for ( final TLongLongIterator it = bestBlock.iterator(); it.hasNext(); )
		{
			it.advance();
			dj.join( dj.findRoot( it.key() ), dj.findRoot( it.value() ) );
		}

		nodeBlockAssignmentLocal.transformValues( ( l ) -> dj.findRoot( l ) );

		final TLongHashSet blocks = new TLongHashSet( nodeBlockAssignmentLocal.valueCollection() );

		final int nBlocks = blocks.size();
		final long ids = idService.requestIds( nBlocks );

		final TLongLongHashMap rootToId = new TLongLongHashMap();

		final GetInternalEdgesAndSplits2.IntraBlockOutput output = new IntraBlockOutput( t._2()._1(), t._2()._2(), t._2()._3(), nodeBlockAssignmentLocal, splitEdges, rootToLocalId.values(), edges, nodeEdgeMap );

		return new Tuple2<>( t._1, output );
	}

	public static TLongLongHashMap assignNodesToBlocks(
			final IdService idService,
			final DisjointSetsHashMap dj,
			final TLongLongHashMap parents,
			final TLongLongHashMap rootToGlobalIdMapping )
	{
		final TLongLongHashMap assignment = new TLongLongHashMap();

		final long startingId = idService.requestIds( dj.setCount() );

		long id = startingId;

		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
			final long k = it.key();
			final long r = dj.findRoot( k );
			final long block;
			if ( !rootToGlobalIdMapping.contains( r ) )
			{
				block = id++;
				rootToGlobalIdMapping.put( r, block );
			}
			else
				block = rootToGlobalIdMapping.get( r );
//			if ( it.key() == 9 || it.key() == 1 )
//				System.out.println( "REGION BLOCK? " + it.key() + " " + r + " " + block + " " + dj.setCount() );
			assignment.put( k, block );
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
		final GetInternalEdgesAndSplits2< Long > splits = new GetInternalEdgesAndSplits2<>( blockDim, new EdgeCreator.SerializableCreator(), new EdgeMerger.MAX_AFFINITY_MERGER(), new EdgeWeight.FunkyWeight(), ( e ) -> e.affinity() > 0, ( n ) -> {
			return startId.getAndIncrement();
		} );

		final Tuple2< Long, IntraBlockOutput > res = splits.call( new Tuple2<>( 1l, new Tuple3<>( l, a, counts ) ) );
		final Edge e = new Edge( res._2().edges, 0 );
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