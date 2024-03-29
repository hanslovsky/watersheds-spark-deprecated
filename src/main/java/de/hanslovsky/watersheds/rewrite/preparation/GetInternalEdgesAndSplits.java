package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.edge.Edge;
import de.hanslovsky.watersheds.rewrite.graph.edge.EdgeCreator;
import de.hanslovsky.watersheds.rewrite.graph.edge.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.edge.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.DisjointSetsHashMap;
import de.hanslovsky.watersheds.rewrite.util.EdgeCheck;
import de.hanslovsky.watersheds.rewrite.util.IdService;
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

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

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

	public GetInternalEdgesAndSplits(
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

		LOG.debug( "BLOCK DIM " + Arrays.toString( blockDim ) );
		final IntervalView< LongType > innerLabels = Views.offsetInterval( labels, offset, blockDim );
		final IntervalView< RealComposite< FloatType > > innerAffinities = Views.offsetInterval( affinities, offset, blockDim );

		final TDoubleArrayList edges = new TDoubleArrayList();
		final TDoubleArrayList edgesDummy = new TDoubleArrayList();
		final Edge e = new Edge( edges, edgeMerger.dataSize() );
		final Edge dummy = new Edge( edgesDummy, edgeMerger.dataSize() );
		edgeCreator.create( dummy, Double.NaN, 0.0, 0, 0, 1 );
		final TLongLongHashMap counts = t._2()._3();
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = new TLongObjectHashMap<>();

		final TLongLongHashMap parents = new TLongLongHashMap();
		PrepareRegionMergingCutBlocks.addEdges( innerLabels, innerAffinities, blockDim, nodeEdgeMap, e, dummy, edgeCreator, edgeMerger, parents );
		final int nIntraBlockEdges = e.size();

		LOG.trace( "Parents after adding edges: " + parents );

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
			{
				dj.join( dj.findRoot( e.from() ), dj.findRoot( e.to() ) );
				LOG.trace( "Join edge: " + e );
			}
			else
			{
				splitEdges.add( i );
				dj.findRoot( e.from() );
				dj.findRoot( e.to() );
				LOG.trace( "Splitting edge: " + e );
			}
		}



		LOG.debug( "Number of split edges: " + splitEdges.size() );
		LOG.trace( "Parents after finding roots: " + parents );
		for ( final TIntIterator it = splitEdges.iterator(); it.hasNext(); )
		{
			e.setIndex( it.next() );
			dj.findRoot( e.from() );
			dj.findRoot( e.to() );

		}

		final TLongLongHashMap rootToGlobalId = new TLongLongHashMap();
		final TLongLongHashMap nodeBlockAssignment = assignNodesToBlocks( idService, dj, parents, rootToGlobalId );
		LOG.trace( t._1() + " - node block assignment: " + nodeBlockAssignment );

		final GetInternalEdgesAndSplits.IntraBlockOutput output = new IntraBlockOutput( t._2()._1(), t._2()._2(), t._2()._3(), nodeBlockAssignment, splitEdges, rootToGlobalId.values(), edges, nodeEdgeMap );


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
		final GetInternalEdgesAndSplits< Long > splits = new GetInternalEdgesAndSplits<>( blockDim, new EdgeCreator.SerializableCreator(), new EdgeMerger.MAX_AFFINITY_MERGER(), new EdgeWeight.FunkyWeight(), ( e ) -> e.affinity() > 0, ( n ) -> {
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