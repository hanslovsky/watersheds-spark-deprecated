package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.EdgeCheck;
import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.IdService;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.Cursor;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class PrepareRegionMergingCutBlocks
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

	public static class BlockDivision implements Serializable
	{
		final public TLongLongHashMap counts;

		final public TLongLongHashMap outsideNodes;

		final public TDoubleArrayList edges;

		final public TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap;

		final public Edge e1, e2;

		public BlockDivision( final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TLongObjectHashMap< TLongHashSet > borderNodes, final TDoubleArrayList edges, final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap )
		{
			super();
			this.counts = counts;
			this.outsideNodes = outsideNodes;
			this.edges = edges;
			this.nodeEdgeMap = nodeEdgeMap;
			this.e1 = new Edge( this.edges );
			this.e2 = new Edge( this.edges );
		}

	}

	public static Tuple2< JavaPairRDD< Long, BlockDivision >, JavaPairRDD< HashableLongArray, long[] > > run(
			final JavaSparkContext sc,
			final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksWithLabelsAffinitiesAndCounts,
			final Broadcast< long[] > dim,
			final Broadcast< long[] > blockDim,
			final EdgeMerger edgeMerger,
			final EdgeWeight edgeWeight,
			final EdgeCheck edgeCheck,
			final IdService idService )
	{
		final JavaPairRDD< HashableLongArray, GetInternalEdgesAndSplits.IntraBlockOutput > allEdges =
				blocksWithLabelsAffinitiesAndCounts.mapToPair( new GetInternalEdgesAndSplits<>( blockDim.getValue(), edgeMerger, edgeWeight, edgeCheck, idService ) ).cache();

		LOG.debug( "After geting internal edges and splits: " + allEdges.count() + " blocks." );

		final JavaPairRDD< HashableLongArray, long[] > initialBlockContains = allEdges
				.mapToPair( t -> {
					final TLongLongHashMap nba = t._2().nodeBlockAssignment;
					final TLongHashSet blocks = new TLongHashSet( nba.valueCollection() );
					return new Tuple2<>( t._1(), blocks.toArray() );
				} );

		final JavaPairRDD< HashableLongArray, GetExternalEdges.BlockOutput > interBlockEdges =
				allEdges.mapToPair( new GetExternalEdges( blockDim, dim, edgeMerger ) ).cache();
		interBlockEdges.count();

		LOG.debug( "After geting inter-block edges: " + interBlockEdges.count() + " blocks." );

		final TLongLongHashMap filteredNodeBlockAssignments = interBlockEdges
				.map( t -> t._2().filteredNodeBlockAssignment )
				.reduce( ( m1, m2 ) -> {
					m1.putAll( m2 );
					return m1;
				} );


		final JavaPairRDD< Long, BlockDivision > mergeBlocs = interBlockEdges
				.values()
				.map( new MapNodesAndSplitBlocks( sc.broadcast( filteredNodeBlockAssignments ), edgeMerger ) )
				.flatMapToPair( new FlattenInputs() )
				.mapToPair( t -> {
					final Edge e = new Edge( t._2().edges );
					final TLongLongHashMap counts = t._2().counts;
					for ( int i = 0; i < e.size(); ++i )
					{
						e.setIndex( i );
						e.weight( edgeWeight.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
						if ( !counts.contains( e.from() ) || !counts.contains( e.to() ) )
							throw new RuntimeException( "(" + t._1() + ") No counts for " + e.from() + " or " + e.to() + " " + counts.get( e.from() ) + " " + counts.get( e.to() ) );
					}
					return t;
				} ).cache();

		LOG.debug( "Block divisions: " + mergeBlocs.count() + " blocks." );

		return new Tuple2<>( mergeBlocs, initialBlockContains );
	}

	public static class GetExternalEdges implements PairFunction< Tuple2< HashableLongArray, GetInternalEdgesAndSplits.IntraBlockOutput >, HashableLongArray, GetExternalEdges.BlockOutput >
	{
		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		{
			LOG.setLevel( Level.TRACE );
		}

		public static class BlockOutput
		{
			public final TLongLongHashMap counts;


			public final TLongLongHashMap nodeBlockAssignment;

			public final TLongLongHashMap filteredNodeBlockAssignment;

			public final TIntArrayList splitEdges;

			public final TLongObjectHashMap< TLongHashSet > borderNodesToOutsideNodes;

			public final long[] blockIds;

			public final int numberOfInternalEdges;

			public final TDoubleArrayList edges;

			public final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap;

			public BlockOutput(
					final TLongLongHashMap counts,
					final TLongLongHashMap nodeBlockAssignment,
					final TLongLongHashMap filteredNodeBlockAssignment,
					final TIntArrayList splitEdges,
					final TLongObjectHashMap< TLongHashSet > borderNodesToOutsideNodes,
					final long[] blockIds,
					final int numberOfInternalEdges,
					final TDoubleArrayList edges,
					final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap )
			{
				super();
				this.counts = counts;
				this.nodeBlockAssignment = nodeBlockAssignment;
				this.filteredNodeBlockAssignment = filteredNodeBlockAssignment;
				this.splitEdges = splitEdges;
				this.borderNodesToOutsideNodes = borderNodesToOutsideNodes;
				this.blockIds = blockIds;
				this.numberOfInternalEdges = numberOfInternalEdges;
				this.edges = edges;
				this.nodeEdgeMap = nodeEdgeMap;
			}


		}

		private final Broadcast< long[] > blockDim;

		private final Broadcast< long[] > dim;

		private final EdgeMerger edgeMerger;

		public GetExternalEdges( final Broadcast< long[] > blockDim, final Broadcast< long[] > dim, final EdgeMerger edgeMerger )
		{
			super();
			this.blockDim = blockDim;
			this.dim = dim;
			this.edgeMerger = edgeMerger;
		}

		@Override
		public Tuple2< HashableLongArray, BlockOutput > call( final Tuple2< HashableLongArray, GetInternalEdgesAndSplits.IntraBlockOutput > t ) throws Exception
		{
			final long[] dim = this.dim.getValue();
			final long[] blockDim = this.blockDim.getValue();
			final long[] extendedBlockDim = padDimensions( blockDim, 1 );
			final long[] extendedAffinitiesBlockDim = getAffinityDims( extendedBlockDim );
			final long[] offset = new long[ blockDim.length ];
			Arrays.fill( offset, 1 );
			final long[] numBlocksByDimension = new long[ blockDim.length ];
			final long[] pos = t._1().getData();
			for ( int d = 0; d < blockDim.length; ++d )
				numBlocksByDimension[ d ] = ( long ) Math.ceil( dim[ d ] * 1.0 / blockDim[ d ] );
			final long[] blockIndices = new long[ pos.length ];
			for ( int d = 0; d < pos.length; ++d )
				blockIndices[ d ] = pos[ d ] / blockDim[ d ];

			final GetInternalEdgesAndSplits.IntraBlockOutput o = t._2();
			final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( o.labels, extendedBlockDim );
			final CompositeIntervalView< FloatType, RealComposite< FloatType > > affinities =
					Views.collapseReal( ArrayImgs.floats( o.affinities, extendedAffinitiesBlockDim ) );

			final Edge e = new Edge( o.edges );
			final int numberOfInternalEdges = e.size();
			final Edge dummy = new Edge( new TDoubleArrayList() );
			dummy.add( Double.NaN, 0.0, 0, 0, 1 );
			final TLongObjectHashMap< TLongHashSet > borderNodesToOutsideNodes = new TLongObjectHashMap<>();
			for ( int d = 0; d < blockDim.length; ++d )
			{
				blockIndices[ d ] -= 1;
				if ( blockIndices[ d ] >= 0 )
				{
					final long outer = 0;
					final long inner = outer + 1;
					addEdgesFromNeighborBlocks(
							labels, affinities, d, inner, outer, o.nodeEdgeMap, e, dummy,
							edgeMerger, borderNodesToOutsideNodes, blockDim );

				}
				blockIndices[ d ] += 2;
				if ( blockIndices[ d ] < numBlocksByDimension[ d ] )
				{
					final long inner = labels.max( d ) - 1;
					final long outer = inner + 1;
					addEdgesFromNeighborBlocks(
							labels, affinities, d, inner, outer, o.nodeEdgeMap, e, dummy,
							edgeMerger, borderNodesToOutsideNodes, blockDim );
				}
				blockIndices[ d ] -= 1;
			}
			final TLongLongHashMap nodesToBlockFiltered = filterBlockAssignments( o.nodeBlockAssignment, borderNodesToOutsideNodes.keySet() );

			return new Tuple2<>(
					t._1(),
					new BlockOutput( o.counts, o.nodeBlockAssignment, nodesToBlockFiltered, o.splitEdges, borderNodesToOutsideNodes, o.blockIds, numberOfInternalEdges, o.edges, o.nodeEdgeMap ) );
		}

	}

	public static class FlattenInputs implements PairFlatMapFunction< TLongObjectHashMap< BlockDivision >, Long, BlockDivision >
	{
		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		{
			LOG.setLevel( Level.INFO );
		}

		@Override
		public Iterator< Tuple2< Long, BlockDivision > > call( final TLongObjectHashMap< BlockDivision > t ) throws Exception
		{
			final Iterator< Tuple2< Long, BlockDivision > > it = new Iterator< Tuple2< Long, BlockDivision > >()
			{

				final TLongObjectIterator< BlockDivision > localIt = t.iterator();

				@Override
				public boolean hasNext()
				{
					return localIt.hasNext();
				}

				@Override
				public Tuple2< Long, BlockDivision > next()
				{
					localIt.advance();
					return new Tuple2<>( localIt.key(), localIt.value() );
				}

			};
			return it;
		}

	}

	static < T extends RealType< T > > void addEdges(
			final IntervalView< LongType > labels,
			final IntervalView< RealComposite< T > > affinities,
			final long[] blockDim,
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap,
			final Edge e,
			final Edge dummy,
			final EdgeMerger edgeMerger,
			final TLongLongHashMap parents )
	{
		final RandomAccess< LongType > labelsAccess = labels.randomAccess();
		final Cursor< RealComposite< T > > affinitiesCursor = affinities.cursor();
		final long[] blockMax = Arrays.stream( blockDim ).map( v -> v - 1 ).toArray();
		while ( affinitiesCursor.hasNext() )
		{
			final RealComposite< T > affinity = affinitiesCursor.next();
			labelsAccess.setPosition( affinitiesCursor );
			final long label = labelsAccess.get().get();
			if ( label >= 0 )
			{
				LOG.trace( "Adding label to parents map: " + label );
				parents.put( label, label );
			}
			for ( int d = 0; d < blockMax.length; ++d )
				// TODO is blockMax[ d ] - 1 bug? should it be blockMax[ d ]?
				// If so -> WHY?
				// Should it be blockMax[ d ]
				// YES IT SHOULD BE BECAUSE WE COMPARE TO blockMax[ d ]!!
				if ( labelsAccess.getLongPosition( d ) < blockMax[ d ] )
				{
					final double aff = affinity.get( d ).getRealDouble();
					if ( !Double.isNaN( aff ) )
					{
						labelsAccess.fwd( d );
						final long otherLabel = labelsAccess.get().get();
						if ( otherLabel != label )
						{
							parents.put( otherLabel, otherLabel );
							addEdge( label, otherLabel, aff, nodeEdgeMap, e, dummy, edgeMerger );
							if ( LOG.isEnabledFor( Level.TRACE ) && ( label == -1 || otherLabel == -1 ) )
								LOG.trace( "Added edge: " + label + " " + otherLabel + " " + aff + " " + new Point( affinitiesCursor ) + " " + new Point( labelsAccess ) );
						}
						labelsAccess.bck( d );
					}
				}
		}
	}

	private static int addEdge(
			final long label,
			final long otherLabel,
			final double aff,
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap,
			final Edge e,
			final Edge dummy,
			final EdgeMerger edgeMerger )
	{
//		if ( label < 0 || otherLabel < 0 || aff < 0.0 || Double.isNaN( aff ) )
//			return -1;
		if ( !nodeEdgeMap.contains( label ) )
			nodeEdgeMap.put( label, new TLongIntHashMap() );
		if ( !nodeEdgeMap.contains( otherLabel ) )
			nodeEdgeMap.put( otherLabel, new TLongIntHashMap() );
		final TLongIntHashMap localEdges = nodeEdgeMap.get( label );
		if ( !localEdges.contains( otherLabel ) )
		{
			assert !nodeEdgeMap.get( otherLabel ).contains( label );
			final int index = e.add( Double.NaN, aff, label, otherLabel, 1 );
			localEdges.put( otherLabel, index );
			nodeEdgeMap.get( otherLabel ).put( label, index );
			return index;
		}
//			return g.addEdge( Double.NaN, aff, label, otherLabel, 1 );
		else
		{
			final int index = localEdges.get( otherLabel );
			assert nodeEdgeMap.get( otherLabel ).contains( label ) &&nodeEdgeMap.get( otherLabel ).get( label ) == index;
			e.setIndex( index );
			dummy.affinity( aff );
			dummy.from( label );
			dummy.to( otherLabel );
			edgeMerger.merge( dummy, e );
			return index;
		}
	}

	private static < T extends RealType< T > > void addEdgesFromNeighborBlocks(
			final RandomAccessibleInterval< LongType > labels,
			final RandomAccessibleInterval< RealComposite< T > > affinities,
			final int d,
			final long innerIndex,
			final long outerIndex,
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap,
			final Edge e,
			final Edge dummy,
			final EdgeMerger edgeMerger,
			final TLongObjectHashMap< TLongHashSet > borderNodesToOutsideNodes,
			final long[] blockDim )
	{
		final long[] croppedDim = new long[ labels.numDimensions() - 1 ];
		final long[] offset = new long[ croppedDim.length ];
		Arrays.fill( offset, 1l );
		for ( int i = 0, k = 0; i < blockDim.length; ++i )
		{
			if ( i == d )
				continue;
			croppedDim[ k ] = blockDim[ i ];
			++k;
		}
		final IntervalView< LongType > labelsInner = Views.hyperSlice( labels, d, innerIndex );
		final IntervalView< LongType > labelsOuter = Views.hyperSlice( labels, d, outerIndex );
		final IntervalView< RealComposite< T > > affinitiesSlice = Views.hyperSlice( affinities, d, Math.min( innerIndex, outerIndex ) );
		final Cursor< LongType > iC = Views.offsetInterval( labelsInner, offset, croppedDim ).cursor();
		final Cursor< LongType > oC = Views.offsetInterval( labelsOuter, offset, croppedDim ).cursor();
		final Cursor< RealComposite< T > > aC = Views.offsetInterval( affinitiesSlice, offset, croppedDim ).cursor();
		while( iC.hasNext() ) {
			final long label = iC.next().get();
			final long otherLabel = oC.next().get();
			final RealComposite< T > affs = aC.next();
			// TODO this check seems unnecessary
			if ( label != otherLabel )
			{
				final double aff = affs.get( d ).getRealDouble();
				if ( !Double.isNaN( aff ) )
				{
					if ( !borderNodesToOutsideNodes.contains( label ) )
						borderNodesToOutsideNodes.put( label, new TLongHashSet() );
					final TLongHashSet nodeToOutsideNode = borderNodesToOutsideNodes.get( label );
					nodeToOutsideNode.add( otherLabel /* neighborId */ );
					addEdge( label, otherLabel, aff, nodeEdgeMap, e, dummy, edgeMerger );
				}
			}
		}
	}

	public static long[] getAffinityDims( final long[] input )
	{
		return getAffinityDims( input, new long[ input.length + 1 ] );
	}

	public static long[] getAffinityDims( final long[] input, final long[] output )
	{
		System.arraycopy( input, 0, output, 0, input.length );
		output[ input.length ] = input.length;
		return output;
	}

	public static long[] padDimensions( final long[] input, final long pad )
	{
		return padDimensions( input, new long[ input.length ], pad );
	}

	public static long[] padDimensions( final long[] input, final long[] output, final long pad )
	{
		for ( int d = 0; d < input.length; ++d )
			output[ d ] = input[ d ] + 2 * pad;
		return output;
	}

	public static TLongLongHashMap filterBlockAssignments( final TLongLongHashMap nodeBlockAssignment, final TLongSet borderNodes )
	{
		final TLongLongHashMap filteredAssignments = new TLongLongHashMap();
		for ( final TLongIterator it = borderNodes.iterator(); it.hasNext(); )
		{
			final long k = it.next();
			filteredAssignments.put( k, nodeBlockAssignment.get( k ) );
		}
		return filteredAssignments;
	}


}
