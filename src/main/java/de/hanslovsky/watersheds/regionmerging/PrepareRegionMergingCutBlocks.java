package de.hanslovsky.watersheds.regionmerging;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.HashableLongArray;
import de.hanslovsky.watersheds.graph.Edge;
import de.hanslovsky.watersheds.graph.EdgeMerger;
import de.hanslovsky.watersheds.graph.Function;
import de.hanslovsky.watersheds.graph.IdService;
import de.hanslovsky.watersheds.graph.MergeBloc;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.UndirectedGraph;
import de.hanslovsky.watersheds.regionmerging.PrepareRegionMergingCutBlocks.GetExternalEdges.BlockOutput;
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

	public static Tuple2< JavaPairRDD< Long, In >, TLongLongHashMap > run(
			final JavaSparkContext sc,
			final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksWithLabelsAffinitiesAndCounts,
			final Broadcast< long[] > dim,
			final Broadcast< long[] > blockDim,
			final EdgeMerger edgeMerger,
			final Function func,
			final EdgeCheck edgeCheck,
			final IdService idService )
	{
		final JavaPairRDD< HashableLongArray, GetInternalEdgesAndSplits.IntraBlockOutput > allEdges =
				blocksWithLabelsAffinitiesAndCounts.mapToPair( new GetInternalEdgesAndSplits<>( blockDim.getValue(), edgeMerger, func, edgeCheck, idService ) );

//		allEdges.map( t -> {
//			System.out.println( Arrays.toString( t._1().getData() ) + " " + t._2().splitEdges.size() + " " + t._2().splitEdges );
//			return true;
//		} ).count();

//		for ( final Tuple2< HashableLongArray, IntraBlockOutput > e : allEdges.collect() )
//			System.out.println( Arrays.toString( e._1().getData() ) + " " + e._2().splitEdges.size() + " " + e._2().splitEdges );

		final JavaPairRDD< HashableLongArray, GetExternalEdges.BlockOutput > interBlockEdges =
				allEdges.mapToPair( new GetExternalEdges( blockDim, blockDim, edgeMerger ) ).cache();
		interBlockEdges.count();

		final TLongLongHashMap filteredNodeBlockAssignments = interBlockEdges
				.map( t -> t._2().filteredNodeBlockAssignment )
				.reduce( ( m1, m2 ) -> {
					m1.putAll( m2 );
					return m1;
				} );

		final JavaPairRDD< Long, In > mergeBlocs = interBlockEdges
				.values()
				.map( new MapNodesAndSplitBlocks( sc.broadcast( filteredNodeBlockAssignments ), edgeMerger ) )
				.flatMapToPair( new FlattenInputs() )
				.mapToPair( t -> {
					final Edge e = new Edge( t._2().g.edges() );
					final TLongLongHashMap counts = t._2().counts;
					for ( int i = 0; i < e.size(); ++i )
					{
						e.setIndex( i );
						e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
					}
					return t;
				} ).cache();

		return new Tuple2<>( mergeBlocs, filteredNodeBlockAssignments );
	}

	public static class GetExternalEdges implements PairFunction< Tuple2< HashableLongArray, GetInternalEdgesAndSplits.IntraBlockOutput >, HashableLongArray, GetExternalEdges.BlockOutput >
	{

		public static class BlockOutput
		{
			public final TLongLongHashMap counts;

			public final UndirectedGraph g;

			public final TLongLongHashMap nodeBlockAssignment;

			public final TLongLongHashMap filteredNodeBlockAssignment;

			public final TIntArrayList splitEdges;

			public final TLongObjectHashMap< TLongHashSet > borderNodesToOutsideNodes;

			public final long[] blockIds;

			public final int numberOfInternalEdges;

			public BlockOutput(
					final TLongLongHashMap counts,
					final UndirectedGraph g,
					final TLongLongHashMap nodeBlockAssignment,
					final TLongLongHashMap filteredNodeBlockAssignment,
					final TIntArrayList splitEdges,
					final TLongObjectHashMap< TLongHashSet > borderNodesToOutsideNodes,
					final long[] blockIds,
					final int numberOfInternalEdges )
			{
				super();
				this.counts = counts;
				this.g = g;
				this.nodeBlockAssignment = nodeBlockAssignment;
				this.filteredNodeBlockAssignment = filteredNodeBlockAssignment;
				this.splitEdges = splitEdges;
				this.borderNodesToOutsideNodes = borderNodesToOutsideNodes;
				this.blockIds = blockIds;
				this.numberOfInternalEdges = numberOfInternalEdges;
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

			final Edge e = new Edge( o.g.edges() );
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
							labels, affinities, d, inner, outer, o.g, o.g.nodeEdgeMap(), e, dummy,
							edgeMerger, borderNodesToOutsideNodes, blockDim );

				}
				blockIndices[ d ] += 2;
				if ( blockIndices[ d ] < numBlocksByDimension[ d ] )
				{
					final long inner = labels.max( d ) - 1;
					final long outer = inner + 1;
					addEdgesFromNeighborBlocks(
							labels, affinities, d, inner, outer, o.g, o.g.nodeEdgeMap(), e, dummy,
							edgeMerger, borderNodesToOutsideNodes, blockDim );
				}
				blockIndices[ d ] -= 1;
			}
			final TLongLongHashMap nodesToBlockFiltered = filterBlockAssignments( o.nodeBlockAssignment, borderNodesToOutsideNodes.keySet() );

			return new Tuple2<>( t._1(), new BlockOutput( o.counts, o.g, o.nodeBlockAssignment, nodesToBlockFiltered, o.splitEdges, borderNodesToOutsideNodes, o.blockIds, numberOfInternalEdges ) );
		}

	}

	public static class MapNodesAndSplitBlocks
	implements org.apache.spark.api.java.function.Function< GetExternalEdges.BlockOutput, TLongObjectHashMap< MergeBloc.In > >
	{

		private final Broadcast< TLongLongHashMap > nodeBlockMapping;

		private final EdgeMerger edgeMerger;

		public MapNodesAndSplitBlocks( final Broadcast< TLongLongHashMap > nodeBlockMapping, final EdgeMerger edgeMerger )
		{
			super();
			this.nodeBlockMapping = nodeBlockMapping;
			this.edgeMerger = edgeMerger;
		}

		@Override
		public TLongObjectHashMap< MergeBloc.In > call( final BlockOutput o ) throws Exception
		{

			final TLongLongHashMap nodeBlockMapping = this.nodeBlockMapping.getValue();

			final TLongObjectHashMap< MergeBloc.In > regionMergingInput = new TLongObjectHashMap<>();
//			final TLongObjectHashMap< Edge > edges = new TLongObjectHashMap<>();

			for ( final long id : o.blockIds ) {
//				final In in = new MergeBloc.In( new UndirectedGraph( new TDoubleArrayList(), edgeMerger ), new TLongLongHashMap(), new TLongObjectHashMap<>() );
				System.out.println( "Block id " + id );
				final In in = new MergeBloc.In(
						new UndirectedGraph( new TDoubleArrayList(), edgeMerger ),
						new TLongLongHashMap(),
						new TLongObjectHashMap<>(),
						new TLongLongHashMap() );
				regionMergingInput.put( id, in );
//				edges.put( id, new Edge( in.edges ) );
			}

			final Edge e = new Edge( o.g.edges() );
			final int nEdges = e.size();

			// add intra block edges
			for ( int i = 0; i < o.numberOfInternalEdges; ++i )
			{
				e.setIndex( i );
				final double w = e.weight();
				final double a = e.affinity();
				final long from = e.from();
				final long to = e.to();
				final long m = e.multiplicity();
				final long r1 = o.nodeBlockAssignment.get( from );
				final long r2 = o.nodeBlockAssignment.get( to );
				if ( r1 == r2 )
				{
					final In in = regionMergingInput.get( r1 );
					if ( !in.counts.contains( from ) )
						in.counts.put( from, o.counts.get( from ) );
					if ( !in.counts.contains( to ) )
						in.counts.put( to, o.counts.get( to ) );
					in.g.addEdge( w, a, from, to, m );
				}
				else
				{
					final In in1 = regionMergingInput.get( r1 );
					if ( !in1.counts.contains( from ) )
						in1.counts.put( from, o.counts.get( from ) );
					final In in2 = regionMergingInput.get( r2 );
					if ( !in2.counts.contains( to ) )
						in2.counts.put( to, o.counts.get( to ) );

					in1.g.addEdge( w, a, from, to, m );
					in2.g.addEdge( w, a, from, to, m );

					if ( !in1.borderNodes.contains( from ) )
						in1.borderNodes.put( from, new TLongHashSet() );
					in1.borderNodes.get( from ).add( r2 );
					in1.outsideNodes.put( to, r2 );

					if ( !in2.borderNodes.contains( to ) )
						in2.borderNodes.put( to, new TLongHashSet() );
					in2.borderNodes.get( to ).add( r1 );
					in2.outsideNodes.put( from, r1 ); // was in1.,outsideNodes
				}
			}

			// add edges that reach beyond block border
			for ( int i = o.numberOfInternalEdges; i < nEdges; ++i ) {
				e.setIndex( i );
				final double w = e.weight();
				final double a = e.affinity();
				final long from = e.from(); // inner
				final long to = e.to(); // outer
				final long m = e.multiplicity();

				final long r1 = o.nodeBlockAssignment.get( from );
				final long r2 = nodeBlockMapping.get( to );
				final In in = regionMergingInput.get( r1 );

				if ( !in.counts.contains( from ) )
					in.counts.put( from, o.counts.get( from ) );

				if ( !in.borderNodes.contains( from ) )
					in.borderNodes.put( from, new TLongHashSet() );

				in.borderNodes.get( from ).add( r2 );

				in.g.addEdge( w, a, from, to, m );

			}


			return regionMergingInput;
		}

	}

	public static class FlattenInputs implements PairFlatMapFunction< TLongObjectHashMap< MergeBloc.In >, Long, MergeBloc.In > {

		@Override
		public Iterable< Tuple2< Long, In > > call( final TLongObjectHashMap< In > t ) throws Exception
		{
			final Iterable< Tuple2< Long, In > > it = () -> new Iterator< Tuple2< Long, In > >() {

				final TLongObjectIterator< MergeBloc.In > localIt = t.iterator();

				@Override
				public boolean hasNext()
				{
					return localIt.hasNext();
				}

				@Override
				public Tuple2< Long, In > next()
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
			final UndirectedGraph g,
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap,
			final Edge e,
			final Edge dummy,
			final EdgeMerger edgeMerger,
			final TLongLongHashMap parents )
	{
		final RandomAccess< LongType > labelsAccess = labels.randomAccess();
		final Cursor< RealComposite< T > > affinitiesCursor = affinities.cursor();
		while ( affinitiesCursor.hasNext() )
		{
			final RealComposite< T > affinity = affinitiesCursor.next();
			labelsAccess.setPosition( affinitiesCursor );
			final long label = labelsAccess.get().get();
			if ( label == 26 )
				System.out.println( "WIR HABEN HIER LABEL 26! " + new Point( labelsAccess ) );
			parents.put( label, label );
			for ( int d = 0; d < blockDim.length; ++d )
				if ( labelsAccess.getLongPosition( d ) < blockDim[ d ] - 1 )
				{
					final double aff = affinity.get( d ).getRealDouble();
					if ( !Double.isNaN( aff ) )
					{
						labelsAccess.fwd( d );
						final long otherLabel = labelsAccess.get().get();
						if ( otherLabel != label )
							addEdge( label, otherLabel, aff, g, nodeEdgeMap, e, dummy, edgeMerger );
						labelsAccess.bck( d );
					}
				}
		}
	}

	private static void addEdge(
			final long label,
			final long otherLabel,
			final double aff,
			final UndirectedGraph g,
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap,
			final Edge e,
			final Edge dummy,
			final EdgeMerger edgeMerger )
	{
		if ( !nodeEdgeMap.contains( label ) )
			g.addNode( label );
		if ( !nodeEdgeMap.contains( otherLabel ) )
			g.addNode( otherLabel );
		final TLongIntHashMap localEdges = nodeEdgeMap.get( label );
		if ( !localEdges.contains( otherLabel ) )
			g.addEdge( Double.NaN, aff, label, otherLabel, 1 );
		else
		{
			e.setIndex( localEdges.get( otherLabel ) );
			dummy.affinity( aff );
			dummy.from( label );
			dummy.to( otherLabel );
			edgeMerger.merge( dummy, e );
		}
	}

	private static < T extends RealType< T > > void addEdgesFromNeighborBlocks(
			final RandomAccessibleInterval< LongType > labels,
			final RandomAccessibleInterval< RealComposite< T > > affinities,
			final int d,
			final long innerIndex,
			final long outerIndex,
			final UndirectedGraph g,
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
			if ( label != otherLabel )
			{
				final double aff = affs.get( d ).getRealDouble();
				if ( !Double.isNaN( aff ) )
				{
					if ( !borderNodesToOutsideNodes.contains( label ) )
						borderNodesToOutsideNodes.put( label, new TLongHashSet() );
					final TLongHashSet nodeToOutsideNode = borderNodesToOutsideNodes.get( label );
					nodeToOutsideNode.add( otherLabel /* neighborId */ );
					addEdge( label, otherLabel, aff, g, nodeEdgeMap, e, dummy, edgeMerger );
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
