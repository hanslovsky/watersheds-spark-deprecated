package de.hanslovsky.watersheds;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.graph.Edge;
import de.hanslovsky.watersheds.graph.EdgeMerger;
import de.hanslovsky.watersheds.graph.Function;
import de.hanslovsky.watersheds.graph.MergeBloc;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.UndirectedGraph;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.affinity.CompositeFactory;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class PrepareRegionMerging
{

	public static class BuildBlockedGraph implements
	PairFunction< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > >, Long, MergeBloc.In >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -2176070570757172192L;

		private final long[] dim;

		private final long[] blockDim;

		private final EdgeMerger edgeMerger;

		private final Function func;

		public BuildBlockedGraph( final long[] dim, final long[] blockDim, final EdgeMerger edgeMerger, final Function func )
		{
			super();
			this.dim = dim;
			this.blockDim = blockDim;
			this.edgeMerger = edgeMerger;
			this.func = func;
		}

		@Override
		public Tuple2< Long, In > call( final Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > t ) throws Exception
		{
			final long[] pos = t._1().getData();

			final long[] dim = this.dim;
			final long[] blockDim = this.blockDim;
			final long[] numBlocksByDimension = new long[ blockDim.length ];
			final long[] extendedBlockDim = new long[ blockDim.length ];
			final long[] extendedAffinitiesBlockDim = new long[ blockDim.length + 1 ];

			System.arraycopy( blockDim, 0, extendedAffinitiesBlockDim, 0, blockDim.length );
			extendedAffinitiesBlockDim[ blockDim.length ] = blockDim.length;
			for ( int d = 0; d < blockDim.length; ++d )
			{
				final long v = blockDim[ d ] + 2;
				extendedBlockDim[ d ] = v;
				extendedAffinitiesBlockDim[ d ] = v;
				numBlocksByDimension[ d ] = ( long ) Math.ceil( dim[ d ] * 1.0 / blockDim[ d ] );
			}
			final long[] offset = new long[ blockDim.length ];
			Arrays.fill( offset, 1 );
			final long[] blockIndices = new long[ pos.length ];
			for ( int d = 0; d < pos.length; ++d )
				blockIndices[ d ] = pos[ d ] / blockDim[ d ];
//					new long[] { t._1()._1() / blockDim[ 1 ], t._1()._2() / blockDim[ 1 ], t._1()._3() / blockDim[ 2 ] };
			final long id = IntervalIndexer.positionToIndex( blockIndices, numBlocksByDimension );

			final CompositeFactory< FloatType, RealComposite< FloatType > > compositeFactory = ( size ) -> Views.collapseReal( ArrayImgs.floats( 1, size ) ).randomAccess().get();
			final RealComposite< FloatType > extension = compositeFactory.create( blockDim.length );
			for ( int d = 0; d < blockDim.length; ++d )
				extension.get( d ).set( Float.NaN );

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
			final TLongLongHashMap counts = new TLongLongHashMap();// t._2()._3();
			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap< TLongHashSet >();
			final UndirectedGraph g = new UndirectedGraph( edges, edgeMerger );
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = g.nodeEdgeMap();


			addEdges( innerLabels, innerAffinities, blockDim, g, nodeEdgeMap, e, dummy, edgeMerger );

			for ( int d = 0; d < blockDim.length; ++d )
			{
//				System.out.println( t._1() );
//				System.out.println( Arrays.toString( dim ) );
//				System.out.println( Arrays.toString( blockDim ) );
//				System.out.println( Arrays.toString( blockIndices ) );
				System.out.println( Arrays.toString( numBlocksByDimension ) );
				blockIndices[ d ] -= 1;
				System.out.println( Arrays.toString( blockIndices ) );
				if ( blockIndices[ d ] >= 0 )
				{
					final long outer = 0;
					final long inner = outer + 1;
					final long neighborId = IntervalIndexer.positionToIndex( blockIndices, numBlocksByDimension );
					addEdgesFromNeighborBlocks( labels, affinities, d, inner, outer, g, nodeEdgeMap, e, dummy, edgeMerger, neighborId, borderNodes, blockDim );

				}
				blockIndices[ d ] += 2;
				System.out.println( Arrays.toString( blockIndices ) );
				if ( blockIndices[ d ] < numBlocksByDimension[ d ] )
				{
					final long inner = labels.max( d ) - 1;
					final long outer = inner + 1;
					final long neighborId = IntervalIndexer.positionToIndex( blockIndices, numBlocksByDimension );
					addEdgesFromNeighborBlocks( labels, affinities, d, inner, outer, g, nodeEdgeMap, e, dummy, edgeMerger, neighborId, borderNodes, blockDim );
				}
				blockIndices[ d ] -= 1;
				System.out.println( Arrays.toString( blockIndices ) );
				System.out.println();
			}

			for ( final TLongLongIterator it = t._2()._3().iterator(); it.hasNext(); )
			{
				it.advance();
				final long k = it.key();
				if ( nodeEdgeMap.contains( k ) )
					counts.put( k, it.value() );
			}

			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			}

			return new Tuple2<>( id, new MergeBloc.In( edges, counts, borderNodes ) );
		}

	}

	private static < T extends RealType< T > > void addEdges(
			final IntervalView< LongType > labels,
			final IntervalView< RealComposite< T > > affinities,
			final long[] blockDim,
			final UndirectedGraph g,
			final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap,
			final Edge e,
			final Edge dummy,
			final EdgeMerger edgeMerger )
	{
		final RandomAccess< LongType > labelsAccess = labels.randomAccess();
		final Cursor< LongType > labelsCursor = labels.cursor();
		final Cursor< RealComposite< T > > affinitiesCursor = affinities.cursor();
		while ( labelsCursor.hasNext() )
		{
			final long label = labelsCursor.next().get();
			final RealComposite< T > affinity = affinitiesCursor.next();
			labelsAccess.setPosition( labelsCursor );
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
			final long neighborId,
			final TLongObjectHashMap< TLongHashSet > borderNodes,
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
					if ( !borderNodes.contains( label ) )
						borderNodes.put( label, new TLongHashSet() );
					borderNodes.get( label ).add( neighborId );
					addEdge( label, otherLabel, aff, g, nodeEdgeMap, e, dummy, edgeMerger );
				}
			}
		}
	}

	public static class CountOverSquaredSize implements Function, Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -2382379605891324313L;

		@Override
		public double weight( final double a, final long c1, final long c2 )
		{
			return Math.min( c1, c2 ) / ( a * a );
		}

	}

	public static void main( final String[] args ) throws Exception
	{

		final Random rng = new Random( 100 );

		final long[] dim = new long[] { 9, 12 };
		final long[] blockDim = new long[] { 3, 4 };
		final long[] paddedBlockDim = new long[] { 5, 6 };

		final long[] labelsArr = new long[ ( int ) Intervals.numElements( paddedBlockDim ) ];
		final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( labelsArr, paddedBlockDim );
		for ( long y = 0; y < labels.dimension( 1 ); ++y )
			for ( final LongType l : Views.hyperSlice( labels, 1, y ) )
				l.set( y + 1 );

		final float[] affsArr = new float[ ( int ) Intervals.numElements( paddedBlockDim ) * 2 ];
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affinities =
				Views.collapseReal( ArrayImgs.floats( affsArr, paddedBlockDim[ 0 ], paddedBlockDim[ 1 ], paddedBlockDim.length ) );
		for ( final RealComposite< FloatType > a : Views.flatIterable( affinities ) )
			for ( int i = 0; i < affinities.numDimensions(); ++i )
				a.get( i ).setReal( rng.nextDouble() );

		long otherLabel = 10;
		for ( int d = 0; d < labels.numDimensions(); ++d )
		{
			for ( final LongType l : Views.hyperSlice( labels, d, 0 ) )
				l.set( otherLabel );
			++otherLabel;
			for ( final LongType l : Views.hyperSlice( labels, d, labels.max( d ) ) )
				l.set( otherLabel );
			++otherLabel;
			for ( final RealComposite< FloatType > a : Views.hyperSlice( affinities, d, labels.max( d ) ) )
				a.get( d ).set( Float.NaN );
		}

		final TLongLongHashMap counts = new TLongLongHashMap();
		for ( final LongType l : labels )
		{
			final long ll = l.get();
			if ( !counts.contains( ll ) )
				counts.put( ll, 1 );
			else
				counts.put( ll, counts.get( ll ) + 1 );
		}

		System.out.println( otherLabel + " " + counts );

		final BuildBlockedGraph builder = new BuildBlockedGraph( dim, blockDim, MergeBloc.DEFAULT_EDGE_MERGER, new CountOverSquaredSize() );

		final Tuple2< Long, In > result = builder.call( new Tuple2<>(
				new HashableLongArray( new long[] { 1 * blockDim[ 0 ], 1 * blockDim[ 1 ] } ),
				new Tuple3<>( labelsArr, affsArr, counts ) ) );
		System.out.println( result._1() );
		System.out.println( result._2().counts );
		System.out.println( result._2().borderNodes );
		final Edge e = new Edge( result._2().edges );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			System.out.println( i + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
		}

	}

//	public static void main( final String[] args ) throws Exception
//	{
//
//		final Random rng = new Random( 100 );
//
//		final long[] dim = new long[] { 13, 14, 16 };
//		final long[] blockDim = new long[] { 3, 4, 5 };
//		final long[] paddedBlockDim = new long[] { 5, 6, 7 };
//
//		final long[] labelsArr = new long[ ( int ) Intervals.numElements( paddedBlockDim ) ];
//		final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( labelsArr, 5, 6, 7 );
//		for ( long y = 0; y < labels.dimension( 1 ); ++y )
//			for ( final LongType l : Views.hyperSlice( labels, 1, y ) )
//				l.set( y + 1);
//
//		final float[] affsArr = new float[ ( int ) Intervals.numElements( paddedBlockDim ) * 3 ];
//		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affinities =
//				Views.collapseReal( ArrayImgs.floats( affsArr, 5, 6, 7, 3 ) );
//		for ( final RealComposite< FloatType > a : Views.flatIterable( affinities ) )
//			for ( int i = 0; i < affinities.numDimensions(); ++i )
//				a.get( i ).setReal( rng.nextDouble() );
//
//		long otherLabel = 10;
//		for ( int d = 0; d < labels.numDimensions(); ++d )
//		{
//			for ( final LongType l : Views.hyperSlice( labels, d, 0 ) )
//				l.set( otherLabel );
//			++otherLabel;
//			for ( final LongType l : Views.hyperSlice( labels, d, labels.max( d ) ) )
//				l.set( otherLabel );
//			++otherLabel;
//			for ( final RealComposite< FloatType > a : Views.hyperSlice( affinities, d, labels.max( d ) ) )
//				a.get( d ).set( Float.NaN );
//		}
//
//		final TLongLongHashMap counts = new TLongLongHashMap();
//		for ( final LongType l : labels )
//		{
//			final long ll = l.get();
//			if ( !counts.contains( ll ) )
//				counts.put( ll, 1 );
//			else
//				counts.put( ll, counts.get( ll ) + 1 );
//		}
//
//		System.out.println( otherLabel + " " + counts );
//
//		final BuildBlockedGraph builder = new BuildBlockedGraph( dim, blockDim, MergeBloc.DEFAULT_EDGE_MERGER, new CountOverSquaredSize() );
//
//		final Tuple2< Long, In > result = builder.call( new Tuple2<>(
//				new HashableLongArray( new long[] { 2 * blockDim[ 0 ], 2 * blockDim[ 1 ], 2 * blockDim[ 2 ] } ),
//				new Tuple3<>( labelsArr, affsArr, counts ) ) );
//		System.out.println( result._1() );
//		System.out.println( result._2().counts );
//		System.out.println( result._2().borderNodes );
//		final Edge e = new Edge( result._2().edges );
//		for ( int i = 0; i < e.size(); ++i )
//		{
//			e.setIndex( i );
//			System.out.println( i + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
//		}
//
//	}


}
