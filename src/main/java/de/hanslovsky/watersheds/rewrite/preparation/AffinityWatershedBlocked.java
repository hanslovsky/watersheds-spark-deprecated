package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.mastodon.collection.ref.RefArrayList;
import org.mastodon.pool.ByteUtils;
import org.mastodon.pool.DoubleMappedElement;
import org.mastodon.pool.DoubleMappedElementArray;
import org.mastodon.pool.Pool;
import org.mastodon.pool.PoolObject;
import org.mastodon.pool.PoolObject.Factory;
import org.mastodon.pool.SingleArrayMemPool;
import org.scijava.util.FileUtils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import gnu.trove.impl.Constants;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.algorithm.morphology.watershed.flat.FlatViews;
import net.imglib2.type.BooleanType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IterableRandomAccessibleInterval;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;

public class AffinityWatershedBlocked
{



	public static long[] generateStride( final Interval i )
	{
		final int nDim = i.numDimensions();
		final long[] strides = new long[ nDim ];

		strides[ 0 ] = 1;
		for ( int d = 1; d < nDim; ++d )
			strides[ d ] = strides[ d - 1 ] * i.dimension( d - 1 );

		return strides;
	}

	public static long[] generateSteps( final long[] strides )
	{
		final int nDim = strides.length;
		final long[] steps = new long[ 2 * nDim ];
		for ( int d = 0; d < nDim; ++d )
		{
			steps[ nDim + d ] = strides[ d ];
			steps[ nDim - 1 - d ] = -strides[ d ];
		}
		return steps;
	}

	public static long[] generateDirectionBitmask( final int nDim )
	{
		final long[] bitmask = new long[ nDim * 2 ];
		for ( int d = 0; d < bitmask.length; ++d )
			bitmask[ d ] = 1 << d;
		return bitmask;
	}

	public static long[] generateInverseDirectionBitmask( final long[] bitmask )
	{
		final long[] inverseBitmask = new long[ bitmask.length ];
		for ( int d = 0; d < bitmask.length; ++d )
			inverseBitmask[ d ] = bitmask[ bitmask.length - 1 - d ];
		return inverseBitmask;
	}

	public static int[][] generateMoves( final int nDim )
	{
		final int[][] moves = new int[ 2 * nDim ][ 2 ];
		for ( int d = 0; d < nDim; ++d )
		{
			moves[ nDim + d ][ 0 ] = d;
			moves[ nDim + d ][ 1 ] = 1;

			moves[ nDim - 1 - d ][ 0 ] = d;
			moves[ nDim - 1 - d ][ 1 ] = -1;
		}
		return moves;
	}

	public static < T extends RealType< T >, B extends BooleanType< B > > long[] letItRain(
			final RandomAccessible< RealComposite< T > > source,
			final RandomAccessibleInterval< LongType > labels,
			final CompareBetter< T > compare,
			final T worst,
			final T low,
			final T high,
			final ExecutorService es,
			final int nTasks,
			final Runnable visitor ) throws InterruptedException, ExecutionException
	{

		final long highBit = 1l << 63;
		final long secondHighBit = 1l << 62;

		final int nDim = source.numDimensions();
		final long[] strides = generateStride( labels );
		final long[] steps = generateSteps( strides );
		final long[] bitmask = generateDirectionBitmask( nDim );
		final long[] inverseBitmask = generateInverseDirectionBitmask( bitmask );

		final long t0 = System.nanoTime();
		findParents( source, labels, compare, worst, bitmask, low, high, es, nTasks );
		final long t1 = System.nanoTime();
		System.out.println( "findParents: " + ( t1 - t0 ) / 1e6 + "ms" );

		visitor.run();

		final long t2 = System.nanoTime();
		final TLongArrayList plateauCorners = findPlateauCorners( labels, steps, bitmask, inverseBitmask, secondHighBit, es, nTasks );
		final long t3 = System.nanoTime();
		System.out.println( "findPlateauCorners: " + ( t3 - t2 ) / 1e6 + "ms" );

		visitor.run();

		final long t4 = System.nanoTime();
		removePlateaus( plateauCorners, labels, steps, bitmask, inverseBitmask, highBit, secondHighBit );
		final long t5 = System.nanoTime();
		System.out.println( "removePlateaus: " + ( t5 - t4 ) / 1e6 + "ms" );

		visitor.run();

		final long t6 = System.nanoTime();
		final long[] counts = nTasks > 1 ? fillFromRoots( labels, steps, bitmask, inverseBitmask, highBit, es, nTasks ) : mergeAndCount( labels, highBit, secondHighBit, bitmask, steps );
		final long t7 = System.nanoTime();
		System.out.println( "mergeAndCount: " + ( t7 - t6 ) / 1e6 + "ms" );

		return counts;

	}

	public static < T extends RealType< T > > void findParents(
			final RandomAccessible< RealComposite< T > > source,
			final RandomAccessibleInterval< LongType > labels,
			final CompareBetter< T > compare,
			final T worst,
			final long[] bitMask,
			final T low,
			final T high,
			final ExecutorService es,
			final int nTasks ) throws InterruptedException, ExecutionException
	{

		final int nDim = source.numDimensions();
		final int nEdges = 2 * nDim;

		final long size = FlatViews.flatten( labels ).dimension( 0 );

		final long taskSize = size / nTasks;

		final ArrayList< Callable< Void > > tasks = new ArrayList<>();

		for ( long start = 0; start < size; start += taskSize )
		{

			final long fStart = start;

			tasks.add( () -> {

				final Cursor< Pair< RealComposite< T >, LongType > > cursor = Views.flatIterable( Views.interval( Views.pair( source, labels ), labels ) ).cursor();
				cursor.jumpFwd( fStart );
				final T currentBest = worst.createVariable();

				final double[] bests = new double[ nEdges ];

				for ( long count = 0; count < taskSize && cursor.hasNext(); ++count )
				{
					final Pair< RealComposite< T >, LongType > p = cursor.next();
					final RealComposite< T > edgeWeights = p.getA();
					final LongType label = p.getB();
					long labelRaw = label.get();
					currentBest.set( worst );

					for ( long i = 0; i < nEdges; ++i )
					{
						final T currentWeight = edgeWeights.get( i );
						bests[(int)i] = currentWeight.getRealDouble();
//						System.out.println( currentWeight + " " + bitMask[ ( int ) i ] );
						if ( compare.isBetter( currentWeight, currentBest ) )
							currentBest.set( currentWeight );
					}
//					System.out.println();

					if ( compare.isBetter( currentBest, worst ) )
						for ( int i = 0; i < nEdges; ++i )
						{
							final T ew = edgeWeights.get( i );
							if ( ew.valueEquals( currentBest ) || compare.isBetter( ew, high ) )
								labelRaw |= bitMask[ i ];
						}
//								break;

//					if ( fStart + count == 226 )
//						System.out.println( "226 points to: " + Long.toBinaryString( labelRaw ) + " " + p.getA().get( 0 ) + " " + p.getA().get( 1 ) + " " + p.getA().get( 2 ) + " " + p.getA().get( 3 ) + " " + p.getA() );

					label.set( labelRaw );
				}
				return null;

			} );
		}

		invokeAllAndWait( es, tasks );

	}

	public static TLongArrayList findPlateauCorners(
			final RandomAccessibleInterval< LongType > labels,
			final long[] steps,
			final long[] bitmask,
			final long[] inverseBitmask,
			final long plateauCornerMask,
			final ExecutorService es,
			final int nTasks ) throws InterruptedException, ExecutionException
	{
		final int nEdges = steps.length;
		final long size = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).dimension( 0 );

		final long taskSize = size / nTasks;

		final ArrayList< Callable< TLongArrayList > > tasks = new ArrayList<>();

		final int[][] moves = generateMoves( labels.numDimensions() );

		for ( long start = 0; start < size; start += taskSize )
		{
			final long finalStart = start;

			tasks.add( () -> {

				final TLongArrayList taskPlateauCornerIndices = new TLongArrayList();
				final RandomAccess< LongType > labelsAccess = labels.randomAccess();
				final Cursor< LongType > cursor = Views.flatIterable( labels ).cursor();
				cursor.jumpFwd( finalStart );

				for ( long count = 0, index = finalStart; count < taskSize; ++count, ++index )
				{
					final LongType label = cursor.next();
					final long labelRaw = label.get();
					labelsAccess.setPosition( cursor );
					for ( int i = 0; i < nEdges; ++i )
						if ( ( labelRaw & bitmask[ i ] ) != 0 )
						{
							final int dimension = moves[ i ][ 0 ];
							final int direction = moves[ i ][ 1 ];
							labelsAccess.move( direction, dimension );

							// if pointed at neighbor does not point back: Mark
							// current location as plateau corner
							if ( Intervals.contains( labels, labelsAccess ) && ( labelsAccess.get().get() & inverseBitmask[ i ] ) == 0 )
							{
								label.set( labelRaw | plateauCornerMask );
								taskPlateauCornerIndices.add( index );
								i = nEdges; // break;
							}

							labelsAccess.move( -direction, dimension );
						}
				}
				return taskPlateauCornerIndices;
			} );

		}

		final List< Future< TLongArrayList > > futures = es.invokeAll( tasks );

		final TLongArrayList plateauCornerIndices = new TLongArrayList();

		for ( final Future< TLongArrayList > f : futures )
			plateauCornerIndices.addAll( f.get() );

		return plateauCornerIndices;
	}

	public static void removePlateaus(
			final TLongArrayList queue,
			final RandomAccessibleInterval< LongType > labels,
			final long[] steps,
			final long[] bitMask,
			final long[] inverseBitmask,
			final long highBit,
			final long secondHighBit )
	{

		// This is the same as example in paper, if traversal order of queue is
		// reversed.

		// helpers
		final int nEdges = steps.length;

//		System.out.println( "REMOVING PLATEAUS!" );

		// accesses
		final RandomAccess< LongType > flatLabels = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).randomAccess();

//		System.out.println( "226: " + Long.toBinaryString( get( flatLabels, 226 ).get() ) );

		final RandomAccess< LongType > labelsAccess = labels.randomAccess();

		final int[][] moves = generateMoves( labels.numDimensions() );

		System.out.println( "Removing plateaus, staring at " + queue.size() + " plateau corners." );

		for( int queueIndex = 0; queueIndex < queue.size(); ++queueIndex ) {
			final long index = queue.get( queueIndex );
			long parent = 0;
			final LongType label = get( flatLabels, index );
			final long labelRaw = label.get();

			IntervalIndexer.indexToPosition( index, labels, labelsAccess );

			for ( int d = 0; d < nEdges; ++d )
				// if pointer active in that direction
				if ( ( labelRaw & bitMask[ d ] ) != 0 )
				{
					// go to neighbor in specified direction
					final int dimension = moves[ d ][ 0 ];
					final int direction = moves[ d ][ 1 ];
					labelsAccess.move( direction, dimension );

//					if ( index + steps[ d ] == 226 )
//						System.out.println( "Neighbor of " + 226 );


					if ( Intervals.contains( labels, labelsAccess ) )
					{
						final long otherIndex = index + steps[ d ];
						final LongType otherLabel = labelsAccess.get();
						final long otherLabelRaw = otherLabel.get();
						if ( ( otherLabelRaw & inverseBitmask[ d ] ) != 0 )
						{
							if ( ( otherLabelRaw & secondHighBit ) == 0 )
							{
//								if ( otherIndex == 226 )
//									System.out.println( "Enqueuing index " + otherIndex );
								queue.add( otherIndex );
								otherLabel.set( otherLabelRaw | secondHighBit );
							}
						}
						// if parent is not set yet, set parent direction to
						// current direction
						else if ( parent == 0 )
							parent = bitMask[ d ];
					}

					labelsAccess.move( -direction, dimension );
				}
//			if ( Long.bitCount( parent ) != 1 )
//				System.out.println( "Setting " + Long.toBinaryString( label.get() ) + " to " + Long.toBinaryString( parent ) + " at " + index );
			label.set( parent );
		}

//		// check 226 neighbors:
//		System.out.println( "Check 226 neighbors:" );
//		final RandomAccess< LongType > l226 = FlatViews.flatten( labels ).randomAccess();
//		System.out.println( Long.toBinaryString( get( l226, 226 ).get() ) );
//		for ( int d = 0; d < nEdges; ++d )
//		{
//
//		}

	}

	private static TLongArrayList buildTree(
			final RandomAccessibleInterval< LongType > labels,
			final long[] steps,
			final long[] bitmask,
			final long[] inverseBitmask )
	{
		final TLongArrayList roots = new TLongArrayList();

		final long size = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).dimension( 0 );
		final int nEdges = steps.length;

		final RandomAccess< LongType > flatLabels = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).randomAccess();

		final Cursor< LongType > c = Views.flatIterable( labels ).cursor();
		for ( long index = 0; index < size; ++index )
		{
			final LongType label = c.next();
			final long labelRaw = label.get();
			boolean hasEdge = false;
			for ( int d = 0; d < nEdges; ++d )
				if ( (labelRaw & bitmask[d]) != 0 )
				{
					hasEdge = true;
					final long otherIndex = index + steps[d];
					if ( otherIndex >= 0 && otherIndex < size && ( get( flatLabels, otherIndex ).get() & inverseBitmask[d] ) != 0 )
					{
						if ( index < otherIndex )
							roots.add( index );
						else
							label.set( bitmask[ d ] );
					}
					else
						label.set( bitmask[ d ] );
					d = nEdges;
				}
			if ( !hasEdge )
				roots.add( index );
		}

		for ( final TLongIterator r = roots.iterator(); r.hasNext(); )
			get( flatLabels, r.next() ).set( 0l );

		c.reset();

		for ( long index = 0; c.hasNext(); ++index )
		{
			final LongType label = c.next();
			final long labelRaw = label.get();
			if ( labelRaw == 0 )
				label.set( index );
			else
				for ( int d = 0; d < bitmask.length; ++d )
					if ( ( labelRaw & bitmask[ d ] ) != 0 )
					{
						label.set( index + steps[d] );
						d = bitmask.length;
					}
		}

		return roots;
	}

	private static < L extends IntegerType< L > > long[] fillFromRoots(
			final RandomAccessibleInterval< L > labels,
			final long[] steps,
			final long[] bitmask,
			final long[] inverseBitmask,
			final long visitedMask,
			final ExecutorService es,
			final int nTasks ) throws InterruptedException, ExecutionException
	{

		final long size = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).dimension( 0 );
		final int nEdges = steps.length;


		final AtomicLong backgroundCount = new AtomicLong( 0 );

		final ArrayList< Callable< TLongArrayList > > rootLocatingTasks = new ArrayList<>();

		final long taskSize = size / nTasks;

		final int[][] moves = generateMoves( labels.numDimensions() );

		for ( long start = 0; start < size; start += taskSize )
		{
			final long finalStart = start;

			rootLocatingTasks.add( () -> {
				final Cursor< L > cursor = Views.flatIterable( labels ).cursor();
				cursor.jumpFwd( finalStart );
				final TLongArrayList roots = new TLongArrayList();
//				final RandomAccess< L > flatLabels = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).randomAccess();
				final RandomAccess< L > labelsAccess = labels.randomAccess();
				for ( long count = 0, index = finalStart; count < taskSize && cursor.hasNext(); ++count, ++index )
				{
					boolean isChild = false;
					boolean hasChild = false;
					boolean pointsOutside = false;

					final long label = cursor.next().getIntegerLong();

					labelsAccess.setPosition( cursor );

					for ( int i = 0; i < nEdges && !isChild && !hasChild; ++i )
						if ( ( label & bitmask[ i ] ) != 0 )
						{
							isChild = true;
							final long otherIndex = index + steps[ i ];

							final int dimension = moves[ i ][ 0 ];
							final int direction = moves[ i ][ 1 ];

							labelsAccess.move( direction, dimension );

							if ( Intervals.contains( labels, labelsAccess ) )
								hasChild = ( labelsAccess.get().getIntegerLong() & inverseBitmask[ i ] ) != 0 && index < otherIndex;
							else
								pointsOutside = true;

//							if ( otherIndex >= 0 && otherIndex < size &&
//									( get( flatLabels, otherIndex ).getIntegerLong() & inverseBitmask[ i ] ) != 0 &&
//									index < otherIndex )
//								hasChild = true;

							labelsAccess.move( -direction, dimension );

						}

//							isChild = true;
//						else
//						{
//							final long otherIndex = index + steps[ i ];
//							if ( otherIndex >= 0 && otherIndex < size && ( get( flatLabels, otherIndex ).getIntegerLong() & inverseBitmask[ i ] ) != 0 )
//								hasChild = true;
//						}

					if ( hasChild || pointsOutside )
						roots.add( index );
					else if ( !isChild )
						backgroundCount.incrementAndGet();
				}
				return roots;
			} );
		}

		final TLongArrayList roots = new TLongArrayList();

		{

			final long t0 = System.nanoTime();
			final List< Future< TLongArrayList > > rootsFutures = es.invokeAll( rootLocatingTasks );
			for ( final Future< TLongArrayList > f : rootsFutures )
				roots.addAll( f.get() );
			final long t1 = System.nanoTime();
			System.out.println( "\tFinding roots " + ( t1 - t0 ) / 1e6 + "ms " + backgroundCount.get() + " " + roots.size() );
		}

		rootLocatingTasks.clear();

//		for ( final TLongIterator r = roots.iterator(); r.hasNext(); )
//			System.out.println( "rtt " + r.next() );

		final long[] counts = new long[ roots.size() + 1 ];
		counts[ 0 ] = backgroundCount.get();

		final ArrayList< Callable< Void > > tasks = new ArrayList<>();

		final int fillTaskSize = Math.max( ( counts.length - 1 ) / nTasks, 1 );

		final Point p1 = new Point( labels.numDimensions() );
		final Point p2 = new Point( labels.numDimensions() );
		final Point p3 = new Point( labels.numDimensions() );

//		final Cursor< L > c = Views.flatIterable( labels ).cursor();
//		for ( long i = 0; c.hasNext(); ++i ) {
//			final long val = c.next().getIntegerLong();
//			int nPointers = 0;
//			for ( int k = 0; k < bitmask.length; ++k )
//				nPointers += (bitmask[k] & val) != 0 ? 1 : 0;
//			if ( nPointers > 1 )
//			{
//				final Point p = new Point( labels.numDimensions() );
//				IntervalIndexer.indexToPosition( i, labels, p );
//				System.out.println( nPointers + " " + i + " " + p + " " + roots.contains( i ) );
//			}
//		}


		for ( int start = 1; start < counts.length; start += fillTaskSize )
		{
			final int finalStart = start;
			final int stop = Math.min( start + fillTaskSize, counts.length );
			tasks.add( () -> {
				final TLongHashSet visitedPoints = new TLongHashSet();
				final RandomAccess< L > flatLabels = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).randomAccess();
				final TLongArrayList queue = new TLongArrayList();
				for ( int i = finalStart; i < stop; ++i )
				{
//					System.out.println( i + " " + finalStart + " " + stop + " " + roots.get( i - 1 ) );
					queue.add( roots.get( i - 1 ) );
					final long regionLabel = i | visitedMask;
					for ( int startIndex = 0; startIndex < queue.size(); ++startIndex )
					{
						final long index = queue.get( startIndex );
						final long label = get( FlatViews.flatten( labels ).randomAccess(), index ).getIntegerLong();
//						if ( ( label & visitedMask ) != 0 )
//						{
//							System.out.println( "Why is non-zero visited mask in queue? " + Long.toBinaryString( label & visitedMask ) );
//							final Point p = new Point( labels.numDimensions() );
//							IntervalIndexer.indexToPosition( index, labels, p );
//							throw new RuntimeException( "ALREADY VISITED POINT: " + index + " " + p + " " + visitedPoints.size() );
//						}
						if ( visitedPoints.contains( index ) )
						{
							final Point p = new Point( labels.numDimensions() );
							IntervalIndexer.indexToPosition( index, labels, p );
							throw new RuntimeException( "ALREADY VISITED POINT: " + index + " " + p + " " + visitedPoints.size() );
						}
						visitedPoints.add( index );
						IntervalIndexer.indexToPosition( index, labels, p3 );
						for ( int d = 0; d < nEdges; ++d )
						{
							final int dimension = moves[ d ][ 0 ];
							final int direction = moves[ d ][ 1 ];
							final long otherIndex = index + steps[ d ];
							p3.move( direction, dimension );
							if ( Intervals.contains( labels, p3 ) && otherIndex >= 0 && otherIndex < size )
							{
								final long otherLabel = get( flatLabels, otherIndex ).getIntegerLong();
								if ( ( otherLabel & visitedMask ) == 0 && ( otherLabel & inverseBitmask[ d ] ) != 0 ) {
									queue.add( otherIndex );
									// TODO remove this debugging output
									if ( i == 2812 )
									{
										IntervalIndexer.indexToPosition( index, labels, p1 );
										IntervalIndexer.indexToPosition( otherIndex, labels, p2 );
										long dist = 0;
										for ( int dm = 0; dm < p1.numDimensions(); ++dm )
											dist += Math.abs( p1.getLongPosition( dm ) - p2.getLongPosition( dm ) );
										if ( dist != 1 )
											System.out.println( "REGION " + i + " " + p1 + " " + p2 + " " + index + " " + otherIndex + " " + dist + " " + d + " " + Arrays.toString( steps ) );
									}
								}
							}
							p3.move( -direction, dimension );
						}
						get( flatLabels, index ).setInteger( regionLabel );
						++counts[ i ];
					}
					queue.clear();
//					System.out.println( "Region " + i + " has size " + counts[ i ] );
				}
				return null;
			} );
		}

		{
			System.out.println( "Flood filling: " + counts.length + " roots" );
			final long t0 = System.nanoTime();
			invokeAllAndWait( es, tasks );
			final long t1 = System.nanoTime();
			System.out.println( "\tFlood fill " + ( t1 - t0 ) / 1e6 + "ms" );
		}

		tasks.clear();

		// should this happen outside?
		final long activeBits = ~visitedMask;
		for ( long start = 0; start < size; start += taskSize )
		{
			final long finalStart = start;
			tasks.add( () -> {
				final Cursor< L > cursor = Views.flatIterable( labels ).cursor();
				cursor.jumpFwd( finalStart );
				for ( long count = 0; count < taskSize && cursor.hasNext(); ++count )
				{
					final L l = cursor.next();
					l.setInteger( l.getIntegerLong() & activeBits );
				}
				return null;
			} );
		}
		{
			final long t0 = System.nanoTime();
			invokeAllAndWait( es, tasks );
			final long t1 = System.nanoTime();
			System.out.println( "\tRemoving mask " + ( t1 - t0 ) / 1e6 + "ms" );
		}

		return counts;
	}

	private static long[] mergeAndCount(
			final RandomAccessibleInterval< LongType > labels,
			final long highBit,
			final long secondHighBit,
			final long[] bitmask,
			final long[] steps )
	{
		long[] counts = new long[ 1 ];
		int countsSize = 1;

		counts[ 0 ] = 0;
		long nextId = 1l;

		System.out.println( Arrays.toString( steps ) );
		for ( int d = 0; d < bitmask.length; ++d )
			System.out.print( Long.toBinaryString( bitmask[d] ) + ", " );
		System.out.println();

		final TLongArrayList queue = new TLongArrayList();

		final RandomAccess< LongType > flatLabels2 = FlatViews.flatten( new IterableRandomAccessibleInterval<>( labels ) ).randomAccess();

		final long relevantBits = ~( highBit | secondHighBit );

		final Cursor< LongType > c = Views.flatIterable( labels ).cursor();
		final Point p = new Point( labels.numDimensions() );
		for ( long index = 0; c.hasNext(); ++index )
		{
			final LongType label = c.next();
			final long labelRaw = label.get();

			if ( labelRaw == 0 )
			{
				label.set( labelRaw | highBit );
				++counts[ 0 ];
			}

			if ( ( labelRaw & highBit ) == 0 && labelRaw != 0 )
			{

				queue.add( index );
				label.set( labelRaw | secondHighBit );

				for ( int queueIndex = 0; queueIndex < queue.size(); ++queueIndex )
				{
					final long nextIndex = queue.get( queueIndex );
					final long nextLabel = get( flatLabels2, nextIndex ).get();

					for ( int d = 0; d < bitmask.length; ++d )
						if ( ( nextLabel & bitmask[ d ] ) != 0 )
						{
							final long otherIndex = nextIndex + steps[ d ];
							IntervalIndexer.indexToPosition( otherIndex, labels, p );
							if ( Intervals.contains( labels, p ) )
							{
								final long otherLabel = get( flatLabels2, otherIndex ).get();
								if ( ( otherLabel & highBit ) != 0 )
								{
									counts[ ( int ) ( otherLabel & ~highBit ) ] += queue.size();
									for ( final TLongIterator q = queue.iterator(); q.hasNext(); )
										get( flatLabels2, q.next() ).set( otherLabel );

									queue.clear();
									d = bitmask.length;
								}
								else if ( ( otherLabel & secondHighBit ) == 0 )
								{
									get( flatLabels2, otherIndex ).set( otherLabel | secondHighBit );
									queue.add( otherIndex );
								}
							}
						}
				}

				if ( queue.size() > 0 )
				{
					if ( countsSize == counts.length )
					{
						final long[] countsTmp = new long[ 2 * counts.length ];
						System.arraycopy( counts, 0, countsTmp, 0, counts.length );
						counts = countsTmp;
					}
					counts[ countsSize ] = queue.size();

					for ( final TLongIterator q = queue.iterator(); q.hasNext(); )
					{
						final long ix = q.next();
						get( flatLabels2, ix ).set( highBit | nextId );
					}

					++countsSize;
					++nextId;
					queue.clear();
				}

			}
		}

		for ( final LongType l : Views.flatIterable( labels ) )
			l.set( l.get()&relevantBits );

		final long[] countsTmp = new long[ countsSize ];
		System.arraycopy( counts, 0, countsTmp, 0, countsSize );
		counts = countsTmp;
		return counts;
	}

	public static < T extends RealType< T >, L extends IntegerType< L > > TLongDoubleHashMap generateRegionGraph(
			final RandomAccessible< RealComposite< T > > source,
			final RandomAccessibleInterval< L > labels,
			final long[] steps,
			final CompareBetter< T > compare,
			final T worstValue,
			final long highBit,
			final long secondHighBit,
			final long nLabels )
	{
		final long validBits = ~( highBit | secondHighBit );
		final int[][] moves = generateMoves( labels.numDimensions() );

		final int nEdges = steps.length;

		final T t = source.randomAccess().get().get( 0 ).createVariable();

		final T compType = t.createVariable();

		final TLongDoubleHashMap graph = new TLongDoubleHashMap( Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, -1, Double.NaN );

		final long[] dim = new long[] { nLabels, nLabels };
		final long[] fromTo = new long[ 2 ];

		final RandomAccessible< Pair< RealComposite< T >, L > > paired = Views.pair( source, labels );
		final RandomAccess< Pair< RealComposite< T >, L > > pairedAccess = paired.randomAccess();

//		for ( long index = 0; index < size; ++index )
		for ( final Cursor< Pair< RealComposite< T >, L > > c = Views.flatIterable( Views.interval( paired, labels ) ).cursor(); c.hasNext(); )
		{
			final Pair< RealComposite< T >, L > p = c.next();
			final long label = p.getB().getIntegerLong() & validBits;
			if ( label == 0 )
				continue;
			final RealComposite< T > edges = p.getA();
			pairedAccess.setPosition( c );
			for ( int d = 0; d < nEdges; ++d )
			{
				final int dimension = moves[ d ][ 0 ];
				final int direction = moves[ d ][ 1 ];

				pairedAccess.move( direction, dimension );

				if ( !Intervals.contains( labels, pairedAccess ) )
				{
					pairedAccess.move( -direction, dimension );
					continue;
				}

				final long otherLabel = pairedAccess.get().getB().getIntegerLong() & validBits;
				final long from, to;

				if ( label == otherLabel || otherLabel == 0 )
				{
					pairedAccess.move( -direction, dimension );
					continue;
				}

				else if ( label < otherLabel )
				{
					from = label;
					to = otherLabel;
				}
				else
				{
					from = otherLabel;
					to = label;
				}

				fromTo[ 0 ] = from;
				fromTo[ 1 ] = to;

				final long currentIndex = IntervalIndexer.positionToIndex( fromTo, dim );

				final T edge = edges.get( d );
				final double edgeDouble = edge.getRealDouble();

				if ( Double.isNaN( edgeDouble ) )
				{
					pairedAccess.move( -direction, dimension );
					continue;
				}

				final double comp = graph.get( currentIndex );
				compType.setReal( comp );

				if ( Double.isNaN( comp ) || compare.isBetter( edge, compType ) )
					graph.put( currentIndex, edgeDouble );

				pairedAccess.move( -direction, dimension );
			}
		}

		return graph;
	}

	public static final TLongHashSet IDS = new TLongHashSet();

	public static final RefArrayList< WeightedEdge > EDGES = new RefArrayList<>( new WeightedEdgePool( 100 ) );

	public static DisjointSets mergeRegionGraph(
			final Collection< WeightedEdge > graph,
			final long[] counts,
			final Predicate compare,
			final Function func
			) {
		return mergeRegionGraph( graph, counts, compare, func, new DisjointSets( counts.length ) );

	}

	public static DisjointSets mergeRegionGraph(
			final Collection< WeightedEdge > graph,
			final long[] counts,
			final Predicate compare,
			final Function func,
			final DisjointSets dj
			)
	{
		final File f = new File( System.getProperty( "user.home" ) + "/local/tmp/4234-regions" );
		TLongHashSet set = null;
		try
		{
			set = f.exists() ? new TLongHashSet( new Gson().fromJson( new String( FileUtils.readFile( f ) ), long[].class ) ) : null;
		}
		catch ( final JsonSyntaxException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch ( final IOException e )
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for ( final WeightedEdge edge : graph )
		{

			final double val = edge.getV();

			final double valProcessed = func.apply( val );

			if ( Double.isNaN( valProcessed ) )
				break;

			final int id1 = dj.findRoot( ( int ) edge.getFirst() );
			final int id2 = dj.findRoot( ( int ) edge.getSecond() );

			if ( id1 != id2 && id1 > 0 && id2 > 0 )
				if ( compare.compare( counts[ id1 ], valProcessed ) || compare.compare( counts[ id2 ], valProcessed ) )
				{
					final int newId = dj.join( id1, id2 );
					final int otherId = newId == id1 ? id2 : id1;
					counts[ newId ] = counts[ newId ] + counts[ otherId ];
					counts[ otherId ] = 0;

					if ( id1 == 4234 || id2 == 4234 )
					{
						IDS.add( edge.getFirst() );
						IDS.add( edge.getSecond() );
//						System.out.println( ref );
					}

					if ( set != null && ( set.contains( edge.getFirst() ) || set.contains( edge.getSecond() ) ) )
					{
						final WeightedEdge ref = ( ( WeightedEdgePool ) EDGES.getRefPool() ).create();
						ref.set( edge.getFirst(), edge.getSecond(), edge.getV() );
						EDGES.add( ref );
					}
				}

		}
		return dj;
	}

	public static DisjointSets[] mergeRegionGraph(
			final Collection< WeightedEdge > graph,
			final long[] counts,
			final Predicate compare,
			final Function[] funcs )
	{
		final DisjointSets[] djs = new DisjointSets[ funcs.length ];
		final DisjointSets dj = new DisjointSets( counts.length );
		for ( int i = 0; i < djs.length; ++i )
			djs[i] = mergeRegionGraph( graph, counts, compare, funcs[i], dj ).clone();
		return djs;
	}

	public static DisjointSets[] mergeRegionGraph(
			final Collection< WeightedEdge > graph,
			final long[] counts,
			final Predicate compare,
			final double[] thresholds )
	{
		final DisjointSets[] djs = new DisjointSets[ thresholds.length ];

		final DisjointSets dj = new DisjointSets( counts.length );
		for ( int i = 0; i < djs.length; ++i )
		{
			final int fi = i;
			djs[ i ] = mergeRegionGraph( graph, counts, compare, ( v ) -> v < 1e-4 ? Double.NaN : thresholds[ fi ] * v * v, dj ).clone();
		}
		return djs;
	}

	public static RefArrayList< WeightedEdge > graphToList(
			final TLongDoubleHashMap graph,
			final long nLabels )
	{
		final long[] fromTo = new long[ 2 ];
		final long[] dim = new long[] { nLabels, nLabels };
		final WeightedEdgePool pool = new WeightedEdgePool( 1 );
		final RefArrayList< WeightedEdge > rg = new RefArrayList<>( pool );

		for ( final TLongDoubleIterator entry = graph.iterator(); entry.hasNext(); )
		{
			final WeightedEdge ref = pool.create();
			entry.advance();
			final long index = entry.key();
			final double val = entry.value();
			IntervalIndexer.indexToPosition( index, dim, fromTo );
			ref.set( fromTo[ 0 ], fromTo[ 1 ], val );
			rg.add( ref );

		}
		return rg;
	}

	public static class WeightedEdge extends PoolObject< WeightedEdge, DoubleMappedElement > implements Comparable< WeightedEdge >
	{

		protected static final int X_OFFSET = 0;

		protected static final int FIRST_OFFSET = X_OFFSET;

		protected static final int SECOND_OFFSET = FIRST_OFFSET + ByteUtils.LONG_SIZE;

		protected static final int V_OFFSET = SECOND_OFFSET + ByteUtils.LONG_SIZE;

		protected static final int SIZE_IN_BYTES = V_OFFSET + ByteUtils.DOUBLE_SIZE;

		public WeightedEdge( final WeightedEdgePool pool )
		{
			super( pool );
		}

		@Override
		public int compareTo( final WeightedEdge o )
		{
			return Double.compare( getV(), o.getV() );
		}

		@Override
		public int hashCode()
		{
			return Long.hashCode( getFirst() ) + Long.hashCode( getSecond() );
		}

		@Override
		public boolean equals( final Object o )
		{
			if ( o instanceof WeightedEdge )
			{
				final WeightedEdge e = ( WeightedEdge ) o;
				return getFirst() == e.getFirst() && getSecond() == e.getSecond();
			}
			else
				return false;
		}

		@Override
		public String toString()
		{
			return new StringBuilder()
					.append( getFirst() )
					.append( " " )
					.append( getSecond() )
					.append( " : " )
					.append( getV() )
					.toString();
		}

		public double getV()
		{
			return access.getDouble( V_OFFSET );
		}

		public long getFirst()
		{
			return access.getLong( FIRST_OFFSET );
		}

		public long getSecond()
		{
			return access.getLong( SECOND_OFFSET );
		}

		public void setFirst( final long first )
		{
			access.putLong( first, FIRST_OFFSET );
		}

		public void setSecond( final long second )
		{
			access.putLong( second, SECOND_OFFSET );
		}

		public void setV( final double v )
		{
			access.putDouble( v, V_OFFSET );
		}

		public WeightedEdge set( final long first, final long second, final double v )
		{
			setFirst( first );
			setSecond( second );
			setV( v );
			return this;
		}

		@Override
		protected void setToUninitializedState()
		{
//			set( 0, 0, 0.0 );
		}

	}

	public static class WeightedEdgePool extends Pool< WeightedEdge, DoubleMappedElement >
	{

		public WeightedEdgePool( final int initialCapacity )
		{
			this( initialCapacity, new WeightedEdgeFactory() );
		}

		@Override
		public WeightedEdge create( final WeightedEdge obj )
		{
			return super.create( obj );
		}

		public WeightedEdge create()
		{
			return super.create( createRef() );
		}

		public void delete( final WeightedEdge obj )
		{
			deleteByInternalPoolIndex( obj.getInternalPoolIndex() );
		}

		private WeightedEdgePool( final int initialCapacity, final WeightedEdgeFactory f )
		{
			super( initialCapacity, f );
			f.pool = this;
		}

		private static class WeightedEdgeFactory implements Factory< WeightedEdge, DoubleMappedElement >
		{

			private WeightedEdgePool pool;

			@Override
			public WeightedEdge createEmptyRef()
			{
				return new WeightedEdge( pool );
			}

			@Override
			public org.mastodon.pool.MemPool.Factory< DoubleMappedElement > getMemPoolFactory()
			{
				return SingleArrayMemPool.factory( DoubleMappedElementArray.factory );
			}

			@Override
			public Class< WeightedEdge > getRefClass()
			{
				return WeightedEdge.class;
			}

			@Override
			public int getSizeInBytes()
			{
				return WeightedEdge.SIZE_IN_BYTES;
			}

		}

	}

	public static interface Predicate
	{
		public boolean compare( double v1, double v2 );
	}

	public static interface Function
	{
		public double apply( double v );
	}

	public static < T > T get( final RandomAccess< T > access, final long index )
	{
		access.setPosition( index, 0 );
		return access.get();
	}

	public static < T > List< Future< T > > invokeAllAndWait( final ExecutorService es, final ArrayList< Callable< T > > tasks ) throws InterruptedException, ExecutionException
	{
		final List< Future< T > > futures = es.invokeAll( tasks );
		for ( final Future< T > f : futures )
			f.get();
		return futures;
	}

}
