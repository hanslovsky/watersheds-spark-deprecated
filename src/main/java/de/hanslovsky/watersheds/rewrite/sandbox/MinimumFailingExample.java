package de.hanslovsky.watersheds.rewrite.sandbox;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import de.hanslovsky.watersheds.Util;
import de.hanslovsky.watersheds.rewrite.VisualizationVisitor;
import de.hanslovsky.watersheds.rewrite.WatershedsSparkWithRegionMergingLoadSegmentation;
import de.hanslovsky.watersheds.rewrite.WatershedsSparkWithRegionMergingLoadSegmentation.VisitorFactory;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger.MAX_AFFINITY_MERGER;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.EdgeCheck;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.AbstractInterval;
import net.imglib2.Dimensions;
import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.view.Views;

public class MinimumFailingExample
{

	public static abstract class RAI< T > extends AbstractInterval implements RandomAccessibleInterval< T >
	{

		public RAI( final Dimensions dimensions )
		{
			super( dimensions );
		}

		public RAI( final long[] dim )
		{
			super( dim );
		}

	};

	public static class FunctionRandomAccess< T > extends Point implements RandomAccess< T >
	{

		private final Function< Localizable, T > func;


		public FunctionRandomAccess( final int nDim, final Function< Localizable, T > func )
		{
			super( nDim );
			this.func = func;
		}

		@Override
		public T get()
		{
			return func.apply( this );
		}

		@Override
		public FunctionRandomAccess< T > copy()
		{
			return copyRandomAccess();
		}

		@Override
		public FunctionRandomAccess< T > copyRandomAccess()
		{
			return new FunctionRandomAccess< T >( numDimensions(), func );
		}

	}

	public static void main( final String[] args ) throws InterruptedException, ExecutionException
	{

		final int nBlocksZ = 3;
		final int nBlocksX = 4;
		final int nLabelsPerBlock = 5;
		final long[] dim = { 30, 4, nBlocksZ * nLabelsPerBlock };
		final long blockDimZ = dim[ 2 ] / nBlocksZ;
		final long blockDimX = dim[ 0 ] / nBlocksX;
		final long blockDimY = dim[ 1 ];
		final long[] blockDims = { blockDimX, blockDimY, blockDimZ };
		final long[] nBlocksPerDimension = IntStream.range( 0, dim.length ).mapToLong( i -> ( long ) Math.ceil( dim[ i ] * 1.0 / blockDims[ i ] ) ).toArray();
		final long nBlocks = Arrays.stream( nBlocksPerDimension ).reduce( 1, ( l1, l2 ) -> l1 * l2 );
		System.out.println( "nBlocks: " + nBlocks + " " + Arrays.toString( nBlocksPerDimension ) );
		final long[] labelDim = { blockDimX, blockDimY, 1 };
		final long[] nLabelsPerDimension = IntStream.range( 0, dim.length ).mapToLong( i -> ( long ) Math.ceil( dim[ i ] * 1.0 / labelDim[ i ] ) ).toArray();


		final RAI< LongType > labels = new RAI< LongType >( dim )
		{

			@Override
			public RandomAccess< LongType > randomAccess( final Interval interval )
			{
				return randomAccess();
			}

			@Override
			public RandomAccess< LongType > randomAccess()
			{
				final LongType f = new LongType();
				return new FunctionRandomAccess< LongType >( dim.length, l -> {
					final long[] labelPos = IntStream.range( 0, dim.length ).mapToLong( i -> l.getLongPosition( i ) / labelDim[ i ] ).toArray();
					final long label = IntervalIndexer.positionToIndex( labelPos, nLabelsPerDimension );
					f.set( label );
					return f;
				} );
			}

		};

		final TLongIntHashMap cmap = new TLongIntHashMap();
		final TLongLongHashMap counts = new TLongLongHashMap();
		final Random rng = new Random( 100 );
		for ( final LongType l : Views.flatIterable( labels ) )
			if ( !cmap.contains( l.get() ) ) {
				cmap.put( l.get(), rng.nextInt() );
				counts.put( l.get(), 1 );
			}
			else
				counts.put( l.get(), counts.get( l.get() ) + 1 );

		System.out.println( "Got " + counts.size() + " distinct labels." );

		final RAI< FloatType > affs = new RAI< FloatType >( Util.append( dim, dim.length ) )
		{

			@Override
			public RandomAccess< FloatType > randomAccess()
			{
				final float[] vals = { 0.0f, 1.0f, 1.0f };
				final long[] maxs = Arrays.stream( dim ).map( i -> i - 1 ).toArray();
				final float oob = Float.NaN;
				final FloatType f = new FloatType();
				return new FunctionRandomAccess<>( dim.length + 1, l -> {
					final int selectedDim = l.getIntPosition( dim.length );
					final long pos = l.getIntPosition( selectedDim );
					f.set( pos == maxs[ selectedDim ] ? oob : vals[ selectedDim ] );
					return f;
				} );
			}

			@Override
			public RandomAccess< FloatType > randomAccess( final Interval interval )
			{
				return randomAccess();
			}

		};

		final TLongIntHashMap blockCMap = new TLongIntHashMap();
		final ArrayImg< LongType, LongArray > blocks = ArrayImgs.longs( dim );
		for ( final ArrayCursor< LongType > b = blocks.cursor(); b.hasNext(); )
		{
			final LongType l = b.next();
			final int x = b.getIntPosition( 0 ) < dim[ 0 ] / 3 ? 0 : b.getIntPosition( 0 ) < dim[ 0 ] / 3 * 2 ? 1 : 2;
			final long label = b.getLongPosition( 2 ) / blockDimZ * 3 + x;
			l.set( label );
			if ( !blockCMap.contains( label ) )
				blockCMap.put( label, rng.nextInt() );
		}

		for ( final LongType l : Views.flatIterable( labels ) )
			if ( !cmap.contains( l.get() ) )
				cmap.put( l.get(), rng.nextInt() );

		final SparkConf conf = new SparkConf().setMaster( "local[1]" ).setAppName( MethodHandles.lookup().lookupClass().getSimpleName() );
		Logger.getRootLogger().setLevel( Level.ERROR );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );
		final MAX_AFFINITY_MERGER merger = new EdgeMerger.MAX_AFFINITY_MERGER();
//		final FunkyWeight weightFunc = new EdgeWeight.FunkyWeight();
		final EdgeCheck edgeCheck = new EdgeCheck.AlwaysTrue();
		final EdgeWeight weightFunc = ( Serializable & EdgeWeight ) ( affinity, count1, count2 ) -> 1.0 - affinity;

		final VisitorFactory visFac = ( sc1, labels1, blocks1, blockToInitialBlockMapBC, labelBlocks ) -> {

			final VisualizationVisitor vis = new VisualizationVisitor( sc1, blockToInitialBlockMapBC, new long[] { blockDimX, blockDimY, blockDimZ }, labelBlocks, labels1, blocks1, new ArrayImgFactory<>(), null );
			return vis;
		};

		System.out.println( "Running region merging." );
		WatershedsSparkWithRegionMergingLoadSegmentation.run(
				sc,
				affs,
				labels,
				new long[] { blockDimX, blockDimY, blockDimZ },
				new ArrayImgFactory< LongType >(),
				merger,
				weightFunc,
				edgeCheck,
				0.5,
				visFac );


	}

}
