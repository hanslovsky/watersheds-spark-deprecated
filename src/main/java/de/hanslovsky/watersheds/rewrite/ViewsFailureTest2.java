package de.hanslovsky.watersheds.rewrite;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.algorithm.neighborhood.DiamondShape.NeighborhoodsAccessible;
import net.imglib2.algorithm.neighborhood.Neighborhood;
import net.imglib2.converter.AbstractConvertedRandomAccess;
import net.imglib2.converter.AbstractConvertedRandomAccessible;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class ViewsFailureTest2
{
	public static void main( final String[] args )
	{
		final long[] dims = { 3, 3, 2, 1 };
		final ArrayImg< FloatType, FloatArray > imgs = ArrayImgs.floats( dims );
		for ( int z = 0; z < dims[ 2 ]; ++z )
			for ( final FloatType h : Views.hyperSlice( imgs, 2, z ) )
				h.set( z );

		final boolean logPosition = false;
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( logPosition ? Views.interval( log( imgs ), imgs ) : imgs );

		final DiamondShape shape = new DiamondShape( 1 );

		final long[] xyPosition = { 1, 1 };

		final int sliceAxis = 2;
		final long slicePos = 1;

		final RandomAccessibleInterval< RealComposite< FloatType > > hs1 = Views.hyperSlice( collapsed, sliceAxis, slicePos );

		final NeighborhoodsAccessible< RealComposite< FloatType > > nh1 = shape.neighborhoodsRandomAccessible( hs1 );
		final NeighborhoodsAccessible< RealComposite< FloatType > > nh2 = shape.neighborhoodsRandomAccessible( Views.extendValue( hs1, Views.collapseReal( ArrayImgs.floats( 1, dims[ 3 ] ) ).randomAccess().get() ) );

		System.out.println( "Without extension: " );
		iterateAndPrintComposite( nh1.randomAccess(), xyPosition, ( int ) dims[ 3 ] );

		System.out.println( "With extension: " );
		iterateAndPrintComposite( nh2.randomAccess(), xyPosition, ( int ) dims[ 3 ] );

	}

	// ---------------------------------------------------------------------------------------------------------
	// HELPERS
	// ---------------------------------------------------------------------------------------------------------

	public static void iterateAndPrintComposite( final RandomAccess< ? extends Neighborhood< ? extends Composite< ? > > > nh, final long[] pos, final int nDim )
	{
		nh.setPosition( pos );
		for ( final Cursor< ? extends Composite< ? > > n = nh.get().cursor(); n.hasNext(); )
		{
			final Composite< ? > c = n.next();
			System.out.println( IntStream.range( 0, nDim ).mapToObj( i -> c.get( i ) ).collect( Collectors.toList() ) + " " + new Point( n ) );
		}
	}

	public static < T > LoggingAccessible< T > log( final RandomAccessible< T > ra )
	{
		return new LoggingAccessible<>( ra );
	}

	public static class LoggingAccessible< T > extends AbstractConvertedRandomAccessible< T, T >
	{

		public LoggingAccessible( final RandomAccessible< T > source )
		{
			super( source );
		}

		@Override
		public AbstractConvertedRandomAccess< T, T > randomAccess()
		{
			return new LoggingAccess<>( source.randomAccess() );
		}

		@Override
		public AbstractConvertedRandomAccess< T, T > randomAccess( final Interval interval )
		{
			return randomAccess();
		}

	}

	public static class LoggingAccess< T > extends AbstractConvertedRandomAccess< T, T >
	{

		public LoggingAccess( final RandomAccess< T > source )
		{
			super( source );
		}

		@Override
		public T get()
		{
			System.out.println( "Pos is: " + new Point( source ) );
			return source.get();
		}

		@Override
		public AbstractConvertedRandomAccess< T, T > copy()
		{
			return new LoggingAccess<>( source.copyRandomAccess() );
		}

	}

}

