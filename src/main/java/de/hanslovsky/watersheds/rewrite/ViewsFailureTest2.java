package de.hanslovsky.watersheds.rewrite;

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
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class ViewsFailureTest2
{
	public static void main( final String[] args )
	{
		final long[] dims = { 3, 3, 2, 3 };
		final ArrayImg< FloatType, FloatArray > imgs = ArrayImgs.floats( dims );
		for ( int z = 0; z < dims[ 2 ]; ++z )
			for ( final FloatType h : Views.hyperSlice( imgs, 2, z ) )
				h.set( z );

		final boolean logPosition = false;
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( logPosition ? Views.interval( log( imgs ), imgs ) : imgs );

		final AvgConverter< FloatType, RealComposite< FloatType > > avgConv = new AvgConverter<>( 2 );

		final DiamondShape shape = new DiamondShape( 1 );

		final long[] xyPosition = { 1, 1 };

		final int sliceAxis = 2;
		final long slicePos = 1;


		final RandomAccessibleInterval< RealComposite< FloatType > > hs1 = Views.hyperSlice( collapsed, sliceAxis, slicePos );
		final RandomAccessibleInterval< FloatType > avg1 = Converters.convert( hs1, avgConv, new FloatType() );
		final NeighborhoodsAccessible< FloatType > nh1 = shape.neighborhoodsRandomAccessible( avg1 );
		final NeighborhoodsAccessible< FloatType > nh2 = shape.neighborhoodsRandomAccessible( Views.extendValue( avg1, new FloatType( Float.NaN ) ) );

		System.out.println( "Without extension: " );
		iterateAndPrint( nh1.randomAccess(), xyPosition );

		System.out.println( "With extension: " );
		iterateAndPrint( nh2.randomAccess(), xyPosition );

	}

	// ---------------------------------------------------------------------------------------------------------
	// HELPERS
	// ---------------------------------------------------------------------------------------------------------

	public static void iterateAndPrint( final RandomAccess< ? extends Neighborhood< ? > > nh, final long[] pos )
	{
		nh.setPosition( pos );
		for ( final Cursor< ? > n = nh.get().cursor(); n.hasNext(); )
			System.out.println( n.next() + " " + new Point( n ) );
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

	public static class AvgConverter< T extends RealType< T >, C extends Composite< T > > implements Converter< C, T >
	{

		private final int nDim;

		public AvgConverter( final int nDim )
		{
			super();
			this.nDim = nDim;
		}

		@Override
		public void convert( final C input, final T output )
		{
			int count = 0;
			output.setZero();
			for ( int i = 0; i < nDim; ++i )
				if ( !Double.isNaN( input.get( i ).getRealDouble() ) )
				{
					output.add( input.get( i ) );
					++count;
				}
			if ( count > 0 )
				output.mul( 1.0 / count );
		}

	}
}
