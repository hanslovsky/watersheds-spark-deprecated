package de.hanslovsky.watersheds.rewrite;

import de.hanslovsky.watersheds.rewrite.ViewsFailureTest.AvgConverter;
import ij.ImageJ;
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
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.ConstantUtils;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class ViewsFailureTest3
{
	public static void main( final String[] args )
	{
		new ImageJ();
		final long[] dims = { 300, 300, 100, 3 };
		final ArrayImg< FloatType, FloatArray > imgs = ArrayImgs.floats( dims );
		for ( int z = 0; z < dims[ 2 ]; ++z )
			for ( final FloatType h : Views.hyperSlice( imgs, 2, z ) )
				h.set( z );

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( Views.interval( log( imgs ), imgs ) );

		final AvgConverter< FloatType, RealComposite< FloatType > > avgConv = new AvgConverter<>( 2 );

		final DiamondShape shape = new DiamondShape( 1 );

		final RandomAccessibleInterval< RealComposite< FloatType > > hs0 = Views.hyperSlice( collapsed, 2, 0l );
		final RandomAccessibleInterval< RealComposite< FloatType > > hs2 = Views.hyperSlice( collapsed, 2, 2l );
		final RandomAccessibleInterval< FloatType > avg0 = Converters.convert( hs0, avgConv, new FloatType() );
		final RandomAccessibleInterval< FloatType > avg2 = Converters.convert( hs2, avgConv, new FloatType() );
		final NeighborhoodsAccessible< FloatType > nh0 = shape.neighborhoodsRandomAccessible( avg0 );
		final NeighborhoodsAccessible< FloatType > nh2 = shape.neighborhoodsRandomAccessible( avg2 );

		final RandomAccess< Pair< Neighborhood< FloatType >, DoubleType > > avgNHAccess1 = Views.pair( nh0, ConstantUtils.constantRandomAccessible( new DoubleType(), nh0.numDimensions() ) ).randomAccess();
		avgNHAccess1.setPosition( new long[] { 150, 150 } );
		for ( final Cursor< FloatType > n = avgNHAccess1.get().getA().cursor(); n.hasNext(); )
			System.out.println( n.next() + " " + new Point( n ) );

		System.out.println();
		System.out.println( "  set to pos 2: " );
		System.out.println();

		final RandomAccess< Pair< Neighborhood< FloatType >, DoubleType > > avgNHAccess2 = Views.pair( nh2, ConstantUtils.constantRandomAccessible( new DoubleType(), nh2.numDimensions() ) ).randomAccess();
		avgNHAccess2.setPosition( new long[] { 150, 150 } );
		for ( final Cursor< FloatType > n = avgNHAccess2.get().getA().cursor(); n.hasNext(); )
			System.out.println( n.next() + " " + new Point( n ) );

		final RandomAccessibleInterval< FloatType > avg = Converters.convert( Views.collapseReal( imgs ), avgConv, new FloatType() );

		ImageJFunctions.show( avg );
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
