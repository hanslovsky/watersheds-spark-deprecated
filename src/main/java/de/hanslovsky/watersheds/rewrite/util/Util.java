package de.hanslovsky.watersheds.rewrite.util;

import java.util.Comparator;
import java.util.Random;

import org.apache.spark.api.java.function.PairFunction;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class Util
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static long[] getCurrentChunkDimensions( final long[] offset, final long[] dims, final int[] intervalDims )
	{
		final long[] intervalDimensionsTruncated = new long[ dims.length ];
		for ( int d = 0; d < intervalDimensionsTruncated.length; ++d )
			intervalDimensionsTruncated[ d ] = Math.min(
					dims[ d ] - offset[ d ],
					intervalDims[ d ] );
		return intervalDimensionsTruncated;
	}

	public static long positionToIndex( final HashableLongArray pos, final long[] dim )
	{
		return IntervalIndexer.positionToIndex( pos.getData(), dim );
	}

	public static class KeyAndCountsComparator< V > implements
	Comparator< Tuple2< HashableLongArray, V > >
	{
		private final long[] dims;

		public KeyAndCountsComparator( final long[] dims )
		{
			super();
			this.dims = dims;
		}

		@Override
		public int compare( final Tuple2< HashableLongArray, V > o1, final Tuple2< HashableLongArray, V > o2 )
		{
			return Long.compare( positionToIndex( o1._1(), dims ), positionToIndex( o2._1(), dims ) );
		}
	}

	public static class DropSecondValue< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, K, V1 >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 3499313015214140909L;

		@Override
		public Tuple2< K, V1 > call( final Tuple2< K, Tuple2< V1, V2 > > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t._2()._1() );
		}

	}

	public static < T extends RealType< T >, U extends RealType< U > > RandomAccessibleInterval< RealComposite< U > > prepareAffinities( final RandomAccessibleInterval< T > data, final ImgFactory< U > fac, final U u, final int... perm )
	{
		return Util.prepareAffinities( data, fac, u, ( s, t ) -> {
			t.setReal( s.getRealDouble() );
		}, perm );
	}

	public static < T extends RealType< T >, U extends RealType< U > > RandomAccessibleInterval< RealComposite< U > > prepareAffinities( final RandomAccessibleInterval< T > data, final ImgFactory< U > fac, final U u, final Converter< T, U > conv, final int... perm )
	{
		final Img< U > input = fac.create( data, u );
		for ( final Pair< T, U > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, data.numDimensions() - 1 ), input ), input ) )
			conv.convert( p.getA(), p.getB() );

		final CompositeIntervalView< U, RealComposite< U > > affs = Views.collapseReal( input );
		return affs;
	}

	public static int[] getFlipPermutation( final int numDimensions )
	{
		final int[] perm = new int[ numDimensions ];
		for ( int d = 0, flip = numDimensions - 1; d < numDimensions; ++d, --flip )
			perm[ d ] = flip;
		return perm;
	}

	public static int[] getStride( final Dimensions dim )
	{
		final int[] stride = new int[ dim.numDimensions() ];
		stride[ 0 ] = 1;
		for ( int d = 1; d < stride.length; ++d )
			stride[ d ] = stride[ d - 1 ] * ( int ) dim.dimension( d - 1 );
		return stride;
	}

	public static int[] dropLast( final int[] input )
	{
		final int[] output = new int[ input.length - 1 ];
		System.arraycopy( input, 0, output, 0, output.length );
		return output;
	}

	public static long[] dropLast( final long[] input )
	{
		final long[] output = new long[ input.length - 1 ];
		System.arraycopy( input, 0, output, 0, output.length );
		return output;
	}

	public static long[] append( final long[] source, final long... addendum )
	{
		final long[] result = new long[ source.length + addendum.length ];
		System.arraycopy( source, 0, result, 0, source.length );
		System.arraycopy( addendum, 0, result, source.length, addendum.length );
		return result;
	}

	public static BdvOptions bdvOptions( final Dimensions dim )
	{
		return dim.numDimensions() == 2 ? BdvOptions.options().is2D() : BdvOptions.options();
	}

	public static < T extends RealType< T > > T min( final Composite< T > c, final T t, final long length )
	{
		t.set( c.get( 0 ) );
		for ( long i = 1; i < length; ++i )
		{
			final T v = c.get( i );
			if ( v.compareTo( t ) < 0 )
				t.set( v );
		}
		return t;
	}

	public static < T extends RealType< T > > T max( final Composite< T > c, final T t, final long length )
	{
		t.set( c.get( 0 ) );
		for ( long i = 1; i < length; ++i )
		{
			final T v = c.get( i );
			if ( v.compareTo( t ) > 0 )
				t.set( v );
		}
		return t;
	}

	public static < T extends NumericType< T > > T sum( final Composite< T > c, final T t, final long length )
	{
		t.set( c.get( 0 ) );
		for ( long i = 1; i < length; ++i )
			t.add( c.get( i ) );
		return t;
	}

	public static void main( final String... args )
	{
		final int[] cellSize = new int[] { 300, 300, 100, 3 };
		final String path = Util.HOME_DIR + String.format( "/Dropbox/misc/excerpt.h5" );

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data = H5Utils.loadFloat( path, "main", cellSize );

		final long inputSize = Intervals.numElements( data );

		System.out.println( "Loaded data (" + inputSize + ")" );

		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > input = Views.permuteCoordinates( data, perm, data.numDimensions() - 1 );

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );

//		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
//			Util.max( s, t, affs.numDimensions() );
//			t.mul( ( 1 << 16 ) * 1.0 );
//		}, new FloatType() ), "affs", Util.bdvOptions( affs ) );

//		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
//			t.set( ( int ) ( 255 * s.get( 0 ).get() ) << 16 | ( int ) ( 255 * s.get( 1 ).get() ) << 8 | ( int ) ( 255 * s.get( 2 ).get() ) << 0 );
//		}, new ARGBType() ), "affs color", Util.bdvOptions( affs ) );

		final BdvStackSource< FloatType > bdv = BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			t.set( ( 1 << 16 ) * s.get( 0 ).get() );
		}, new FloatType() ), "affsChannels" );
		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			t.set( ( 1 << 16 ) * s.get( 1 ).get() );
		}, new FloatType() ), "affsChannels", BdvOptions.options().addTo( bdv ) );
		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			t.set( ( 1 << 16 ) * s.get( 2 ).get() );
		}, new FloatType() ), "affsChannels", BdvOptions.options().addTo( bdv ) );
		bdv.getBdvHandle().getSetupAssignments().getConverterSetups().get( 0 ).setColor( new ARGBType( ARGBType.red( 255 ) ) );
		bdv.getBdvHandle().getSetupAssignments().getConverterSetups().get( 1 ).setColor( new ARGBType( ARGBType.green( 255 ) ) );
		bdv.getBdvHandle().getSetupAssignments().getConverterSetups().get( 2 ).setColor( new ARGBType( ARGBType.blue( 255 ) ) );

		final float thresh = 0.95f;
		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			Util.max( s, t, affs.numDimensions() );
			t.set( t.get() > thresh ? ( 1 << 16 ) * 1.0f : 0.0f );
		}, new FloatType() ), "thresh", Util.bdvOptions( affs ) );

		final String p2 = Util.HOME_DIR + String.format( "/Dropbox/misc/excerpt.h5" );
		final RandomAccessibleInterval< LongType > data2 = H5Utils.loadUnsignedLong( p2, "seg-200", new int[] { 100, 300, 300 } );
		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random();
		BdvFunctions.show( Converters.convert( data2, ( s, t ) -> {
			if ( !cmap.contains( s.get() ) )
				cmap.put( s.get(), rng.nextInt() );
			t.set( cmap.get( s.get() ) );
		},
				new ARGBType() ), "seg-200" );

	}

	public static long findRoot( final TLongLongHashMap assignments, final long index )
	{
		long i1 = index, i2 = index;

		while ( i1 != assignments.get( i1 ) )
			i1 = assignments.get( i1 );

		while ( i2 != assignments.get( i2 ) )
		{
			final long tmp = assignments.get( i2 );
			assignments.put( i2, i1 );
			i2 = tmp;

		}
		return i1;
	}

	public static RandomAccessibleInterval< ARGBType > toColor( final RandomAccessibleInterval< LongType > rai, final TLongIntHashMap cmap )
	{
		return Converters.convert( rai, ( s, t ) -> {
			t.set( cmap.get( s.get() ) );
		}, new ARGBType() );
	}

	public static < T extends IntegerType< T > > TLongLongHashMap countLabels( final RandomAccessibleInterval< T > labels )
	{
		final TLongLongHashMap counts = new TLongLongHashMap();
		for ( final T l : Views.flatIterable( labels ) )
		{
			final long ll = l.getIntegerLong();
			counts.put( ll, counts.contains( ll ) ? counts.get( ll ) + 1 : 1 );
		}
		return counts;
	}

}
