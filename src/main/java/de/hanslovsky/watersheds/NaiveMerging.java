package de.hanslovsky.watersheds;

import java.util.Arrays;
import java.util.Random;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.Cursor;
import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class NaiveMerging
{

	public static < T extends RealType< T > > void merge( final RandomAccessibleInterval< RealComposite< T > > affs, final DisjointSets dj, final double threshold, final int... strides )
	{
		for ( final Cursor< RealComposite< T > > c = Views.flatIterable( affs ).cursor(); c.hasNext(); )
		{
			final RealComposite< T > rc = c.next();
			final int index = ( int ) IntervalIndexer.positionToIndex( c, affs );

			for ( int d = 0; d < strides.length; ++d )
				if ( rc.get( d ).getRealDouble() > threshold )
				{
					final int r1 = dj.findRoot( index );
					final int r2 = dj.findRoot( index + strides[ d ] );
					if ( r1 != r2 )
						dj.join( r1, r2 );
				}
		}
	}

	public static < T extends RealType< T >, U extends RealType< U > > RandomAccessibleInterval< RealComposite< U > > prepareAffinities( final RandomAccessibleInterval< T > data, final ImgFactory< U > fac, final U u, final int... perm )
	{
		return prepareAffinities( data, fac, u, ( s, t ) -> {
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

	public static void main( final String... args )
	{
		final int[] cellSize = new int[] { 60, 60, 60, 2 };

		final String HOME_DIR = System.getProperty( "user.home" );
		final String path = HOME_DIR + String.format( "/Dropbox/misc/excerpt.h5" );

		final CellImg< FloatType, ?, ? > data = H5Utils.loadFloat( path, "main", cellSize );

		System.out.println( data.numDimensions() + " " + Arrays.toString( getFlipPermutation( data.numDimensions() - 1 ) ) );
		final RandomAccessibleInterval< RealComposite< FloatType > > affs = prepareAffinities( data, new ArrayImgFactory<>(), new FloatType(), getFlipPermutation( data.numDimensions() - 1 ) );

		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( affs );

		for ( int d = 0; d < affs.numDimensions(); ++d )
		{
			final IntervalView< RealComposite< FloatType > > hs = Views.hyperSlice( affs, d, affs.max( d ) );
			for ( final RealComposite< FloatType > c : hs )
				c.get( d ).set( Float.NaN );
		}

		final int nPixels = ( int ) Intervals.numElements( affs );
		final DisjointSets dj = new DisjointSets( nPixels );

		final double threshold = 0.99;

		final int[] stride = getStride( affs );

		merge( affs, dj, threshold, stride );

		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );
		for ( int i = 0; i < nPixels; ++i )
		{
			final int r = dj.findRoot( i );
			if ( !cmap.contains( r ) )
				cmap.put( r, rng.nextInt() );
		}

		final ArrayImg< LongType, LongArray > img = ArrayImgs.longs( dimsNoChannels );
		int k = 0;
		for ( final ArrayCursor< LongType > i = img.cursor(); i.hasNext(); )
			i.next().set( dj.findRoot( k++ ) );

		final BdvStackSource< ARGBType > bdv = BdvFunctions.show( Converters.convert( ( RandomAccessibleInterval< LongType > ) img, ( s, t ) -> {
			t.set( cmap.get( s.get() ) );
		}, new ARGBType() ), "ok", affs.numDimensions() == 2 ? BdvOptions.options().is2D() : BdvOptions.options() );

		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			t.set( 65000.0f * Math.max( s.get( 0 ).get(), s.get( 1 ).get() ) );
		}, new FloatType() ), "affs max", BdvOptions.options().addTo( bdv ) );

	}

}
