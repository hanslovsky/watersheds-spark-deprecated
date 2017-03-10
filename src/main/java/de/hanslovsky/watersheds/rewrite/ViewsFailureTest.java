package de.hanslovsky.watersheds.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import bdv.img.h5.H5Utils;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import ij.ImageJ;
import net.imglib2.Point;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized;
import net.imglib2.algorithm.morphology.watershed.Watershed;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class ViewsFailureTest
{

	public static void main( final String[] args ) throws InterruptedException, ExecutionException
	{
		new ImageJ();

		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt.h5";
//		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";
		final CellImg< FloatType, ?, ? > input = H5Utils.loadFloat( path, "main", new int[] { 300, 300, 100, 3 } );
		final ArrayImg< FloatType, FloatArray > affs = ArrayImgs.floats( Intervals.dimensionsAsLongArray( input ) );
		final int[] perm = Util.getFlipPermutation( input.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > inputPerm = Views.permuteCoordinates( input, perm, input.numDimensions() - 1 );

		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( inputPerm, affs ), affs ) )
			p.getB().set( p.getA() );

		ImageJFunctions.show( Views.hyperSlice( affs, 3, 0 ) );

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( Views.interval( ViewsFailureTest2.log( affs ), affs ) );
		ImageJFunctions.show( Converters.convert( Views.collapseReal( affs ), new AvgConverter<>( 2 ), new FloatType() ) );
		final long[] dims = Intervals.dimensionsAsLongArray( collapsed );
		final RandomAccessibleInterval< LongType > labels = ArrayImgs.longs( dims );

		final long nLabels;
		long offset = 1;
		for ( long z = 1; z < dims[ 2 ]; ++z )
		{
			final IntervalView< RealComposite< FloatType > > affC = Views.hyperSlice( collapsed, 2, z );
			System.out.println( "Processing for z=" + z + " " + Arrays.toString( dims ) + " " + Arrays.toString( Intervals.dimensionsAsLongArray( Views.hyperSlice( collapsed, 2, z ) ) ) + " " + Arrays.toString( Intervals.dimensionsAsLongArray( Views.hyperSlice( labels, 2, z ) ) ) );
			offset += process( affC, Views.hyperSlice( labels, 2, z ), new ArrayImgFactory<>(), new FloatType(), 0.0001, offset );
		}
		nLabels = offset - 1;

		ImageJFunctions.show( Converters.convert( Views.collapseReal( affs ), new AvgConverter<>( 2 ), new FloatType() ) );
		ImageJFunctions.show( Converters.convert( labels, new ColorMapConverter<>(), new ARGBType() ) );


	}

	public static < A extends RealType< A >, C extends Composite< A >, L extends IntegerType< L > > int process(
			final RandomAccessibleInterval< C > affs,
			final RandomAccessibleInterval< LongType > labels,
			final ImgFactory< A > fac,
			final A ext,
			final double dtWeight,
			final long firstLabel ) throws InterruptedException, ExecutionException
	{
		final int nDim = labels.numDimensions();
		final RandomAccessibleInterval< A > avg = Converters.convert( affs, new AvgConverter<>( 2 ), ext.createVariable() );
		final Img< A > dt = fac.create( affs, ext );
		final Img< A > avgCopy = fac.create( avg, ext );
		DistanceTransform.transform( avg, dt, DISTANCE_TYPE.EUCLIDIAN, 1, dtWeight );

		final A thresh = ext.createVariable();
		thresh.setZero();
		final LocalExtrema.LocalNeighborhoodCheck< Point, A > check = new LocalExtrema.MaximumCheck<>( thresh );
		final A extremaExtension = ext.createVariable();
		extremaExtension.setReal( -1.0 );
		final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( Views.extendValue( dt, extremaExtension ), Intervals.expand( dt, 1 ) ), check, Executors.newFixedThreadPool( 1 ) );
		System.out.println( extrema );

		final Watershed.WatershedDistance< A > dist = ( comparison, reference ) -> 1.0 - comparison.getRealDouble();

		final AtomicLong id = new AtomicLong( firstLabel );
		// WHY CAN I NOT USE AVG? WHY DOES IT USE FIRST SLICE OF AFFINITIES
		// THEN? WHYYYYYYYY?
		// WHY DO I NEED TO COPY AVG? THIS SUCKS!!!
//		for ( final Pair< A, A > p : Views.interval( Views.pair( avg, avgCopy ), avgCopy ) )
//			p.getB().set( p.getA() );
		Watershed.flood(
				avg, // dt,
				labels, // Views.offset( labels, Intervals.minAsLongArray(
				// labels ) ),
				extrema,
				new DiamondShape( 1 ),
				new LongType( 0 ),
				new LongType( -1 ),
				dist,
				new ArrayImgFactory< LongType >(), new HierarchicalPriorityQueueQuantized.Factory( 256, 0.0, 1.0 ),
				() -> id.getAndIncrement() );

		return extrema.size();

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

	public static class ColorMapConverter< T extends IntegerType< T > > implements Converter< T, ARGBType > {

		public static interface ColorService {
			int nextColor();
		}

		public static class RngColorService implements ColorService
		{


			private final Random rng;

			public RngColorService()
			{
				this( new Random() );
			}

			public RngColorService( final Random rng )
			{
				super();
				this.rng = rng;
			}

			@Override
			public int nextColor()
			{
				return rng.nextInt();
			}

		}

		private final TLongIntHashMap cmap;

		private final ColorService colorService;

		public ColorMapConverter()
		{
			this( new TLongIntHashMap(), new RngColorService() );
		}

		public ColorMapConverter( final TLongIntHashMap cmap, final ColorService colorService )
		{
			super();
			this.cmap = cmap;
			this.colorService = colorService;
		}

		@Override
		public void convert( final T input, final ARGBType output )
		{
			final long i = input.getIntegerLong();
			if ( !cmap.contains( i ) ) {
				final int c = colorService.nextColor();
				cmap.put( i, c );
				output.set( c );
			}
			else
				output.set( cmap.get( i ) );

		}

	}

}
