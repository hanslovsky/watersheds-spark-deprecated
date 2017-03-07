package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.function.Function;

import bdv.img.h5.H5Utils;
import de.hanslovsky.watersheds.rewrite.util.Util;
import ij.ImageJ;
import net.imglib2.Point;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.morphology.distance.DistanceTransform;
import net.imglib2.algorithm.morphology.distance.DistanceTransform.DISTANCE_TYPE;
import net.imglib2.algorithm.morphology.watershed.Distance;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized;
import net.imglib2.algorithm.morphology.watershed.Watershed;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class InitialSeededWatershedBlock
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.DEBUG );
	}

	public static class RunWatersheds implements Function< ArrayImg< FloatType, FloatArray >, Tuple2< ArrayImg< LongType, LongArray >, Long > >
	{

		private final boolean processBySlice;

		private final double dtWeight;

		public RunWatersheds( final boolean processBySlice, final double dtWeight )
		{
			super();
			this.processBySlice = processBySlice;
			this.dtWeight = dtWeight;
		}

		@Override
		public Tuple2< ArrayImg< LongType, LongArray >, Long > call( final ArrayImg< FloatType, FloatArray > affs ) throws Exception
		{
			final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( affs );
			final long[] dims = Intervals.dimensionsAsLongArray( collapsed );
			LOG.debug( "dims: " + Arrays.toString( dims ) );
			final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( dims );

			final ArrayImgFactory< FloatType > fac = new ArrayImgFactory<>();
			final long nLabels;
			if ( dims.length == 3 && this.processBySlice )
			{
				long offset = 1;
				for ( long z = 0; z < dims[ 2 ]; ++z )
					offset += process( Views.hyperSlice( collapsed, 2, z ), Views.hyperSlice( labels, 2, z ), fac, new FloatType(), dtWeight, offset );
				nLabels = offset - 1;
			}
			else
				nLabels = process( collapsed, labels, fac, new FloatType(), dtWeight, 1 );

			return new Tuple2<>( labels, nLabels );
		}

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
		final double norm = 1.0 / nDim;
		final RandomAccessibleInterval< A > avg = Converters.convert( affs, ( s, t ) -> {
			t.setZero();
			for ( int i = 0; i < nDim; ++i )
				t.add( s.get( i ) );
			t.mul( norm );
		}, ext.createVariable() );
		final Img< A > dt = fac.create( affs, ext );
		DistanceTransform.transform( avg, dt, DISTANCE_TYPE.EUCLIDIAN, 1, dtWeight );
//		ImageJFunctions.show( dt, "dt" );

		final A thresh = ext.createVariable();
		thresh.setZero();
		final LocalExtrema.LocalNeighborhoodCheck< Point, A > check = new LocalExtrema.MaximumCheck<>( thresh );
		final A extremaExtension = ext.createVariable();
		extremaExtension.setReal( -1.0 );
		final ArrayList< Point > extrema = LocalExtrema.findLocalExtrema( Views.interval( Views.extendValue( dt, extremaExtension ), Intervals.expand( dt, 1 ) ), check, Executors.newFixedThreadPool( 1 ) );
		LOG.trace( "Found extrema: " + extrema );

		final RandomAccess< LongType > lAccess = labels.randomAccess();
		long l = firstLabel;
		final int[] distance = IntStream.generate( () -> 0 ).limit( dt.numDimensions() ).toArray();
		for ( final Point p : extrema ) {
			lAccess.setPosition( p );
			lAccess.move( distance );
			lAccess.get().set( l );
			++l;
		}

		final ArrayImg< LongType, LongArray > labelsCopy = ArrayImgs.longs( Intervals.dimensionsAsLongArray( labels ) );
		for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( labels, labelsCopy ), labels ) )
			p.getB().set( p.getA() );

//		ImageJFunctions.show( Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsCopy, ( s, t ) -> {
//			t.set( s.get() == 0 ? 0.0f : 1.0f );
//		}, new FloatType() ) );


		final Distance< A > dist = ( comparison, reference, position, seedPosition, numberOfSteps ) -> 1.0 - comparison.getRealDouble();
//		final Distance< A > dist = ( comparison, reference, position, seedPosition, numberOfSteps ) -> Math.abs( comparison.getRealDouble() - reference.getRealDouble() );

		Watershed.flood( avg, labels, new DiamondShape( 1 ), new LongType( 0 ), new LongType( -1 ), dist, ( size, t ) -> ArrayImgs.longs( size ), new HierarchicalPriorityQueueQuantized.Factory( 256, 0.0, 1.0 ) );

		return extrema.size();

	}

	public static void main( final String[] args ) throws Exception
	{
		new ImageJ();

		PropertyConfigurator.configure( new File( "resources/log4j.properties" ).getAbsolutePath() );

		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt.h5";
//		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";
		final CellImg< FloatType, ?, ? > input = H5Utils.loadFloat( path, "main", new int[] { 300, 300, 100, 3 } );
		final ArrayImg< FloatType, FloatArray > affs = ArrayImgs.floats( Intervals.dimensionsAsLongArray( input ) );

		final int nDim = affs.numDimensions() - 1;
		final double norm = 1.0 / nDim;
		final RandomAccessibleInterval< FloatType > avg = Converters.convert( Views.collapseReal( affs ), ( s, t ) -> {
			t.setZero();
			for ( int i = 0; i < nDim; ++i )
				t.add( s.get( i ) );
			t.mul( norm );
		}, new FloatType() );

		final int[] perm = Util.getFlipPermutation( input.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > inputPerm = Views.permuteCoordinates( input, perm, input.numDimensions() - 1 );

		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( inputPerm, affs ), affs ) )
			p.getB().set( p.getA() );

		final RunWatersheds rw = new RunWatersheds( true, 0.001 );
		final Tuple2< ArrayImg< LongType, LongArray >, Long > labelsAndcounts = rw.call( affs );
		LOG.info( "Found " + labelsAndcounts._2() + " distinct labels." );

		final int[] colors = new int[ labelsAndcounts._2().intValue() + 1 ];
		final Random rng = new Random( 100 );
		colors[ 0 ] = 0;
		for ( int i = 1; i < colors.length; ++i )
			colors[i] = rng.nextInt();

		ImageJFunctions.show( avg, "aff avg" );
		ImageJFunctions.show( Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsAndcounts._1(), ( s, t ) -> {
			t.set( colors[ Math.max( s.getInteger(), 0 ) ] );
		}, new ARGBType() ), "labels" );
	}


}
