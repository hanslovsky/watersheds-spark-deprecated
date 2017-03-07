package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.function.Function;

import bdv.img.h5.H5Utils;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TIntLongHashMap;
import ij.ImageJ;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.algorithm.morphology.watershed.affinity.CompositeFactory;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
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

public class InitialThresholdBlock
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.DEBUG );
	}

	public static class Threshold implements Function< ArrayImg< FloatType, FloatArray >, Tuple2< ArrayImg< LongType, LongArray >, Long > >
	{

		private final boolean processBySlice;

		private final double threshold;

		public Threshold( final boolean processBySlice, final double dtWeight )
		{
			super();
			this.processBySlice = processBySlice;
			this.threshold = dtWeight;
		}

		@Override
		public Tuple2< ArrayImg< LongType, LongArray >, Long > call( final ArrayImg< FloatType, FloatArray > affs ) throws Exception
		{
			final CompositeIntervalView< FloatType, RealComposite< FloatType > > collapsed = Views.collapseReal( affs );
			final long[] dims = Intervals.dimensionsAsLongArray( collapsed );
			LOG.debug( "dims: " + Arrays.toString( dims ) );

			for ( int d = 0; d < dims.length; ++d )
				for ( final RealComposite< FloatType > a : Views.hyperSlice( collapsed, d, collapsed.max( d ) ) )
					a.get( d ).set( Float.NaN );

			final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( dims );

			final CompositeFactory< FloatType, RealComposite< FloatType > > fac = size -> Views.collapseReal( ArrayImgs.floats( 1, size ) ).randomAccess().get();
			final long nLabels;
			if ( dims.length == 3 && this.processBySlice )
			{
				long offset = 0;
				for ( long z = 0; z < dims[ 2 ]; ++z )
					offset += process( Views.hyperSlice( collapsed, 2, z ), Views.hyperSlice( labels, 2, z ), fac, threshold, offset ).size();
				nLabels = offset - 1;
			}
			else
				nLabels = process( collapsed, labels, fac, threshold, 0 ).size();

			return new Tuple2<>( labels, nLabels );
		}

	}

	public static < A extends RealType< A >, C extends Composite< A >, L extends IntegerType< L > > TIntLongHashMap process(
			final RandomAccessibleInterval< C > affs,
			final RandomAccessibleInterval< LongType > labels,
			final CompositeFactory< A, C > cFac,
			final double threshold,
			final long offset ) throws InterruptedException, ExecutionException
	{
		final int nDim = labels.numDimensions();

		final C extension = cFac.create( nDim );
		for ( int d = 0; d < nDim; ++d )
			extension.get( d ).setReal( Double.NaN );

		final DisjointSets dj = new DisjointSets( ( int ) Intervals.numElements( labels ) );

		final int[] strides = new int[ nDim ];
		strides[ 0 ] = 1;
		for ( int d = 1; d < strides.length; ++d )
			strides[ d ] = ( int ) ( strides[ d - 1 ] * labels.dimension( d - 1 ) );
		System.out.println( Arrays.toString( strides ) );

		{
			int idx = 0;
			for ( final Cursor< C > pc = Views.interval( affs, labels ).cursor(); pc.hasNext(); ++idx )
			{
				final C p = pc.next();
				for ( int d = 0; d < nDim; ++d )
					if ( p.get( d ).getRealDouble() > threshold )
						dj.join( dj.findRoot( idx ), dj.findRoot( idx + strides[ d ] ) );
//						System.out.println( "Joining " + idx + " " + ( idx + strides[ d ] ) + " " + d + " " + p.get( d ).getRealDouble() + " " + threshold );
			}
		}

		final TIntLongHashMap labelings = new TIntLongHashMap();

		long start = offset;
		int idx = 0;
		for ( final Cursor< LongType > lbc = Views.flatIterable( labels ).cursor(); lbc.hasNext(); ++idx )
		{
			final LongType lb = lbc.next();
			final int l = dj.findRoot( idx );
//			System.out.println( l + " " + idx );
			if ( !labelings.contains( l ) )
			{
				lb.set( start );
				labelings.put( l, start );
				++start;
			}
			else
				lb.set( labelings.get( l ) );
		}

		return labelings;

	}

	public static void main( final String[] args ) throws Exception
	{
		new ImageJ();

		PropertyConfigurator.configure( new File( "resources/log4j.properties" ).getAbsolutePath() );

//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt.h5";
		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";
		final CellImg< FloatType, ?, ? > input = H5Utils.loadFloat( path, "main", new int[] { 432, 432, 100, 3 } );
		final ArrayImg< FloatType, FloatArray > affs = ArrayImgs.floats( Intervals.dimensionsAsLongArray( input ) );

		final int nDim = affs.numDimensions() - 1;
		final double norm = 1.0 / nDim;
		final RandomAccessibleInterval< FloatType > avg = Converters.convert( Views.collapseReal( affs ), ( s, t ) -> {
			t.setZero();
			for ( int i = 0; i < nDim; ++i )
				t.set( Math.max( s.get( i ).get(), t.get() ) );
//			t.mul( norm );
		}, new FloatType() );

		final int[] perm = Util.getFlipPermutation( input.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > inputPerm = Views.permuteCoordinates( input, perm, input.numDimensions() - 1 );

		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( inputPerm, affs ), affs ) )
			p.getB().set( p.getA() );

		final Threshold rw = new Threshold( true, 0.99 );
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
