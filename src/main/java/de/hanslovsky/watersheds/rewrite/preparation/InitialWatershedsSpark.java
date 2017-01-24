package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.NumElements;
import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.IntensityMouseOver;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class InitialWatershedsSpark
{

	public static void main( final String[] args ) throws IOException
	{


		final int[] cellSize = new int[] { 100, 100, 100, 3 };
		final int[] cellSizeLabels = Util.dropLast( cellSize );
		final int[] dimsIntervalInt = new int[] { 432, 432, 432, 3 };
		final long[] dimsInterval = Arrays.stream( dimsIntervalInt ).mapToLong( i -> i ).toArray();
		final int[] dimsIntervalIntNoChannels = Util.dropLast( dimsIntervalInt );

//		final String path = HOME_DIR + String.format( "/Dropbox/misc/excerpt.h5" );
//		final String path = HOME_DIR + "/Dropbox/misc/sample_A.augmented.0-500x500x50+500+500+50.hdf";
//		final String path = Util.HOME_DIR + "/tstvol-520-1-h5/groundtruth_aff-200x200x50.h5";
		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data =
				H5Utils.loadFloat( path, "main", cellSize );

		final long[] dims = Intervals.dimensionsAsLongArray( data );
		final long inputSize = Intervals.numElements( data );

		System.out.println( "Loaded data (" + inputSize + ")" );


		final int[] cellDims = new int[ data.numDimensions() ];
		data.getCells().cellDimensions( cellDims );

		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
		final ArrayImg< FloatType, FloatArray > input = ArrayImgs.floats( dims );
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, data.numDimensions() - 1 ), input ), input ) )
		{
			final float f = p.getA().getRealFloat();
			// threshold affinities
			p.getB().set( f < 0.001 ? 0.0f : f > 0.999 ? 1.0f : f );
		}

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );
		for ( int d = 0; d < affs.numDimensions(); ++d )
			for ( final RealComposite< FloatType > a : Views.hyperSlice( affs, d, affs.max( d ) ) )
				a.get( d ).set( Float.NaN );

		final BdvStackSource< FloatType > bdv = BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			t.set( s.get( 0 ).get() * ( 1 << 16 ) );
		}, new FloatType() ), "affs" );
		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			t.set( s.get( 1 ).get() * ( 1 << 16 ) );
		}, new FloatType() ), "affs", BdvOptions.options().addTo( bdv ) );

		new IntensityMouseOver( bdv.getBdvHandle().getViewerPanel() );

		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( affs );



		System.out.println( "Generating map" );

		final long[] offset = new long[ dimsNoChannels.length ];
		final ArrayList< HashableLongArray > lowerBounds = new ArrayList<>();
		final ArrayList< Tuple2< HashableLongArray, float[] > > blocks = new ArrayList<>();
		for ( int d = 0; d < dimsNoChannels.length; )
		{
			System.out.println( Arrays.toString( offset ) );
			lowerBounds.add( new HashableLongArray( offset.clone() ) );
			final long[] currentDims = new long[ offset.length + 1 ];
			for ( int k = 0; k < offset.length; ++k )
				currentDims[k] = Math.min( offset[k] + dimsInterval[k], dims[k] ) - offset[k];
			currentDims[ offset.length ] = offset.length;
			final float[] blockData = new float[ ( int ) Intervals.numElements( currentDims ) ];
			final ArrayImg< FloatType, FloatArray > blockImg = ArrayImgs.floats( blockData, currentDims );

			System.out.println( Arrays.toString( currentDims ) + " " + blockData.length + " " + Arrays.toString( offset ) + " " + Arrays.toString( Intervals.dimensionsAsLongArray( input ) ) );
			for ( final Pair< FloatType, FloatType > p : Views.flatIterable( Views.interval( Views.pair( Views.offsetInterval(
					Views.extendValue( input, new FloatType( Float.NaN ) ), Util.append( offset, 0 ), currentDims ), blockImg ), blockImg ) ) )
				p.getB().set( p.getA() );

			for ( int k = 0; k < Views.collapseReal( blockImg ).numDimensions(); ++k )
				for ( final RealComposite< FloatType > l : Views.hyperSlice( Views.collapseReal( blockImg ), k, Views.collapseReal( blockImg ).max( k ) ) )
					l.get( k ).set( Float.NaN );

			blocks.add( new Tuple2<>( new HashableLongArray( offset.clone() ), blockData ) );


			for ( d = 0; d < dimsNoChannels.length; ++d )
			{
				offset[ d ] += dimsIntervalIntNoChannels[ d ];
				if ( offset[ d ] < dimsNoChannels[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}

		System.out.println( "Generated map" );

		final PairFunction< Tuple2< HashableLongArray, float[] >, HashableLongArray, Tuple2< long[], long[] > > func =
				new InitialWatershedBlock( dimsIntervalInt, dims, 0.0, ( a, b ) -> {} );// new
		// ShowTopLeftVisitor()
		// );

		final SparkConf conf = new SparkConf()
				.setAppName( "Watersheds" )
				.setMaster( "local[*]" )
				.set( "spark.driver.maxResultSize", "4g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );

		final JavaPairRDD< HashableLongArray, float[] > imgs = sc.parallelizePairs( blocks );

		final JavaPairRDD< HashableLongArray, Tuple2< long[], long[] > > ws = imgs.mapToPair(
				func ).cache();

		final List< Tuple2< HashableLongArray, Long > > labelingsAndCounts = ws
				.mapToPair( t -> new Tuple2<>( t._1(), t._2()._2() ) )
				.mapToPair( new NumElements<>() )
				.collect();

		final ArrayList< Tuple2< HashableLongArray, Long > > al = new ArrayList<>();
		for ( final Tuple2< HashableLongArray, Long > lac : labelingsAndCounts )
			al.add( lac );

		Collections.sort( al, new Util.KeyAndCountsComparator<>( dimsNoChannels ) );

		final TLongLongHashMap startIndices = new TLongLongHashMap( 0, 1.0f, -1, -1 );
		long startIndex = 0;
		for ( final Tuple2< HashableLongArray, Long > t : al )
		{
			final HashableLongArray t1 = t._1();
			final long[] arr1 = t1.getData();// new long[] { t1._1(), t1._2(),
			// t1._3() };
			startIndices.put( IntervalIndexer.positionToIndex( arr1, dimsNoChannels ), startIndex );
			// "real" labels start at 1, so no id conflicts
			final long c = t._2() - 1l;
			startIndex += c; // don't count background
		} ;
		final long numRegions = startIndex;

		final Broadcast< TLongLongHashMap > startIndicesBC = sc.broadcast( startIndices );

		System.out.println( "Start indices: " + startIndices );

		System.out.println( "Collected " + labelingsAndCounts.size() + " labelings." );


		final JavaPairRDD< HashableLongArray, long[] > offsetLabels = ws
				.mapToPair( new OffsetLabels( startIndicesBC, dimsNoChannels ) )
				.mapToPair( new Util.DropSecondValue<>() )
				.cache();

		final List< Tuple2< HashableLongArray, long[] > > labelings = offsetLabels.collect();

		final Img< LongType > labelsTarget = new CellImgFactory< LongType >( dimsIntervalInt ).create( affs, new LongType() );
		for ( final Tuple2< HashableLongArray, long[] > l : labelings )
		{
			final long[] sz = new long[ l._1().getData().length ];
			for ( int d = 0; d < sz.length; ++d )
				sz[d] = Math.min( l._1().getData()[d] + dimsInterval[d], dims[d] ) - l._1().getData()[d];
			final ArrayImg< LongType, LongArray > img = ArrayImgs.longs( l._2(), sz );

			for ( final Pair< LongType, LongType > p : Views.flatIterable( Views.interval( Views.pair( Views.offsetInterval( Views.extendValue( labelsTarget, new LongType() ), l._1().getData(), sz ), img ), img ) ) )
				p.getA().set( p.getB() );
		}


		System.out.print( "Closing context" );

		System.out.println( "Closing spark" );
		sc.close();

		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );
		final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsTarget, ( s, t ) -> {
			if ( !cmap.contains( s.get() ) )
				cmap.put( s.get(), rng.nextInt() );
			t.set( cmap.get( s.get() ) );
		}, new ARGBType() );

		System.out.println( Arrays.toString( Intervals.dimensionsAsLongArray( labelsTarget ) ) );
		System.out.println( "Saving to h5..." );
		H5Utils.saveUnsignedLong( labelsTarget, path, "zws", Intervals.dimensionsAsIntArray( labelsTarget ) );
		System.out.println( "Done saving to h5..." );


		BdvFunctions.show( coloredLabels, "labels", coloredLabels.numDimensions() == 2 ? BdvOptions.options().is2D() : BdvOptions.options() );
	}

}
