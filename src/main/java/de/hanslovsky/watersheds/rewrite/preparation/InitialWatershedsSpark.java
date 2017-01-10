package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import de.hanslovsky.watersheds.NumElements;
import de.hanslovsky.watersheds.rewrite.io.AffinitiesChunkLoader;
import de.hanslovsky.watersheds.rewrite.io.LabelsChunkWriter;
import de.hanslovsky.watersheds.rewrite.io.ZMQFileOpenerFloatType;
import de.hanslovsky.watersheds.rewrite.io.ZMQFileWriterLongType;
import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
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


		final int[] cellSize = new int[] { 300, 300, 100, 3 };
		final int[] cellSizeLabels = Util.dropLast( cellSize );
		final int[] dimsIntervalInt = new int[] { 300, 300, 100, 3 };
		final long[] dimsInterval = Arrays.stream( dimsIntervalInt ).mapToLong( i -> i ).toArray();
		final int[] dimsIntervalIntNoChannels = Util.dropLast( dimsIntervalInt );

		final String HOME_DIR = System.getProperty( "user.home" );
		final String path = HOME_DIR + String.format( "/Dropbox/misc/excerpt-no-blocks.h5" );

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data =
				H5Utils.loadFloat( path, "main", cellSize );

		final long[] dims = Intervals.dimensionsAsLongArray( data );
		final long inputSize = Intervals.numElements( data );

		System.out.println( "Loaded data (" + inputSize + ")" );


		final int[] cellDims = new int[ data.numDimensions() ];
		data.getCells().cellDimensions( cellDims );

		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
//		final CellImg< FloatType, ?, ? > input = new CellImgFactory< FloatType >( dimsIntervalInt ).create( dims, new FloatType() );
		final ArrayImg< FloatType, FloatArray > input = ArrayImgs.floats( dims );
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, data.numDimensions() - 1 ), input ), input ) )
			p.getB().set( p.getA().getRealFloat() );

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );
		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( affs );

		final Img< LongType > labelsTarget = new CellImgFactory< LongType >( dimsIntervalInt ).create( affs, new LongType() );

		final String addr = "ipc://data_server";
		final ZContext zmqContext = new ZContext();
		final Socket serverSocket = zmqContext.createSocket( ZMQ.REP );
		serverSocket.bind( addr );
		final Gson gson = new Gson();

		final Thread serverThread = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] message = serverSocket.recv( 0 );
				if ( message.length == 0 )
					continue;
				final JsonArray req = gson.fromJson( new String( message ), JsonArray.class );
				final long[] dim = new long[ dims.length ];
				final long[] offset = new long[ dims.length ];
				long reqSize = 1;
				for ( int d = 0; d < dim.length; ++d )
				{
					offset[ d ] = req.get( d ).getAsLong();
					dim[ d ] = req.get( d + dim.length ).getAsLong();
					reqSize *= dim[ d ];
				}

				final byte[] bytes = new byte[ Float.BYTES * ( int ) reqSize ];

				final ByteBuffer bb = ByteBuffer.wrap( bytes );

				for ( final FloatType v : Views.flatIterable( Views.offsetInterval( Views.extendValue( input, new FloatType( Float.NaN ) ), offset, dim ) ) )
					bb.putFloat( v.get() );

				serverSocket.send( bytes, 0 );
			}

		} );
		System.out.print( "Starting server thread!" );
		serverThread.start();
		System.out.println( "Started server thread!" );

		System.out.println( "Generating map" );

		final long[] offset = new long[ dimsNoChannels.length ];
		final ArrayList< HashableLongArray > lowerBounds = new ArrayList<>();
		for ( int d = 0; d < dimsNoChannels.length; )
		{
			System.out.println( Arrays.toString( offset ) );
			lowerBounds.add( new HashableLongArray( offset.clone() ) );
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

		final ZMQFileOpenerFloatType opener = new ZMQFileOpenerFloatType( addr );
		final JavaPairRDD< HashableLongArray, float[] > imgs =
				sc.parallelize( lowerBounds ).mapToPair( new AffinitiesChunkLoader( opener, dims, dimsIntervalInt ) ).cache();

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

		final String listenerAddr = "ipc://labels_listener";
		final Socket labelsTargetListener = zmqContext.createSocket( ZMQ.REP );
		labelsTargetListener.bind( listenerAddr );
		final Thread labelsThread = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] message = labelsTargetListener.recv( 0 );
				if ( message.length == 0 )
					continue;
				System.out.println( "RECEIVED MESSAGE OF SIZE " + message.length );
				final JsonObject json = gson.fromJson( new String( message ), JsonObject.class );
				final long[] min = new long[ labelsTarget.numDimensions() ];
				final long[] max = new long[ labelsTarget.numDimensions() ];
				final byte[] bytes = DatatypeConverter.parseBase64Binary( json.get( "data" ).getAsString() );// .getBytes();
				final ByteBuffer bb = ByteBuffer.wrap( bytes );
				final JsonArray minA = json.get( "min" ).getAsJsonArray();
				final JsonArray maxA = json.get( "max" ).getAsJsonArray();
				for ( int d = 0; d < min.length; ++d )
				{
					min[ d ] = minA.get( d ).getAsLong();
					max[ d ] = maxA.get( d ).getAsLong();
				}

				long m = Long.MAX_VALUE;
				long M = Long.MIN_VALUE;
				boolean hasZero = false;
				final long validBits = ~( 1l << 63 | 1l << 62 );
				for ( final LongType l : Views.flatIterable( Views.interval( labelsTarget, min, max ) ) )
				{
					final long n = bb.getLong() & validBits;
					l.set( n );
					if ( n == 0 )
						hasZero = true;
					else if ( n < m )
						m = n;
					else if ( n > M )
						M = n;
				}
				System.out.println( "Got min=" + m + ", max=" + M + " for " + Arrays.toString( min ) + " " + Arrays.toString( max ) + " has background: " + hasZero );
				labelsTargetListener.send( "" );
			}
		} );
		labelsThread.start();

		final JavaPairRDD< HashableLongArray, long[] > offsetLabels = ws
				.mapToPair( new OffsetLabels( startIndicesBC, dimsNoChannels ) )
				.mapToPair( new Util.DropSecondValue<>() )
				.cache();

		final List< Tuple2< HashableLongArray, long[] > > labelings = offsetLabels.collect();
		final TLongHashSet alreadyThere = new TLongHashSet();
		alreadyThere.addAll( labelings.get( 0 )._2() );
		System.out.println( alreadyThere.size() );
		System.out.println( labelings.get( 0 )._1() + "ock?" );
		for ( int i = 1; i < labelings.size(); ++i )
		{
			final TLongHashSet curr = new TLongHashSet();
			System.out.println( labelings.get( 1 )._1() + "ock?" );
			for ( final long l : labelings.get( i )._2() )
			{
				curr.add( l );
				if ( alreadyThere.contains( l ) )
					System.out.print( "EVIL!!! " + l + " " + labelings.get( i )._1() );
			}
			alreadyThere.addAll( curr );
			System.out.println( alreadyThere.size() );
		}

		final JavaPairRDD< HashableLongArray, Boolean > abc = offsetLabels.mapToPair( ( t ) -> {
			boolean doesNotContain4234 = true;
			final long[] arr = t._2();
			for ( int i = 0; i < arr.length && doesNotContain4234; ++i )
				if ( arr[ i ] == 4234 )
					doesNotContain4234 = false;
			return new Tuple2<>( t._1(), doesNotContain4234 );
		} );

		for ( final Tuple2< HashableLongArray, Boolean > ab : abc.collect() )
			System.out.println( "DOES IT CONTAIN IT? " + ab );

		offsetLabels.mapToPair( new LabelsChunkWriter(
				new ZMQFileWriterLongType( listenerAddr ),
				dimsNoChannels,
				dimsIntervalIntNoChannels ) ).count();

		{
			System.out.println( "Interrupting labels thread" );
			labelsThread.interrupt();
			final Socket closeSocket = zmqContext.createSocket( ZMQ.REQ );
			closeSocket.connect( listenerAddr );
			closeSocket.send( new byte[ 0 ], 0 );
			try
			{
				labelsThread.join();
			}
			catch ( final InterruptedException e )
			{
				e.printStackTrace();
			}
		}

		{
			System.out.println( "Interrupting server thread" );
			serverThread.interrupt();
			final Socket closeSocket = zmqContext.createSocket( ZMQ.REQ );
			closeSocket.connect( addr );
			closeSocket.send( new byte[ 0 ], 0 );
			try
			{
				serverThread.join();
			}
			catch ( final InterruptedException e )
			{
				e.printStackTrace();
			}
		}

		System.out.print( "Closing context" );
		zmqContext.close();

		System.out.println( "Closing spark" );
		sc.close();

		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );
		final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsTarget, ( s, t ) -> {
			if ( !cmap.contains( s.get() ) )
				cmap.put( s.get(), rng.nextInt() );
			t.set( cmap.get( s.get() ) );
		}, new ARGBType() );


		BdvFunctions.show( coloredLabels, "labels", coloredLabels.numDimensions() == 2 ? BdvOptions.options().is2D() : BdvOptions.options() );

		H5Utils.saveUnsignedLong( labelsTarget, path, "zws", cellSizeLabels );
	}

}
