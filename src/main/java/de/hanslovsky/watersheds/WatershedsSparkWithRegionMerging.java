package de.hanslovsky.watersheds;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import bdv.img.h5.H5Utils;
import de.hanslovsky.watersheds.graph.EdgeMerger;
import de.hanslovsky.watersheds.graph.Function;
import de.hanslovsky.watersheds.graph.IdServiceZMQ;
import de.hanslovsky.watersheds.graph.MergeBloc;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.MergerServiceZMQ;
import de.hanslovsky.watersheds.graph.MergerServiceZMQ.MergeActionAddToList;
import de.hanslovsky.watersheds.graph.MergerServiceZMQ.MergeActionParentMap;
import de.hanslovsky.watersheds.graph.RegionMerging;
import de.hanslovsky.watersheds.io.AffinitiesChunkLoader;
import de.hanslovsky.watersheds.io.LabelsChunkWriter;
import de.hanslovsky.watersheds.io.ZMQFileOpenerFloatType;
import de.hanslovsky.watersheds.io.ZMQFileWriterLongType;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class WatershedsSparkWithRegionMerging
{

	public static void main( final String[] args ) throws IOException
	{

		final int[] dimsInt = new int[] { 200, 200, 20, 3 }; // dropbox
//		final int[] dimsInt = new int[] { 1554, 1670, 153, 3 }; // A
		final long[] dims = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ], dimsInt[ 3 ] };
		final long[] dimsNoChannels = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ] };
		final int[] dimsIntervalInt = new int[] { 20, 20, 20, 3 };
		final long[] dimsInterval = new long[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ], dimsIntervalInt[ 2 ], dimsIntervalInt[ 3 ] };
		final int[] dimsIntervalIntNoChannels = new int[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ], dimsIntervalInt[ 2 ] };
		final long[] dimsIntervalNoChannels = new long[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ], dimsIntervalInt[ 2 ] };
		final int[] cellSize = new int[] { 200, 200, 20, 3 };
		final long inputSize = Intervals.numElements( dims );



//		final String path = "/groups/saalfeld/home/funkej/workspace/projects/caffe/run/cremi/03_process_training/processed/setup26/100000/sample_A.augmented.0.hdf"; // A

		final String HOME_DIR = System.getProperty( "user.home" );
		final String path = HOME_DIR + String.format(
				"/Dropbox/misc/excerpt-%dx%dx%d.h5", dims[ 2 ], dims[ 1 ], dims[ 0 ] ); // dropbox
		// example

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data =
				H5Utils.loadFloat( path, "main", cellSize );
		System.out.println( "Loaded data (" + inputSize + ")" );

		final int[] cellDims = new int[ data.numDimensions() ];
		data.getCells().cellDimensions( cellDims );

		final int[] perm = new int[] { 2, 1, 0 };
//		final CellImg< FloatType, ?, ? > input = new CellImgFactory< FloatType >( dimsIntervalInt ).create( dims, new FloatType() );
		final ArrayImg< FloatType, FloatArray > input = ArrayImgs.floats( dims );
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, 3 ), input ), input ) )
			p.getB().set( p.getA().getRealFloat() );

		for ( int i = 0; i < Views.collapseReal( input ).numDimensions(); ++i )
		{
			final IntervalView< RealComposite< FloatType > > hs = Views.hyperSlice( Views.collapseReal( input ), i, Views.collapse( input ).max( i ) );
			for ( final RealComposite< FloatType > c : hs )
				c.get( i ).setReal( Float.NaN );
		}

		final Img< LongType > labelsTarget = new CellImgFactory< LongType >( dimsIntervalInt ).create( Views.collapseReal( input ), new LongType() );

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
				final long[] dim = new long[ 4 ];
				final long[] offset = new long[ 4 ];
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

		final ArrayList< HashableLongArray > lowerBounds = new ArrayList<>();
		for ( long z = 0; z < dims[ 2 ]; z += dimsIntervalInt[ 2 ] )
			for ( long y = 0; y < dims[ 1 ]; y += dimsIntervalInt[ 1 ] )
				for ( long x = 0; x < dims[ 0 ]; x += dimsIntervalInt[ 0 ] )
				{
					final HashableLongArray t = new HashableLongArray( x, y, z );
					lowerBounds.add( t );


					final long[] lower = new long[] { x, y, z, 0 };
					final long[] upper = new long[ lower.length ];
					final long[] dm = new long[ upper.length ];
					upper[ 3 ] = 3;
					for ( int d = 0; d < upper.length; ++d )
					{
						upper[ d ] = Math.min( dims[ d ] - 1, lower[ d ] + dimsInterval[ d ] - 1 );
						dm[ d ] = upper[ d ] - lower[ d ] + 1;
					}

				}

		System.out.println( "Generated map" );


		final PairFunction< Tuple2< HashableLongArray, float[] >, HashableLongArray, Tuple2< long[], long[] > > func =
				new InitialWatershedBlock( dimsIntervalInt, dims, 0.0, ( a, b ) -> {} );
//						new ShowTopLeftVisitor() );

		final SparkConf conf = new SparkConf().setAppName( "Watersheds" ).setMaster( "local[*]" ).set( "spark.driver.maxResultSize", "4g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );


		final ZMQFileOpenerFloatType opener = new ZMQFileOpenerFloatType( addr );
		final JavaPairRDD< HashableLongArray, float[] > imgs =
				sc.parallelize( lowerBounds ).mapToPair( new AffinitiesChunkLoader( opener, dims, dimsIntervalInt ) ).cache();

		final JavaPairRDD< HashableLongArray, Tuple2< long[], long[] > > ws = imgs.mapToPair(
				func
				).cache();

		final List< Tuple2< HashableLongArray, Long > > labelingsAndCounts = ws
				.mapToPair( t -> new Tuple2<>( t._1(), t._2()._2() ) )
				.mapToPair( new NumElements<>() )
				.collect();

		final ArrayList< Tuple2< HashableLongArray, Long > > al = new ArrayList<>();
		for ( final Tuple2< HashableLongArray, Long > lac : labelingsAndCounts )
			al.add( lac );

		Collections.sort( al, new Util.KeyAndCountsComparator<>( dimsNoChannels ) );

		final TLongLongHashMap startIndices = new TLongLongHashMap( al.size(), 1.5f, -1, -1 );
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

		final JavaPairRDD< HashableLongArray, Tuple2< long[], TLongLongHashMap > > offsetLabelsWithCounts = ws
				.mapToPair( new OffsetLabels( startIndicesBC, dimsNoChannels ) )
				.cache();

		final TLongLongHashMap counts = new TLongLongHashMap();
		offsetLabelsWithCounts.map( t -> {
			System.out.println( t );
			if ( t == null )
				throw new RuntimeException( t + " is null!" );
			else if ( t._1() == null )
				throw new RuntimeException( "t._1() is null!" );
			else if ( t._2() == null )
				throw new RuntimeException( "t._2() is null!" );
			return true;
		} ).count();
		for ( final TLongLongHashMap m : offsetLabelsWithCounts.values().mapToPair( t -> t ).values().collect() )
			counts.putAll( m );

		final JavaPairRDD< HashableLongArray, long[] > offsetLabels = offsetLabelsWithCounts.mapToPair( new Util.DropSecondValue<>() );

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

		final ExtendedRandomAccessibleInterval< LongType, Img< LongType > > labelsExtend = Views.extendValue( labelsTarget, new LongType( -1 ) );

		final long[] extendedBlockSize = new long[ dimsInterval.length ];
		final long[] extendedBlockSizeAffs = new long[ dimsInterval.length + 1 ];
		for ( int i = 0; i < extendedBlockSize.length; ++i ) {
			final long v = dimsInterval[i] + 2;
			extendedBlockSize[i] = v;
			extendedBlockSizeAffs[i] = v;
		}
		extendedBlockSizeAffs[extendedBlockSize.length ] = extendedBlockSize.length;

		final int extendedBlockElements = ( int ) Intervals.numElements( extendedBlockSize );
		final int extendedBlockElementsAffs = (int) Intervals.numElements( extendedBlockSizeAffs );

		final ArrayList< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > > blocks = new ArrayList<>();
		for ( long z = 0; z < dims[ 2 ]; z += dimsInterval[ 2 ] )
			for ( long y = 0; y < dims[1]; y += dimsInterval[1] )
				for ( long x = 0; x < dims[0]; x += dimsInterval[0] ) {
					final long[] labelData = new long[ extendedBlockElements ];
					final Cursor< LongType > l = Views.offsetInterval( labelsExtend, new long[] { x - 1, y - 1, z - 1 }, extendedBlockSize ).cursor();
					for ( int i = 0; l.hasNext(); ++i )
						labelData[i] = l.next().get();
					final float[] affsData = new float[ extendedBlockElementsAffs ];
					final Cursor< FloatType > a = Views.offsetInterval( Views.extendValue( input, new FloatType( Float.NaN ) ), new long[] { x - 1, y - 1, z - 1, 0 }, extendedBlockSizeAffs ).cursor();
					for ( int i = 0; a.hasNext(); ++i )
						affsData[ i ] = a.next().get();

					blocks.add( new Tuple2<>( new HashableLongArray( new long[] { x, y, z } ), new Tuple3<>( labelData, affsData, counts ) ) );

				}

		final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksRdd =
				sc.parallelizePairs( blocks ).cache();
		final EdgeMerger merger = MergeBloc.DEFAULT_EDGE_MERGER;
		final Function weightFunc = ( Function & Serializable ) ( a, c1, c2 ) -> Math.min( c1, c2 ) / ( a * a );
		final JavaPairRDD< Long, In > graphs =
				blocksRdd.mapToPair( new PrepareRegionMerging.BuildBlockedGraph( dimsNoChannels, dimsIntervalNoChannels, merger, weightFunc ) ).cache();

//		System.out.println( "GRAPHS " + graphs.collect().get( 0 )._2().counts );

		final String idAddr = "ipc://idService";
		final String mergerAddr = "ipc://mergerService";
		final Context ctx = ZMQ.context( 1 );
		final Socket idSocket = IdServiceZMQ.createServerSocket( ctx, idAddr );
		final Socket mergerSocket = MergerServiceZMQ.createServerSocket( ctx, mergerAddr );
		final Thread idThread = IdServiceZMQ.createServerThread( idSocket, new AtomicLong( 123 ) );
		idThread.start();
		final TLongArrayList merges = new TLongArrayList();
		final TLongLongHashMap mergedParents = new TLongLongHashMap();
		final MergeActionAddToList action1 = new MergerServiceZMQ.MergeActionAddToList( merges );
		final MergeActionParentMap action2 = new MergerServiceZMQ.MergeActionParentMap( mergedParents );
		final Thread mergerThread = MergerServiceZMQ.createServerThread( mergerSocket, ( n1, n2, n, w ) -> {
			action1.add( n1, n2, n, w );
			action2.add( n1, n2, n, w );
		} );
		mergerThread.start();
		final IdServiceZMQ idService = new IdServiceZMQ( idAddr );
		final MergerServiceZMQ mergerService = new MergerServiceZMQ( mergerAddr );

		final RegionMerging rm = new RegionMerging( weightFunc, merger, idService, mergerService );

		final JavaPairRDD< Long, In > graphsAfterMerging = rm.run( sc, graphs, 5.0 );

		graphsAfterMerging.collect();

		System.out.println( mergedParents );

		sc.close();

		ctx.close();


	}


}
