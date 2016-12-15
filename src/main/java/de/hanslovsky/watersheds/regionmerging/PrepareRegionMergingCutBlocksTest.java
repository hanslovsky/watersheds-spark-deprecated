package de.hanslovsky.watersheds.regionmerging;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.DatatypeConverter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
import bdv.util.BdvFunctions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.HashableLongArray;
import de.hanslovsky.watersheds.InitialWatershedBlock;
import de.hanslovsky.watersheds.NumElements;
import de.hanslovsky.watersheds.OffsetLabels;
import de.hanslovsky.watersheds.Util;
import de.hanslovsky.watersheds.ValueDisplayListener;
import de.hanslovsky.watersheds.graph.EdgeMerger;
import de.hanslovsky.watersheds.graph.Function;
import de.hanslovsky.watersheds.graph.IdServiceZMQ;
import de.hanslovsky.watersheds.graph.MergeBloc;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.io.AffinitiesChunkLoader;
import de.hanslovsky.watersheds.io.LabelsChunkWriter;
import de.hanslovsky.watersheds.io.ZMQFileOpenerFloatType;
import de.hanslovsky.watersheds.io.ZMQFileWriterLongType;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.type.numeric.ARGBType;
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

public class PrepareRegionMergingCutBlocksTest
{

	public static void main( final String[] args ) throws IOException
	{

		final int[] dimsInt = new int[] { 300, 300, 2 }; // dropbox
//		final int[] dimsInt = new int[] { 1554, 1670, 153, 3 }; // A
		final long[] dims = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ] };
		final long[] dimsNoChannels = new long[] { dimsInt[ 0 ], dimsInt[ 1 ] };
		final int[] dimsIntervalInt = new int[] { 30, 30, 2 };
		final long[] dimsInterval = new long[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ], dimsIntervalInt[ 2 ] };
		final int[] dimsIntervalIntNoChannels = new int[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ] };
		final long[] dimsIntervalNoChannels = new long[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ] };
		final int[] cellSize = new int[] { 300, 300, 2 };
		final long inputSize = Intervals.numElements( dims );



//		final String path = "/groups/saalfeld/home/funkej/workspace/projects/caffe/run/cremi/03_process_training/processed/setup26/100000/sample_A.augmented.0.hdf"; // A

		final String HOME_DIR = System.getProperty( "user.home" );
		final String path = HOME_DIR + String.format(
				"/Dropbox/misc/excerpt2D.h5" );

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data =
				H5Utils.loadFloat( path, "main", cellSize );
		System.out.println( "Loaded data (" + inputSize + ")" );

		final int[] cellDims = new int[ data.numDimensions() ];
		data.getCells().cellDimensions( cellDims );

		final int[] perm = new int[] { 1, 0 };
//		final CellImg< FloatType, ?, ? > input = new CellImgFactory< FloatType >( dimsIntervalInt ).create( dims, new FloatType() );
		final ArrayImg< FloatType, FloatArray > input = ArrayImgs.floats( dims );
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, 2 ), input ), input ) )
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
				final long[] dim = new long[ 3 ];
				final long[] offset = new long[ 3 ];
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
		for ( long y = 0; y < dims[ 1 ]; y += dimsIntervalInt[ 1 ] )
			for ( long x = 0; x < dims[ 0 ]; x += dimsIntervalInt[ 0 ] )
			{
				final HashableLongArray t = new HashableLongArray( x, y );
				lowerBounds.add( t );


				final long[] lower = new long[] { x, y, 0 };
				final long[] upper = new long[ lower.length ];
				final long[] dm = new long[ upper.length ];
				upper[ 2 ] = 2;
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

		final SparkConf conf = new SparkConf().setAppName( "Watersheds" ).setMaster( "local[1]" ).set( "spark.driver.maxResultSize", "4g" );
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
//				System.out.println( "RECEIVED MESSAGE OF SIZE " + message.length );
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
//				System.out.println( "Got min=" + m + ", max=" + M + " for " + Arrays.toString( min ) + " " + Arrays.toString( max ) + " has background: " + hasZero );
				labelsTargetListener.send( "" );
			}
		} );
		labelsThread.start();

		final JavaPairRDD< HashableLongArray, Tuple2< long[], TLongLongHashMap > > offsetLabelsWithCounts = ws
				.mapToPair( new OffsetLabels( startIndicesBC, dimsNoChannels ) )
				.cache();

		final TLongLongHashMap counts = new TLongLongHashMap();
//		offsetLabelsWithCounts.map( t -> {
////			System.out.println( t );
//			if ( t == null )
//				throw new RuntimeException( t + " is null!" );
//			else if ( t._1() == null )
//				throw new RuntimeException( "t._1() is null!" );
//			else if ( t._2() == null )
//				throw new RuntimeException( "t._2() is null!" );
//			return true;
//		} ).count();
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
		for ( long y = 0; y < dims[1]; y += dimsInterval[1] )
			for ( long x = 0; x < dims[0]; x += dimsInterval[0] ) {
				final long[] labelData = new long[ extendedBlockElements ];
				final Cursor< LongType > l = Views.offsetInterval( labelsExtend, new long[] { x - 1, y - 1 }, extendedBlockSize ).cursor();
				for ( int i = 0; l.hasNext(); ++i )
					labelData[i] = l.next().get();
				final float[] affsData = new float[ extendedBlockElementsAffs ];
				final Cursor< FloatType > a = Views.offsetInterval( Views.extendValue( input, new FloatType( Float.NaN ) ), new long[] { x - 1, y - 1, 0 }, extendedBlockSizeAffs ).cursor();
				for ( int i = 0; a.hasNext(); ++i )
					affsData[ i ] = a.next().get();

				blocks.add( new Tuple2<>( new HashableLongArray( x, y ), new Tuple3<>( labelData, affsData, counts ) ) );

			}

		final Context ctx = ZMQ.context( 1 );

		final String blockIdAddr = "ipc://blockIdService";
		final Socket blockIdSocket = IdServiceZMQ.createServerSocket( ctx, blockIdAddr );
		final Thread blockIdThread = IdServiceZMQ.createServerThread( blockIdSocket, new AtomicLong( 1 ) );
		blockIdThread.start();
		final IdServiceZMQ blockIdService = new IdServiceZMQ( blockIdAddr );

		final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksRdd =
				sc.parallelizePairs( blocks ).cache();
		final EdgeMerger merger = MergeBloc.DEFAULT_EDGE_MERGER;
		final Function weightFunc = ( Function & Serializable ) ( a, c1, c2 ) -> Math.min( c1, c2 ) / ( a * a );
		final Tuple2< JavaPairRDD< Long, In >, TLongLongHashMap > graphsAndBorderNodes =
				PrepareRegionMergingCutBlocks.run( sc, blocksRdd, sc.broadcast( dimsNoChannels ),
						sc.broadcast( dimsIntervalNoChannels ), merger, weightFunc, ( EdgeCheck & Serializable ) e -> e.affinity() > 0.5, blockIdService );
		final JavaPairRDD< Long, In > graphs = graphsAndBorderNodes._1().cache();
		System.out.println( "KEY TWO: " + graphs.filter( ( t ) -> t._1().longValue() == 2 ).values().collect().get( 0 ).counts );
		{
			final Random rng = new Random();
			final TLongIntHashMap colors = new TLongIntHashMap();
			for ( final Tuple2< Long, In > t : graphs.collect() )
				colors.put( t._1(), rng.nextInt() );
			final TLongLongHashMap nodeBlockMap = graphs.aggregate(
					new TLongLongHashMap(),
					( m, t ) -> {
						for ( final TLongIterator it = t._2().counts.keySet().iterator(); it.hasNext(); )
						{
							final long nxt = it.next();
							if ( !t._2().outsideNodes.contains( nxt ) )
								m.put( nxt, t._1() );
						}
						return m;
					},
					( m1, m2 ) -> {
						final TLongLongHashMap m = new TLongLongHashMap();
						m.putAll( m1 );
						m.putAll( m2 );
						return m;
					} );

			final RandomAccessibleInterval< LongType > labelsTargetRAI = labelsTarget;
			final RandomAccessibleInterval< LongType > blockLabels = Converters.convert( labelsTargetRAI, ( s, t ) -> {
				t.set( nodeBlockMap.get( s.get() ) );
			}, new LongType() );
			final RandomAccessibleInterval< ARGBType > coloredBlockLabels = Converters.convert( blockLabels, ( s, t ) -> {
				t.set( colors.get( s.get() ) );
			}, new ARGBType() );
			final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredBlockLabels, "INITIAL BLOCKS " + graphs.count() );

			ValueDisplayListener.addValueOverlay( Views.interpolate(
					Views.extendValue( Views.addDimension( blockLabels, 0, 0 ), new LongType( -1 ) ),
					new NearestNeighborInterpolatorFactory<>() ), bdv.getBdvHandle().getViewerPanel() );


		}

		final TLongHashSet relevant = new TLongHashSet( new long[] { 26 } );
		System.out.println( relevant );

		BdvFunctions.show( Converters.convert(
				( RandomAccessibleInterval< LongType > ) labelsTarget,
				( s, t ) -> {
					t.set( relevant.contains( s.get() ) ? 255 << 16 : 0 << 8 );
				},
				new ARGBType() ), "RELEVANT" );

		final TLongIntHashMap colors = new TLongIntHashMap();
		final Random rng = new Random( 100 );
		final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert(
				( RandomAccessibleInterval< LongType > ) labelsTarget,
				( s, t ) -> {
					if ( !colors.contains( s.get() ) )
						colors.put( s.get(), rng.nextInt() );
					t.set( colors.get( s.get() ) );
				},
				new ARGBType() );

		final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredLabels, "coloredLabels" );

		ValueDisplayListener.addValueOverlay( Views.interpolate( Views.extendValue( Views.addDimension( labelsTarget, 0, 0 ), new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ), bdv.getBdvHandle().getViewerPanel() );

		final JavaRDD< In > blockContaining5711 =
				graphs.filter( t -> t._2().counts.contains( 5711 ) && !t._2().outsideNodes.contains( 5711 ) ).values();
		final In data5711 = blockContaining5711.collect().get( 0 );

		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rg = new Random( 100 );
		final BdvStackSource< ARGBType > bv = BdvFunctions.show(
				Converters.convert(
						( RandomAccessibleInterval< LongType > ) labelsTarget,
						( s, t ) -> {
							final long sv = s.get();
							if ( data5711.counts.contains( sv ) )
							{
								if ( data5711.outsideNodes.contains( sv ) )
									t.set( 80 << 16 | 80 << 8 | 80 << 0 );
								else if ( data5711.borderNodes.contains( sv ) )
									t.set( 160 << 16 | 160 << 8 | 160 << 0 );
								else
								{
									if ( !cmap.contains( sv ) )
										cmap.put( sv, rg.nextInt() );
									t.set( cmap.get( sv ) );
								}
							}
							else if ( data5711.outsideNodes.contains( sv ) )
								t.set( 80 << 16 | 80 << 8 | 80 << 0 );
							else
								t.set( 0 );
						},
						new ARGBType() ),
				"b386 " + data5711.outsideNodes.size() + " " + data5711.borderNodes.size() );

		for ( final TLongObjectIterator< TLongHashSet > it = data5711.borderNodes.iterator(); it.hasNext(); )
		{
			it.advance();
			System.out.println( it.key() + " " + it.value() );
		}

		sc.close();

		ctx.close();



	}


}
