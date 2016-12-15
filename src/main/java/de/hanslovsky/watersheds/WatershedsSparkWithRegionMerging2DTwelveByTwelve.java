package de.hanslovsky.watersheds;

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
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.graph.Edge;
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
import de.hanslovsky.watersheds.regionmerging.EdgeCheck;
import de.hanslovsky.watersheds.regionmerging.PrepareRegionMergingCutBlocks;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
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

public class WatershedsSparkWithRegionMerging2DTwelveByTwelve
{

	public static void main( final String[] args ) throws IOException
	{

		final int[] dimsInt = new int[] { 12, 12, 2 }; // dropbox
//		final int[] dimsInt = new int[] { 1554, 1670, 153, 3 }; // A
		final long[] dims = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ] };
		final long[] dimsNoChannels = new long[] { dimsInt[ 0 ], dimsInt[ 1 ] };
		final int[] dimsIntervalInt = new int[] { 6, 6, 2 };
		final long[] dimsInterval = new long[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ], dimsIntervalInt[ 2 ] };
		final int[] dimsIntervalIntNoChannels = new int[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ] };
		final long[] dimsIntervalNoChannels = new long[] { dimsIntervalInt[ 0 ], dimsIntervalInt[ 1 ] };
		final int[] cellSize = new int[] { 12, 12, 2 };
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
				labelsTargetListener.send( "" );
			}
		} );
		labelsThread.start();

		final JavaPairRDD< HashableLongArray, Tuple2< long[], TLongLongHashMap > > offsetLabelsWithCounts = ws
				.mapToPair( new OffsetLabels( startIndicesBC, dimsNoChannels ) )
				.cache();

		final TLongLongHashMap counts = new TLongLongHashMap();
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
		final Tuple2< JavaPairRDD< Long, In >, TLongLongHashMap > graphsAndBorderNodes = PrepareRegionMergingCutBlocks.run( sc, blocksRdd, sc.broadcast( dimsNoChannels ),
				sc.broadcast( dimsIntervalNoChannels ), merger, weightFunc, ( EdgeCheck & Serializable ) e -> e.affinity() > 0.1, blockIdService );
		final JavaPairRDD< Long, In > graphs = graphsAndBorderNodes._1();
		{
			final Random rng = new Random();
			final TLongIntHashMap colors = new TLongIntHashMap();
			for ( final Long k : graphs.keys().collect() )
				colors.put( k, rng.nextInt() );
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
			BdvFunctions.show( Converters.convert( labelsTargetRAI, ( s, t ) -> {
				t.set( colors.get( nodeBlockMap.get( s.get() ) ) );
			}, new ARGBType() ), "INITIAL BLOCKS " + graphs.count() + " " + graphs.collect() );

		}


		final String idAddr = "ipc://idService";
		final String mergerAddr = "ipc://mergerService";
		final Socket idSocket = IdServiceZMQ.createServerSocket( ctx, idAddr );
		final Socket mergerSocket = MergerServiceZMQ.createServerSocket( ctx, mergerAddr );
		final Thread idThread = IdServiceZMQ.createServerThread( idSocket, new AtomicLong( 300 * 300 * 10 ) );
		idThread.start();
		final TLongArrayList merges = new TLongArrayList();
		final TLongLongHashMap mergedParents = new TLongLongHashMap();
		final MergeActionAddToList action1 = new MergerServiceZMQ.MergeActionAddToList( merges );
		final MergeActionParentMap action2 = new MergerServiceZMQ.MergeActionParentMap( mergedParents );

		final TLongIntHashMap colorMap = new TLongIntHashMap();
		final Random rngCm = new Random( 100 );
		for ( final LongType l : labelsTarget )
			if ( !colorMap.contains( l.get() ) )
				colorMap.put( l.get(), rngCm.nextInt() );

		final Thread mergerThread = MergerServiceZMQ.createServerThread( mergerSocket, ( n1, n2, n, w ) -> {
			action1.add( n1, n2, n, w );
			action2.add( n1, n2, n, w );
			colorMap.put( n, colorMap.get( n1 ) );
		} );
		mergerThread.start();
		final IdServiceZMQ idService = new IdServiceZMQ( idAddr );
		final MergerServiceZMQ mergerService = new MergerServiceZMQ( mergerAddr );

		final RegionMerging rm = new RegionMerging( weightFunc, merger, idService, mergerService );

		final ArrayList< RandomAccessibleInterval< LongType > > blockImages = new ArrayList<>();
		final Img< LongType > blockZero = labelsTarget.factory().create( labelsTarget, new LongType() );
		final TLongLongHashMap labelBlockmap = new TLongLongHashMap();
		final List< Long > blockIds = graphs.keys().collect();
		for ( final Tuple2< Long, In > g : graphs.collect() )
		{
			final long id = g._1();
			final TLongObjectHashMap< TLongHashSet > cbns = g._2().borderNodes;
			final Edge e = new Edge( g._2().g.edges() );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long f = e.from();
				final long t = e.to();
				if ( cbns.contains( f ) )
					labelBlockmap.put( f, id );
				else if ( cbns.contains( t ) )
					labelBlockmap.put( t, id );
				else
				{
					labelBlockmap.put( t, id );
					labelBlockmap.put( f, id );
				}
			}
		}
		for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( labelsTarget, blockZero ), blockZero ) )
			p.getB().set( labelBlockmap.get( p.getA().get() ) );
		blockImages.add( blockZero );
		final TLongIntHashMap blockColors = new TLongIntHashMap();
		final Random blockRng = new Random( 100 );
		for ( final Long b : blockIds )
			blockColors.put( b.longValue(), blockRng.nextInt() );

		final ArrayList< RandomAccessibleInterval< LongType > > images = new ArrayList<>();
		images.add( labelsTarget );
		final RegionMerging.Visitor rmVisitor = ( parents ) -> {
			final Img< LongType > img = labelsTarget.factory().create( images.get( 0 ), new LongType() );
			for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( images.get( images.size() - 1 ), img ), img ) )
				p.getB().set( mergedParents.contains( p.getA().get() ) ? mergedParents.get( p.getA().get() ) : p.getA().get() );
			images.add( img );

			final Img< LongType > blockImg = labelsTarget.factory().create( blockImages.get( 0 ), new LongType() );
			for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( blockImages.get( blockImages.size() - 1 ), blockImg ), blockImg ) )
				p.getB().set( parents.get( p.getA().get() ) );
			blockImages.add( blockImg );
		};
		final JavaPairRDD< Long, In > graphsAfterMerging = rm.run( sc, graphs, 50000.0, rmVisitor, labelsTarget );

		final List< Tuple2< Long, In > > gs = graphsAfterMerging.collect();

		final RandomAccessibleInterval< ARGBType > coloredHistory = Converters.convert( Views.stack( images ), ( s, t ) -> {
			t.set( colorMap.get( s.get() ) );
		}, new ARGBType() );
		BdvFunctions.show( coloredHistory, "colored history" );
		System.out.println( "COLORED HISTORY SIZE " + images.size() + " COLORED BLOCK HISTORY SIZE " + blockImages.size() );

		final RandomAccessibleInterval< ARGBType > coloredBlockHistory =
				Converters.convert( Views.stack( blockImages ), ( s, t ) -> {
					t.set( blockColors.get( s.get() ) );
				}, new ARGBType() );
		BdvFunctions.show( coloredBlockHistory, "colored block history" );

		for ( final TLongIterator kIt = mergedParents.keySet().iterator(); kIt.hasNext(); )
			MergeBloc.findRoot( mergedParents, kIt.next() );

		final TLongIntHashMap colors = new TLongIntHashMap();
		final Random rng = new Random( 100 );
		for ( final TLongLongIterator it = mergedParents.iterator(); it.hasNext(); )
		{
			it.advance();
			final long r = it.value();
			if ( colors.contains( r ) )
			{

			}
			else
				colors.put( r, rng.nextInt() );
		}

		final TLongIntHashMap colorsMap = new TLongIntHashMap();
		for ( final TLongLongIterator it = mergedParents.iterator(); it.hasNext(); )
		{
			it.advance();
			colorsMap.put( it.key(), colors.get( it.value() ) );
		}

		final RandomAccessibleInterval< LongType > rooted = Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsTarget, ( s, t ) -> {
			t.set( mergedParents.contains( s.get() ) ? mergedParents.get( s.get() ) : s.get() );
		}, new LongType() );

		for ( final TLongIntIterator it = colorsMap.iterator(); it.hasNext(); )
		{
			it.advance();
			System.out.println( "cmap: " + it.key() + " " + it.value() );
		}

		final RandomAccessibleInterval< ARGBType > colored = Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsTarget, ( s, t ) -> {
			t.set( colorsMap.get( s.get() ) );
		}, new ARGBType() );

		final BdvStackSource< ARGBType > bdv = BdvFunctions.show( colored, "colored" );
		BdvFunctions.show( Converters.convert(
				( RandomAccessibleInterval< RealComposite< FloatType > > ) Views.collapseReal( input ),
				( s, t ) -> {
					t.set( ( int ) ( 255 * s.get( 0 ).get() ) << 8 | ( int ) ( 255 * s.get( 1 ).get() ) );
//					t.setReal( Math.max( s.get( 0 ).get(), s.get( 1 ).get() ) );
				}, new ARGBType() ),
				"aff max projection", BdvOptions.options().addTo( bdv ) );


		ValueDisplayListener.addValueOverlay(
				Views.interpolate( Views.extendValue( Views.addDimension( rooted, 0, 0 ), new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ),
				bdv.getBdvHandle().getViewerPanel() );

//		final RealComposite< FloatType > extension = Views.collapseReal( ArrayImgs.floats( new float[] { Float.NaN, Float.NaN }, 1, 2 ) ).randomAccess().get();
//		ValueDisplayListener.addValueOverlay(
//				Views.interpolate( Views.extendValue( Views.addDimension( Views.collapseReal( input ), 0, 0 ), extension ), new NearestNeighborInterpolatorFactory<>() ),
//				bdv.getBdvHandle().getViewerPanel(),
//				t -> "(" + t.get( 0 ) + "," + t.get( 1 ) + ")" );

		System.out.println( mergedParents );

		sc.close();

		ctx.close();


	}


}
