package de.hanslovsky.watersheds;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.mastodon.collection.ref.RefArrayList;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvStackSource;
import bdv.viewer.ViewerPanel;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershed2;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.Predicate;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.WeightedEdge;
import net.imglib2.algorithm.morphology.watershed.CompareBetter;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.algorithm.morphology.watershed.affinity.AffinityView;
import net.imglib2.algorithm.morphology.watershed.affinity.CompositeFactory;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.ui.OverlayRenderer;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class WatershedsSpark
{

	public static interface FileOpener extends Serializable
	{
		public float[] open( long[] offset, long[] dims );
	}

	public static void main( final String[] args ) throws IOException
	{

		final int[] dimsInt = new int[] { 200, 200, 20, 3 }; // dropbox
//		final int[] dimsInt = new int[] { 1554, 1670, 153, 3 }; // A
		final long[] dims = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ], dimsInt[ 3 ] };
		final long[] dimsNoChannels = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ] };
		final int[] dimsIntvInt = new int[] { 200, 200, 20, 3 };
		final long[] dimsIntv = new long[] { dimsIntvInt[ 0 ], dimsIntvInt[ 1 ], dimsIntvInt[ 2 ], dimsIntvInt[ 3 ] };
		final int[] cellSize = new int[] { 200, 200, 20, 3 };
		final long size = dimsIntv[ 3 ] * dimsIntv[ 2 ] * dimsIntv[ 1 ] * dimsIntv[ 0 ];
		final long inputSize = dims[ 0 ] * dims[ 1 ] * dims[ 2 ] * dims[ 3 ];



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
//		final double[] inputArr = new double[ ( int ) ( dims[ 0 ] * dims[ 1 ] * dims[ 2 ] * dims[ 3 ] ) ];
		final CellImg< FloatType, ?, ? > input = new CellImgFactory< FloatType >( dimsIntvInt ).create( dims, new FloatType() );
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, 3 ), input ), input ) )
			p.getB().set( p.getA().getRealFloat() );

		for ( int i = 0; i < Views.collapseReal( input ).numDimensions(); ++i )
		{
			final IntervalView< RealComposite< FloatType > > hs = Views.hyperSlice( Views.collapseReal( input ), i, Views.collapse( input ).max( i ) );
			for ( final RealComposite< FloatType > c : hs )
				c.get( i ).setReal( Float.NaN );
		}

		final Img< LongType > labelsTarget = new CellImgFactory< LongType >( dimsIntvInt ).create( Views.collapseReal( input ), new LongType() );

		final double threshold = 0;// 1e-20;


//		final Cells< ?, ? > cells = input.getCells();
//		int n = 0;
//		for ( final Cursor< ? > c = cells.cursor(); c.hasNext(); ++n )
//		{
//			final Object cell = c.next();
//			System.out.println( new Point( c ) );
//		}
//		System.out.println( n + " cells" );
//		System.exit( 123 );

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

		final ArrayList< Tuple3< Long, Long, Long > > lowerBounds = new ArrayList<>();
		for ( long z = 0; z < dims[ 2 ]; z += dimsIntvInt[ 2 ] )
			for ( long y = 0; y < dims[ 1 ]; y += dimsIntvInt[ 1 ] )
				for ( long x = 0; x < dims[ 0 ]; x += dimsIntvInt[ 0 ] )
				{
					final Tuple3< Long, Long, Long > t = new Tuple3<>( x, y, z );
					lowerBounds.add( t );


					final long[] lower = new long[] { x, y, z, 0 };
					final long[] upper = new long[ lower.length ];
					final long[] dm = new long[ upper.length ];
					upper[ 3 ] = 2;
					for ( int d = 0; d < upper.length; ++d )
					{
						upper[ d ] = Math.min( dims[ d ] - 1, lower[ d ] + dimsIntv[ d ] - 1 );
						dm[ d ] = upper[ d ] - lower[ d ] + 1;
					}

				}

		System.out.println( "Generated map" );



		final FileOpener opener = ( o, d ) -> {
			final ZContext zmqContext2 = new ZContext();
			final Socket requester = zmqContext2.createSocket( ZMQ.REQ );
			requester.connect( addr );
			final long[] req = new long[ o.length + d.length ];
			System.arraycopy( o, 0, req, 0, o.length );
			System.arraycopy( d, 0, req, o.length, d.length );
			final Gson gson2 = new Gson();
			System.out.print( "Sending request " + gson2.toJson( req ).toString() );
			requester.send( gson2.toJson( req ).toString(), 0 );
			final byte[] response = requester.recv();
			System.out.println( "Response of size " + response.length + " " + response.length / 8 + " " + size );
			final ByteBuffer bb = ByteBuffer.wrap( response );
			final float[] result = new float[ ( int ) size ];
			for ( int i1 = 0; i1 < result.length; ++i1 )
				result[ i1 ] = bb.getFloat();
			final ArrayImg< FloatType, FloatArray > img = ArrayImgs.floats( result, d );
//
			for ( int i2 = 0; i2 < 3; ++i2 )
			{
				final IntervalView< RealComposite< FloatType > > hs = Views.hyperSlice( Views.collapseReal( img ), i2, img.max( i2 ) );
				for ( final RealComposite< FloatType > comp : hs )
					comp.get( i2 ).set( Float.NaN );
			}

			zmqContext2.close();
			return result;
		};

		final double[] thresholds = { 5e3 };// , 1e5, 1e6 };

		final AtomicInteger doneCount = new AtomicInteger( 0 );

		final PairFunction< Tuple2< Tuple3< Long, Long, Long >, float[] >, Tuple3< Long, Long, Long >, Tuple2< long[], long[] > > func = t -> {
			final ArrayImg< FloatType, FloatArray > affs = ArrayImgs.floats( t._2(), dimsIntv );
			final long[] labelsArr = new long[ dimsIntvInt[ 2 ] * dimsIntvInt[ 1 ] * dimsIntvInt[ 0 ] ];
			final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( labelsArr, dimsIntvInt[ 0 ], dimsIntvInt[ 1 ], dimsIntvInt[ 2 ] );

			final RealComposite< FloatType > extension = Views.collapseReal(
					ArrayImgs.floats( new float[] { Float.NaN, Float.NaN, Float.NaN }, 1, 3 ) ).randomAccess().get();

			final CompositeIntervalView< FloatType, RealComposite< FloatType > > affsCollapsed = Views.collapseReal( affs );

			final CompositeFactory< FloatType, RealComposite< FloatType > > compFac = sz -> Views.collapseReal( ArrayImgs.floats( 1, sz ) ).randomAccess().get();

			final AffinityView< FloatType, RealComposite< FloatType > > affsView =
					new AffinityView<>( Views.extendValue( affsCollapsed, extension ), compFac );

			final ArrayImg< FloatType, FloatArray > affsCopy = ArrayImgs.floats( dimsIntvInt[ 0 ], dimsIntvInt[ 1 ], dimsIntvInt[ 2 ], dimsIntvInt[ 3 ] * 2 );

			for ( final Pair< RealComposite< FloatType >, RealComposite< FloatType > > p : Views.interval( Views.pair( affsView, Views.collapseReal( affsCopy ) ), labels ) )
				p.getB().set( p.getA() );

			final CompareBetter< FloatType > compare = ( f, s ) -> f.get() > s.get();

			final FloatType worstValue = new FloatType( -Float.MAX_VALUE );

			System.out.println( "Letting it rain with sizes " + Arrays.toString( Intervals.dimensionsAsLongArray( labels ) ) );

			final long[] counts = AffinityWatershedBlocked.letItRain(
					Views.collapseReal( affsCopy ),
					labels,
					compare,
					worstValue,
					Executors.newFixedThreadPool( 1 ),
					1,
					() -> {} );

//			final long[] counts = AffinityWatershed2.letItRain(
//					Views.collapseReal( affsCopy ),
//					labels,
//					compare,
//					worstValue,
//					Executors.newFixedThreadPool( 1 ),
//					1,
//					() -> {} );

			System.out.println( "Done with letting it rain: " + doneCount.incrementAndGet() );

			if ( t._1()._1() == 0 && t._1()._2() == 0 && t._1()._3() == 0 )
			{
				final TLongIntHashMap colors = new TLongIntHashMap();
				final Random rng = new Random( 100 );
				colors.put( 0, 0 );
				for ( final LongType l : labels )
				{
					final long lb = l.get();
					if ( !colors.contains( lb ) )
						colors.put( lb, rng.nextInt() );
				}

				final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert(
						( RandomAccessibleInterval< LongType > ) labels,
						( Converter< LongType, ARGBType > ) ( s, tt ) -> tt.set( colors.get( s.getInteger() ) ),
						new ARGBType() );
//				BdvFunctions.show( labels, "LABELS TOP LEFT " + threshold );
				BdvFunctions.show( coloredLabels, "LABELS TOP LEFT " + threshold );
			}

			if ( threshold < 1e-4 )
			{
				System.out.println( "PRE_MERGING NOW!!!" );
				return new Tuple2<>( t._1(), new Tuple2<>( labelsArr, counts ) );
			}
			else
			{
				final TLongDoubleHashMap rg = AffinityWatershedBlocked.generateRegionGraph(
						Views.collapseReal( affsCopy ),
						labels,
						AffinityWatershedBlocked.generateSteps( AffinityWatershedBlocked.generateStride( labels ) ),
						compare, worstValue,
						1 << 63,
						1 << 62,
						counts.length );
				final RefArrayList< WeightedEdge > edgeList = AffinityWatershedBlocked.graphToList( rg, counts.length );
				Collections.sort( edgeList, Collections.reverseOrder() );
				final DisjointSets dj = AffinityWatershedBlocked.mergeRegionGraph( edgeList, counts, ( Predicate ) ( v1, v2 ) -> v1 < v2, new double[] { threshold } )[ 0 ];
				final long[] newCounts = new long[ dj.setCount() ];
				final TLongLongHashMap rootToNewIndexMap = new TLongLongHashMap();
				rootToNewIndexMap.put( 0, 0 );
				newCounts[ 0 ] = counts[ 0 ];
				for ( int i1 = 1, newIndex = 1; i1 < counts.length; ++i1 )
				{
					final int root = dj.findRoot( i1 );
					if ( !rootToNewIndexMap.contains( root ) )
					{
						rootToNewIndexMap.put( root, newIndex );
						newCounts[ newIndex ] = counts[ root ];
						++newIndex;
					}
				}

				for ( int i2 = 0; i2 < labelsArr.length; ++i2 )
					labelsArr[ i2 ] = rootToNewIndexMap.get( dj.findRoot( ( int ) labelsArr[ i2 ] ) );

				return new Tuple2<>( t._1(), new Tuple2<>( labelsArr, newCounts ) );
			}
		};

		final SparkConf conf = new SparkConf().setAppName( "Watersheds" ).setMaster( "local[*]" ).set( "spark.driver.maxResultSize", "4g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );


		final JavaPairRDD< Tuple3< Long, Long, Long >, float[] > imgs =
				sc.parallelize( lowerBounds ).mapToPair( t -> {
					final long[] o = new long[] { t._1(), t._2(), t._3(), 0 };
					final long[] currentDims = new long[ dimsIntv.length ];
					for ( int d = 0; d < dimsIntv.length; ++d )
						currentDims[ d ] = Math.min( dimsIntv[d], dims[d] - o[0] );
					return new Tuple2<>( t, opener.open( o, dimsIntv ) );
				} ).cache();

		final JavaPairRDD< Tuple3< Long, Long, Long >, Tuple2< long[], long[] > > ws = imgs.mapToPair(
				func
				).cache();

		final List< Tuple2< Tuple3< Long, Long, Long >, long[] > > labelingsAndCounts = ws
				.mapToPair( t -> new Tuple2<>( t._1(), t._2()._2() ) )
				.collect();

		final ArrayList< Tuple2< Tuple3< Long, Long, Long >, long[] > > al = new ArrayList< Tuple2< Tuple3< Long, Long, Long >, long[] > >();
		for ( final Tuple2< Tuple3< Long, Long, Long >, long[] > lac : labelingsAndCounts )
			al.add( lac );

		Collections.sort( al, ( o1, o2 ) -> {
			final Tuple3< Long, Long, Long > o11 = o1._1();
			final Tuple3< Long, Long, Long > o21 = o2._1();
			final long[] arr1 = new long[] { o11._1(), o11._2(), o11._3() };
			final long[] arr2 = new long[] { o21._1(), o21._2(), o21._3() };
			return Long.compare( IntervalIndexer.positionToIndex( arr1, dimsNoChannels ), IntervalIndexer.positionToIndex( arr2, dimsNoChannels ) );
		} );

		final TLongLongHashMap startIndices = new TLongLongHashMap();
		long startIndex = 0;
		for ( final Tuple2< Tuple3< Long, Long, Long >, long[] > t : al )
		{

			final Tuple3< Long, Long, Long > t1 = t._1();
			final long[] arr1 = new long[] { t1._1(), t1._2(), t1._3() };
			startIndices.put( IntervalIndexer.positionToIndex( arr1, dimsNoChannels ), startIndex );
			final long c = t._2().length - 1l;
			startIndex += c; // don't count background
		} ;
		final int numRegions = ( int ) startIndex;

		System.out.println( "Collected " + labelingsAndCounts.size() + " labelings." );

		final String listenerAddr = "ipc://labels_listener";
		final Socket labelsTargetListener = zmqContext.createSocket( ZMQ.REP );
		labelsTargetListener.bind( listenerAddr );
//
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

				final long offset = startIndices.get( json.get( "id" ).getAsLong() );

//					System.out.println( "Received labeling message: " + new String( message ) );

				long minL = Long.MAX_VALUE;
				long maxL = Long.MIN_VALUE;
				boolean hasZero = false;

				for ( final LongType l : Views.flatIterable( Views.interval( labelsTarget, min, max ) ) )
				{
					final long next = bb.getLong();
					final long val = next == 0 ? 0 : offset + next;
					l.set( val );
					if ( val == 0 )
						hasZero = true;
					else if ( val < minL )
						minL = val;
					else if ( val > maxL )
						maxL = val;

//						System.out.println( l );
				}
				System.out.println( Arrays.toString( min ) + " " + minL + " " + maxL + " " + hasZero );
				labelsTargetListener.send( "" );
			}
		} );
		labelsThread.start();

		ws.map( t -> {
			final ZContext context = new ZContext();
			final Tuple3< Long, Long, Long > t1 = t._1();
			final long[] min = new long[] { t1._1(), t1._2(), t1._3() };
			final long[] max = new long[] { t1._1() + dimsIntv[ 0 ] - 1, t1._2() + dimsIntv[ 1 ] - 1, t1._3() + dimsIntv[ 2 ] - 1 };
			final byte[] bytes = new byte[ ( int ) ( size * Long.BYTES ) ];
			final ByteBuffer bb = ByteBuffer.wrap( bytes );
			final long[] lbls = t._2()._1();
			final long[] arr2 = new long[] { t1._1(), t1._2(), t1._3() };
			for ( final long l : lbls )
				bb.putLong( l );

			final Gson localGson = new Gson();
			final JsonObject obj = new JsonObject();
			obj.add( "min", localGson.toJsonTree( min ) );
			obj.add( "max", localGson.toJsonTree( max ) );
//				obj.add( "data", localGson.toJsonTree( new String( bytes ) ) );
			obj.add( "data", localGson.toJsonTree( DatatypeConverter.printBase64Binary( bytes ) ) );

			obj.addProperty( "id", IntervalIndexer.positionToIndex( arr2, dimsNoChannels ) );

			final Socket socket = context.createSocket( ZMQ.REQ );
			socket.connect( listenerAddr );

//				System.out.println( "Sending " + obj.toString() );
			System.out.println( "Sending message to " + socket.getType() + "(" + ZMQ.REQ + ") " + socket.getIdentity() );
			socket.send( obj.toString() );
			socket.recv();

			context.close();

			return true;
		} ).count();

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
				// TODO Auto-generated catch block
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.print( "Closing context" );
		zmqContext.close();

		System.out.println( "Closing spark" );
		sc.close();


		System.out.println( "show labels" );
		{
			final int[] colors = new int[ numRegions + 1 ];
			final Random rng = new Random( 100 );
			{
				colors[ 0 ] = 0;
				for ( int i = 1; i < colors.length; ++i )
					colors[ i ] = rng.nextInt();
			}

			final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert(
					( RandomAccessibleInterval< LongType > ) labelsTarget,
					( Converter< LongType, ARGBType > ) ( s, t ) -> t.set( colors[ s.getInteger() ] ),
					new ARGBType() );
//			BdvFunctions.show( labelsTarget, "labels" );
//			BdvFunctions.show( coloredLabels, "colored labels" );
			System.out.println( startIndex + " distinct labels (" + startIndex * 1.0 / Integer.MAX_VALUE + ")" );
		}

		{

			final CompositeFactory< FloatType, RealComposite< FloatType > > compFac = ( requiredSize ) -> Views.collapseReal( ArrayImgs.floats( 1, requiredSize ) ).randomAccess().get();
			final RealComposite< FloatType > extension = compFac.create( 3 );
			for ( long i = 0; i < 3; ++i )
				extension.get( i ).set( Float.NaN );

//			final ArrayImg< DoubleType, DoubleArray > affinities = ArrayImgs.doubles( dims[ 0 ], dims[ 1 ], dims[ 2 ], 6 );
//			for ( final Pair< RealComposite< DoubleType >, RealComposite< DoubleType > > p : Views.flatIterable( Views.interval( Views.pair( new AffinityView<>( Views.extendValue( Views.collapseReal( input ), extension ), compFac ), Views.collapseReal( affinities ) ), labelsTarget ) ) )
//			{
//				final RealComposite< DoubleType > ps = p.getA();
//				final RealComposite< DoubleType > pt = p.getB();
//				for ( int i = 0; i < 6; ++i )
//					pt.get( i ).set( ps.get( i ) );
//			}

			final AffinityView< FloatType, RealComposite< FloatType > > affinities =
					new AffinityView<>( Views.extendValue( Views.collapseReal( input ), extension ), compFac );

			final TLongDoubleHashMap rg = AffinityWatershed2.generateRegionGraph(
					affinities,
					labelsTarget,
					AffinityWatershed2.generateSteps( AffinityWatershed2.generateStride( labelsTarget ) ),
					( f, s ) -> f.get() > s.get(),
					new FloatType( -Float.MIN_VALUE ),
					1 << 63,
					1 << 62,
					numRegions + 1 );

			final Path p = Paths.get( System.getProperty( "user.home" ) + "/local/tmp/watershed-spark-rg" );
			Files.deleteIfExists( p );
			Files.createFile( p );
			for ( final TLongDoubleIterator it = rg.iterator(); it.hasNext(); )
			{
				it.advance();
				Files.write( p, ( it.key() + "," + it.value() + "\n" ).getBytes(), StandardOpenOption.APPEND );
			}

			final RefArrayList< net.imglib2.algorithm.morphology.watershed.AffinityWatershed2.WeightedEdge > edgeList =
					AffinityWatershed2.graphToList( rg, numRegions + 1 );
			Collections.sort( edgeList, Collections.reverseOrder() );
			final long[] offsetCounts = new long[ numRegions + 1 ];
			for( final LongType l : labelsTarget )
				++offsetCounts[ l.getInteger() ];

//			offsetCounts[ 269511 ] = 858;
//			offsetCounts[ 276235 ] = 3232;
//			offsetCounts[ 283176 ] = 143;
//			offsetCounts[ 433426 ] = 82;

			System.out.println( offsetCounts[ 0 ] + " " + offsetCounts[ 1 ] + " OFFSET COUNTS MAN! " + offsetCounts.length + " " + numRegions );

			FileUtils.writeStringToFile(
					new File( System.getProperty( "user.home" ) + "/local/tmp/watershed-spark-counts" ),
					Arrays.toString( offsetCounts ) );

			FileUtils.writeLines( new File( System.getProperty( "user.home" ) + "/local/tmp/watershed-spark-edges" ), edgeList );

			final DisjointSets dj = AffinityWatershed2.mergeRegionGraph(
					edgeList,
					offsetCounts,
					( net.imglib2.algorithm.morphology.watershed.AffinityWatershed2.Predicate ) ( v1, v2 ) -> v1 < v2,
					new double[] { -1.0e4, 10, 100, 1000, 10000 } )[ 4 ];



			final TLongIntHashMap colors = new TLongIntHashMap();
			colors.put( 0, 0 );
			final Random rng = new Random( 100 );
			for ( int i = 1; i < numRegions + 1; ++i )
			{
				final int r = dj.findRoot( i );
				if ( !colors.contains( r ) )
					colors.put( r, rng.nextInt() );
			}

//			{
//				colors[ 0 ] = 0;
//				for ( int i = 1; i < colors.length; ++i )
//					colors[ i ] = rng.nextInt();
//			}

			System.out.println( colors.size() + " " + dj.size() + " " + dj.setCount() + " DAKLFJSDLKFJD " );

			final RandomAccessibleInterval< LongType > mergedLabels = Converters.convert(
					( RandomAccessibleInterval< LongType > ) labelsTarget,
					( Converter< LongType, LongType > ) ( s, t ) -> t.set( dj.findRoot( s.getInteger() ) ),
					new LongType() );

			final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert(
					mergedLabels,
					( Converter< LongType, ARGBType > ) ( s, t ) -> t.set( colors.get( s.getInteger() ) ),
					new ARGBType() );
			final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredLabels, "colored labels" );
			final RealRandomAccessible< LongType > rra = Views.interpolate( Views.extendValue( mergedLabels, new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() );

			addValueOverlay( rra, bdv.getBdvHandle().getViewerPanel() );
		}

		final TLongHashSet s = new TLongHashSet();
		for ( final LongType l : labelsTarget )
			s.add( l.get() );
		System.out.println( " TLHS " + s.size() + " " + numRegions );
		for ( final int[] m : AffinityWatershedBlocked.generateMoves( 3 ) )
			System.out.println( "Moves: " + Arrays.toString( m ) );

	}

	public static < T extends Type< T > > void addValueOverlay( final RealRandomAccessible< T > accessible, final ViewerPanel panel )
	{
		final ValueDisplayListener< T > vdl = new ValueDisplayListener<>( accessible.realRandomAccess(), panel );
		panel.getDisplay().addOverlayRenderer( vdl );
		panel.getDisplay().addMouseMotionListener( vdl );
	}

	public static < T extends Type< T > > T getVal( final int x, final int y, final RealRandomAccess< T > access, final ViewerPanel viewer )
	{
		access.setPosition( x, 0 );
		access.setPosition( y, 1 );
		access.setPosition( 0, 2 );

		viewer.displayToGlobalCoordinates( access );

		return access.get();
	}

	public static class ValueDisplayListener< T extends Type< T > > implements MouseMotionListener, OverlayRenderer
	{

		private final RealRandomAccess< T > access;

		private final ViewerPanel viewer;

		private int width = 0;

		private int height = 0;

		private T val = null;

		public ValueDisplayListener( final RealRandomAccess< T > access, final ViewerPanel viewer )
		{
			super();
			this.access = access;
			this.viewer = viewer;
		}

		@Override
		public void mouseDragged( final MouseEvent e )
		{}

		@Override
		public void mouseMoved( final MouseEvent e )
		{
			final int x = e.getX();
			final int y = e.getY();

			this.val = getVal( x, y, access, viewer );
			viewer.getDisplay().repaint();
		}

		@Override
		public void drawOverlays( final Graphics g )
		{
			if ( val != null )
			{
				final Graphics2D g2d = ( Graphics2D ) g;

				g2d.setRenderingHint( RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON );
				g2d.setComposite( AlphaComposite.SrcOver );

				final int w = 176;
				final int h = 11;

				final int top = height - h;
				final int left = width - w;

				g2d.setColor( Color.white );
				g2d.fillRect( left, top, w, h );
				g2d.setColor( Color.BLACK );
				final String string = val.toString();
				g2d.drawString( string, left + 1, top + h - 1 );
//				drawBox( "selection", g2d, top, left, fid );
			}
		}

		@Override
		public void setCanvasSize( final int width, final int height )
		{
			this.width = width;
			this.height = height;
		}

	}
}
