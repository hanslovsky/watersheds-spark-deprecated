package de.hanslovsky.watersheds;

import java.io.File;
import java.io.IOException;
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

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.mastodon.collection.ref.RefArrayList;
import org.scijava.ui.behaviour.io.InputTriggerConfig;
import org.scijava.ui.behaviour.io.InputTriggerDescription;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.io.AffinitiesChunkLoader;
import de.hanslovsky.watersheds.io.LabelsChunkWriter;
import de.hanslovsky.watersheds.io.ZMQFileOpenerFloatType;
import de.hanslovsky.watersheds.io.ZMQFileWriterLongType;
import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershed2;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.Predicate;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.WeightedEdge;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.algorithm.morphology.watershed.affinity.AffinityView;
import net.imglib2.algorithm.morphology.watershed.affinity.CompositeFactory;
import net.imglib2.converter.Converter;
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
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class WatershedsSpark
{

	public static void main( final String[] args ) throws IOException
	{

		final int[] dimsInt = new int[] { 200, 200, 20, 3 }; // dropbox
//		final int[] dimsInt = new int[] { 1554, 1670, 153, 3 }; // A
		final long[] dims = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ], dimsInt[ 3 ] };
		final long[] dimsNoChannels = new long[] { dimsInt[ 0 ], dimsInt[ 1 ], dimsInt[ 2 ] };
		final int[] dimsIntervalInt = new int[] { 20, 20, 10, 3 };
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
					upper[ 3 ] = 2;
					for ( int d = 0; d < upper.length; ++d )
					{
						upper[ d ] = Math.min( dims[ d ] - 1, lower[ d ] + dimsInterval[ d ] - 1 );
						dm[ d ] = upper[ d ] - lower[ d ] + 1;
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
//				.set( "log4j.logger.org", "OFF" )
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

		System.out.println( "show labels" );
		{
			final int[] colors = new int[ ( int ) ( numRegions + 1 ) ];
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
			System.out.println( startIndex + " distinct labels (" + startIndex * 1.0 / Integer.MAX_VALUE + ")" );
		}

		{

			final CompositeFactory< FloatType, RealComposite< FloatType > > compFac = ( requiredSize ) -> {
				return Views.collapseReal( ArrayImgs.floats( 1, requiredSize ) ).randomAccess().get();
			};
			final RealComposite< FloatType > extension = compFac.create( 3 );
			for ( long i = 0; i < 3; ++i )
				extension.get( i ).set( Float.NaN );

			final TLongHashSet lHS = new TLongHashSet();
			for ( final LongType l : labelsTarget )
				if ( !lHS.contains( l.get() ) )
					lHS.add( l.get() );
			System.out.println( "LABEEEEELLLLS: " + lHS.size() + " " + numRegions );

			final AffinityView< FloatType, RealComposite< FloatType > > affinities =
					new AffinityView<>( Views.extendValue( Views.collapseReal( input ), extension ), compFac );

			final TLongDoubleHashMap rg = AffinityWatershedBlocked.generateRegionGraph(
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

			final RefArrayList< WeightedEdge > edgeList =
					AffinityWatershedBlocked.graphToList( rg, numRegions + 1 );
			Collections.sort( edgeList, Collections.reverseOrder() );
			final long[] offsetCounts = new long[ ( int ) ( numRegions + 1 ) ];
			for ( final LongType l : labelsTarget )
				++offsetCounts[ l.getInteger() ];

			System.out.println( offsetCounts[ 0 ] + " " + offsetCounts[ 1 ] + " OFFSET COUNTS MAN! " + offsetCounts.length + " " + numRegions );

			FileUtils.writeStringToFile(
					new File( System.getProperty( "user.home" ) + "/local/tmp/watershed-spark-counts" ),
					Arrays.toString( offsetCounts ) );

			FileUtils.writeLines( new File( System.getProperty( "user.home" ) + "/local/tmp/watershed-spark-edges" ), edgeList );

			final DisjointSets dj = AffinityWatershedBlocked.mergeRegionGraph(
					edgeList,
					offsetCounts,
					( Predicate ) ( v1, v2 ) -> v1 < v2,
					new double[] { -1.0, 100, 1000, 10000 } )[ 3 ];

			final TLongIntHashMap colors = new TLongIntHashMap();
			colors.put( 0, 0 );
			final Random rng = new Random( 100 );
			for ( int i = 1; i < numRegions + 1; ++i )
			{
				final int r = dj.findRoot( i );
				if ( !colors.contains( r ) )
					colors.put( r, rng.nextInt() & 0x00ffffff ); // set alpha to
				// 0?
			}

			final RandomAccessibleInterval< LongType > mergedLabels = Converters.convert(
					( RandomAccessibleInterval< LongType > ) labelsTarget,
					( Converter< LongType, LongType > ) ( s, t ) -> t.set( dj.findRoot( s.getInteger() ) ),
					new LongType() );

			final TLongHashSet hs = new TLongHashSet();
			for ( final Pair< LongType, LongType > pair : Views.flatIterable( Views.interval( Views.pair( labelsTarget, mergedLabels ), labelsTarget ) ) )
				if ( pair.getB().get() == 4234 )
					hs.add( pair.getA().get() );
			{
				final File f = new File( System.getProperty( "user.home" ) + "/local/tmp/4234-regions" );
				if ( !f.exists() )
					FileUtils.writeByteArrayToFile( f, new Gson().toJson( hs.toArray() ).getBytes() );
			}

			final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert(
					mergedLabels,
					( Converter< LongType, ARGBType > ) ( s, t ) -> t.set( colors.get( s.getInteger() ) ),
					new ARGBType() );
			final InputTriggerConfig config = new InputTriggerConfig(
					Arrays.asList(
							new InputTriggerDescription[] {
									new InputTriggerDescription( new String[] { "not mapped" }, "drag rotate slow", "bdv" )
							} ) );
			final BdvOptions opts = BdvOptions.options().inputTriggerConfig( config );
			final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredLabels, "colored labels", opts );
			final RandomAccessibleInterval< LongType > l4234 = Converters.convert(
					( RandomAccessibleInterval< LongType > ) labelsTarget,
					( s, t ) -> t.set( AffinityWatershedBlocked.IDS.contains( s.get() ) ? 1 << 15 : 0 ),
//					( s, t ) -> t.set( s.get() == 4234 ? 1 << 15 : 0 ),
					new LongType() );
			BdvFunctions.show( l4234, "4234", BdvOptions.options().addTo( bdv ) );

			final RealRandomAccessible< LongType > rra = Views.interpolate( Views.extendValue( mergedLabels, new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() );
			ValueDisplayListener.addValueOverlay( rra, bdv.getBdvHandle().getViewerPanel() );

//			final ShowEdges se = new ShowEdges(
//					AffinityWatershedBlocked.EDGES,
//					bdv.getBdvHandle().getViewerPanel(),
//					bdv.getBdvHandle().getTriggerbindings() );
//
//			final RandomAccessibleInterval< ARGBType > singleEdgeDisplay = Converters.convert(
//					( RandomAccessibleInterval< LongType > ) labelsTarget,
//					( s, t ) -> {
//						final WeightedEdge e = se.getEdge();
//						if ( e == null )
//							t.set( 0 );
//						else if ( s.get() == e.getFirst() )
//							t.set( ARGBType.rgba( 255, 0, 0, 255 ) );
//						else if ( s.get() == e.getSecond() )
//							t.set( ARGBType.rgba( 0, 255, 0, 255 ) );
//						else
//							t.set( 0 );
//					},
//					new ARGBType() );
//
//			BdvFunctions.show( singleEdgeDisplay, "single edges", BdvOptions.options().addTo( bdv ) );
//
//			BdvFunctions.show( labelsTarget, "labels", BdvOptions.options().addTo( bdv ) );

		}

		final TLongHashSet s = new TLongHashSet();
		for ( final LongType l : labelsTarget )
			s.add( l.get() );
		System.out.println( " TLHS " + s.size() + " " + numRegions );
		for ( final int[] m : AffinityWatershedBlocked.generateMoves( 3 ) )
			System.out.println( "Moves: " + Arrays.toString( m ) );

	}

}
