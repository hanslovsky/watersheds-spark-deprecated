package de.hanslovsky.watersheds;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.graph.Edge;
import de.hanslovsky.watersheds.graph.EdgeMerger;
import de.hanslovsky.watersheds.graph.Function;
import de.hanslovsky.watersheds.graph.IdServiceZMQ;
import de.hanslovsky.watersheds.graph.MergeBloc;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.MergerServiceZMQ;
import de.hanslovsky.watersheds.graph.MergerServiceZMQ.MergeActionAddToList;
import de.hanslovsky.watersheds.graph.RegionMerging;
import de.hanslovsky.watersheds.regionmerging.EdgeCheck;
import de.hanslovsky.watersheds.regionmerging.PrepareRegionMergingCutBlocks;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class WatershedsSparkWithRegionMergingLoadSegmentation
{

	public static void main( final String[] args ) throws IOException
	{

		final int[] cellSize = new int[] { 150, 150, 100, 3 };
		final int[] cellSizeLabels = Util.dropLast( cellSize );
		final int[] dimsIntervalInt = new int[] { 150, 150, 100, 3 };
		final long[] dimsInterval = Arrays.stream( dimsIntervalInt ).mapToLong( i -> i ).toArray();
		final int[] dimsIntervalIntNoChannels = Util.dropLast( dimsIntervalInt );
		final long[] dimsIntervalNoChannels = Util.dropLast( dimsInterval );


		final String path = Util.HOME_DIR + String.format( "/Dropbox/misc/excerpt-full-in-z.h5" );

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data = H5Utils.loadFloat( path, "main", cellSize );

		final long[] dims = Intervals.dimensionsAsLongArray( data );
		final long inputSize = Intervals.numElements( data );

		System.out.println( "Loaded data (" + inputSize + ")" );


		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > input = Views.permuteCoordinates( data, perm, data.numDimensions() - 1 );

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );
		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( affs );

		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			Util.max( s, t, affs.numDimensions() );
			t.mul( ( 1 << 16 ) * 1.0 );
		}, new FloatType() ), "affs", Util.bdvOptions( affs ) );

//		BdvFunctions.show( img, name )



		final Img< LongType > labelsTargetInput = H5Utils.loadUnsignedLong( path, "zws", cellSizeLabels );
		final ArrayImg< LongType, LongArray > labelsTarget = ArrayImgs.longs( Intervals.dimensionsAsLongArray( labelsTargetInput ) );
		for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( labelsTargetInput, labelsTarget ), labelsTarget ) )
			p.getB().set( p.getA().getIntegerLong() );

		final TLongLongHashMap counts = new TLongLongHashMap();
		for ( final LongType l : labelsTarget )
			counts.put( l.get(), counts.contains( l.get() ) ? counts.get( l.get() ) + 1 : 1  );



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

		final long[] offset = new long[ dimsNoChannels.length ];
		for ( int d = 0; d < dimsNoChannels.length; )
		{
			System.out.println( Arrays.toString( offset ) );
			final long[] labelData = new long[ extendedBlockElements ];
			final long[] lower = Arrays.stream( offset ).map( val -> val - 1 ).toArray();
			final Cursor< LongType > l = Views.offsetInterval( labelsExtend, lower, extendedBlockSize ).cursor();
			for ( int i = 0; l.hasNext(); ++i )
				labelData[ i ] = l.next().get();

			final float[] affsData = new float[ extendedBlockElementsAffs ];


			final Cursor< FloatType > a = Views.offsetInterval( Views.extendValue( input, new FloatType( Float.NaN ) ), Util.append( lower, 0 ), extendedBlockSizeAffs ).cursor();
			for ( int i = 0; a.hasNext(); ++i )
				affsData[ i ] = a.next().get();

			blocks.add( new Tuple2<>( new HashableLongArray( offset.clone() ), new Tuple3<>( labelData, affsData, counts ) ) );

			for ( d = 0; d < dimsNoChannels.length; ++d )
			{
				offset[ d ] += dimsIntervalIntNoChannels[ d ];
				if ( offset[ d ] < dimsNoChannels[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}

		final SparkConf conf = new SparkConf().setAppName( "Watersheds" ).setMaster( "local[*]" ).set( "spark.driver.maxResultSize", "4g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );

		final Context ctx = ZMQ.context( 1 );

		final String blockIdAddr = "ipc://blockIdService";
		final Socket blockIdSocket = IdServiceZMQ.createServerSocket( ctx, blockIdAddr );
		final Thread blockIdThread = IdServiceZMQ.createServerThread( blockIdSocket, new AtomicLong( 1 ) );
		blockIdThread.start();
		final IdServiceZMQ blockIdService = new IdServiceZMQ( blockIdAddr );

		final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksRdd =
				sc.parallelizePairs( blocks ).cache();


//		final EdgeMerger merger = MergeBloc.MIN_EDGE_MERGER;
//		final EdgeMerger merger = MergeBloc.DEFAULT_EDGE_MERGER;
		final EdgeMerger merger = MergeBloc.MAX_AFFINITY_MERGER;
//		final Function weightFunc = ( Function & Serializable ) ( a, c1, c2 ) -> Math.min( c1, c2 ) / ( a * a );
//		final Function weightFunc = new RegionMerging.CountOverAffinityToFourthPower();
//		final Function weightFunc = new RegionMerging.CountOverAffinityToPower( 8.0 );
		final Function weightFunc = new RegionMerging.FunkyWeights();
//		final JavaPairRDD< Long, In > graphs =
//				blocksRdd.mapToPair( new PrepareRegionMerging.BuildBlockedGraph( dimsNoChannels, dimsIntervalNoChannels, merger, weightFunc ) ).cache();
		final Tuple2< JavaPairRDD< Long, In >, TLongLongHashMap > graphsAndBorderNodes = PrepareRegionMergingCutBlocks.run( sc, blocksRdd, sc.broadcast( dimsNoChannels ),
				sc.broadcast( dimsIntervalNoChannels ), merger, weightFunc, ( EdgeCheck & Serializable ) e -> e.affinity() >= 0.0, blockIdService );
		final JavaPairRDD< Long, In > graphs = graphsAndBorderNodes._1().cache();

		final List< Tuple2< Long, Long > > duplicateKeys = graphs
				.mapToPair( t -> new Tuple2<>( t._1(), 1l ) )
				.reduceByKey( ( l1, l2 ) -> l1 + l2 )
				.filter( t -> t._2() > 1 )
				.collect();

		System.out.println( "Duplicate keys!\n" + duplicateKeys );


		// print to std out initial number of edges
//		graphs.map( ( t ) -> {
//			System.out.println( "initial: " + t._2().g.edges().size() );
//			return null;
//		} ).count();

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
			final RandomAccessibleInterval< LongType > initialBlocks = Converters.convert( labelsTargetRAI, ( s, t ) -> {
				t.set( nodeBlockMap.get( s.get() ) );
			}, new LongType() );
			final RandomAccessibleInterval< ARGBType > coloredInitialBlocks = Converters.convert( initialBlocks, (s,t ) -> {
				t.set( colors.get( s.get() ) ); }, new ARGBType() );
//			final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredInitialBlocks, "INITIAL BLOCKS " + graphs.count() );
//			ValueDisplayListener.addValueOverlay( Views.interpolate( Views.extendValue(
//					Views.addDimension( initialBlocks, 0, 0 ), new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ), bdv.getBdvHandle().getViewerPanel() );

		}

//		System.out.println( graphs.collect() );
//		for ( final Tuple2< Long, In > gr : graphs.collect() ) {
//			System.out.println( gr._1() );
//			System.out.println( gr._2().counts );
//			final Edge e = new Edge( gr._2().g.edges() );
//			for ( int i = 0; i < e.size(); ++i )
//			{
//				e.setIndex( i );
//				System.out.println( e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
//			}
//		}
//		System.exit( 123 );

//		System.out.println( "GRAPHS " + graphs.collect().get( 0 )._2().counts );

		final String idAddr = "ipc://idService";
		final String mergerAddr = "ipc://mergerService";
		final Socket idSocket = IdServiceZMQ.createServerSocket( ctx, idAddr );
		final Socket mergerSocket = MergerServiceZMQ.createServerSocket( ctx, mergerAddr );
		final Thread idThread = IdServiceZMQ.createServerThread( idSocket, new AtomicLong( 300 * 300 * 10 ) );
		idThread.start();
		final TLongArrayList merges = new TLongArrayList();
		final TLongLongHashMap mergedParents = new TLongLongHashMap();
		final MergeActionAddToList action1 = new MergerServiceZMQ.MergeActionAddToList( merges );

		for ( final TLongIterator it = counts.keySet().iterator(); it.hasNext(); )
		{
			final long k = it.next();
			mergedParents.put( k, k );
		}

//		final TLongIntHashMap colorMap = new TLongIntHashMap();
//		final Random rngCm = new Random( 100 );
//		for ( final LongType l : labelsTarget )
//			if ( !colorMap.contains( l.get() ) )
//				colorMap.put( l.get(), rngCm.nextInt() );

		final Thread mergerThread = MergerServiceZMQ.createServerThread( mergerSocket, ( n1, n2, n, w ) -> {
			action1.add( n1, n2, n, w );
//			action2.add( n1, n2, n, w );
//			synchronized ( colorMap )
//			{
//				colorMap.put( n, colorMap.contains( n1 ) ? colorMap.get( n1 ) : colorMap.get( n2 ) );
//			}
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
//			final TLongObjectHashMap< TLongHashSet > cbns = g._2().borderNodes;
			final TLongLongHashMap cons = g._2().outsideNodes;
			final Edge e = new Edge( g._2().g.edges() );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long f = e.from();
				final long t = e.to();
				if ( !cons.contains( f ) )
					labelBlockmap.put( f, id );
				if ( !cons.contains( t ) )
					labelBlockmap.put( t, id );
//				if ( cbns.contains( f ) )
//					labelBlockmap.put( f, id );
//				else if ( cbns.contains( t ) )
//					labelBlockmap.put( t, id );
//				else
//				{
//					labelBlockmap.put( t, id );
//					labelBlockmap.put( f, id );
//				}
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
		final DisjointSetsHashMap mergeUnionFind = new DisjointSetsHashMap();

		images.add( labelsTarget );
		final RegionMerging.Visitor rmVisitor = ( parents ) -> {
			final Img< LongType > img = labelsTarget.factory().create( images.get( 0 ), new LongType() );

			System.out.println( "Merging " + merges.size() + " edges" );
			for ( int i = 0; i < merges.size(); i += 4 )
			{
				final long f = mergeUnionFind.findRoot( merges.get( i ) );
				final long t = mergeUnionFind.findRoot( merges.get( i + 1 ) );
				if ( f != t )
					mergeUnionFind.join( f, t );
			}

			for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( images.get( images.size() - 1 ), img ), img ) )
//				p.getB().set( mergedParents.contains( p.getA().get() ) ? mergedParents.get( p.getA().get() ) : p.getA().get() );
				p.getB().set( mergeUnionFind.findRoot( p.getA().get() ) );
			images.add( img );

			final Img< LongType > blockImg = labelsTarget.factory().create( blockImages.get( 0 ), new LongType() );
			for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( blockImages.get( blockImages.size() - 1 ), blockImg ), blockImg ) )
				//				if ( !parents.contains( p.getA().get() ) )
//				{
//					System.out.println( "parents does not contain "  + p.getA() );
//					System.exit( 21412412 );
//				}
				p.getB().set( parents.contains( p.getA().get() ) ? parents.get( p.getA().get() ) : p.getA().get() );
			blockImages.add( blockImg );
		};
		final double threshold = 200;// Double.POSITIVE_INFINITY;
		final double thresholdTolerance = 1e0;

		final JavaPairRDD< Long, In > graphsAfterMerging = rm.run( sc, graphs, threshold, rmVisitor, labelsTarget, thresholdTolerance );

		final TLongIntHashMap colorMap = new TLongIntHashMap();
		{
			final Random rng = new Random( 100 );
			final RandomAccessibleInterval< LongType > i0 = images.get( 0 );
			for ( final LongType i : Views.flatIterable( i0 ) )
				if ( !colorMap.contains( i.get() ) )
					colorMap.put( i.get(), rng.nextInt() );

		}

		final RandomAccessibleInterval< ARGBType > coloredHistory = Converters.convert( Views.stack( images ), ( s, t ) -> {
			t.set( colorMap.get( s.get() ) );
		}, new ARGBType() );

		final BdvStackSource< ARGBType > chBdv = BdvFunctions.show( coloredHistory, "colored history", Util.bdvOptions( labelsTarget ) );

		ValueDisplayListener.addValueOverlay(
				Views.interpolate( Views.extendValue( Views.stack( images ), new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ),
				chBdv.getBdvHandle().getViewerPanel() );
		System.out.println( "COLORED HISTORY SIZE " + images.size() + " COLORED BLOCK HISTORY SIZE " + blockImages.size() );
		System.out.println( "merges size: " + merges.size() );

		final RandomAccessibleInterval< ARGBType > coloredBlockHistory =
				Converters.convert( Views.stack( blockImages ), ( s, t ) -> {
					t.set( blockColors.get( s.get() ) );
				}, new ARGBType() );
		final BdvStackSource< ARGBType > cbhBdv = BdvFunctions.show( coloredBlockHistory, "colored block history", Util.bdvOptions( labelsTarget ) );
		ValueDisplayListener.addValueOverlay(
				Views.interpolate( Views.extendValue( Views.stack( blockImages ), new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ),
				cbhBdv.getBdvHandle().getViewerPanel() );

		for ( final TLongIterator kIt = mergedParents.keySet().iterator(); kIt.hasNext(); )
			MergeBloc.findRoot( mergedParents, kIt.next() );

		final Random rng = new Random( 100 );

		final TLongLongHashMap parents = new TLongLongHashMap();
		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, new TLongLongHashMap(), 0 );
		for ( int i = 0; i < merges.size(); i += 4 )
			dj.join( dj.findRoot( merges.get( i ) ), merges.get( i+1 ) );

		final TLongIntHashMap colorsMap = new TLongIntHashMap();
		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
			if ( !colorsMap.contains( it.value() ) )
				colorsMap.put( it.value(), rng.nextInt() );
		}

		final RandomAccessibleInterval< LongType > rooted = Converters.convert( ( RandomAccessibleInterval< LongType > ) labelsTarget, ( s, t ) -> {
			t.set( parents.contains( s.get() ) ? parents.get( s.get() ) : s.get() );
		}, new LongType() );

		final RandomAccessibleInterval< ARGBType > colored = Converters.convert( rooted, ( s, t ) -> {
			t.set( colorsMap.get( s.get() ) );
		}, new ARGBType() );

		sc.close();

		ctx.close();


	}


}
