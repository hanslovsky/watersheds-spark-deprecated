package de.hanslovsky.watersheds.rewrite;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight.FunkyWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.BlockDivision;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingArrayBased;
import de.hanslovsky.watersheds.rewrite.util.EdgeCheck;
import de.hanslovsky.watersheds.rewrite.util.ExtractLabelsOnly;
import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.IdServiceZMQ;
import de.hanslovsky.watersheds.rewrite.util.IterableWithConstant;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class WatershedsSparkWithRegionMergingLoadSegmentation
{

	public static void main( final String[] args ) throws IOException
	{

		// define dimensions and load data
		final int[] cellSize = new int[] { 300, 300, 100, 3 };
		final int[] cellSizeLabels = Util.dropLast( cellSize );
//		final int[] dimsIntervalInt = new int[] { 300, 300, 100, 3 };
		final int[] dimsIntervalInt = new int[] { 100, 100, 2 };
		final long[] dimsInterval = Arrays.stream( dimsIntervalInt ).mapToLong( i -> i ).toArray();
		final int[] dimsIntervalIntNoChannels = Util.dropLast( dimsIntervalInt );
		final long[] dimsIntervalNoChannels = Util.dropLast( dimsInterval );


		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt2D.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt-60x60x20-blocks.h5";

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data = H5Utils.loadFloat( path, "main", cellSize );
		final long[] dims = Intervals.dimensionsAsLongArray( data );
		final long inputSize = Intervals.numElements( data );
		System.out.println( "Loaded data (" + inputSize + ")" );
		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > input = Views.permuteCoordinates( data, perm, data.numDimensions() - 1 );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );
		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( affs );

		assert Arrays.equals( Util.dropLast( dims ), dimsNoChannels ): Arrays.toString( dims ) + " " + Arrays.toString( dimsNoChannels );

		BdvFunctions.show( Converters.convert( affs, ( s, t ) -> {
			Util.max( s, t, affs.numDimensions() );
			t.mul( ( 1 << 16 ) * 1.0 );
		}, new FloatType() ), "affs", Util.bdvOptions( affs ) );


		final Img< LongType > labelsTarget = H5Utils.loadUnsignedLong( path, "zws", cellSizeLabels );

		final TLongLongHashMap counts = Util.countLabels( labelsTarget );

		final ArrayList< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > > blocks =
				createBlocks( Views.extendValue( input, new FloatType( Float.NaN ) ), Views.extendValue( labelsTarget, new LongType( -1 ) ), dimsNoChannels, dimsIntervalNoChannels, counts );


		// start spark server
		final SparkConf conf = new SparkConf().setAppName( "Watersheds" ).setMaster( "local[*]" ).set( "spark.driver.maxResultSize", "4g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );

		final Context ctx = ZMQ.context( 1 );

		final String blockIdAddr = "ipc://blockIdService";
		final Socket blockIdSocket = IdServiceZMQ.createServerSocket( ctx, blockIdAddr );
		final Thread blockIdThread = IdServiceZMQ.createServerThread( blockIdSocket, new AtomicLong( 0 ) );
		blockIdThread.start();
		final IdServiceZMQ blockIdService = new IdServiceZMQ( blockIdAddr );

		final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksRdd =
				sc.parallelizePairs( blocks ).cache();


		final EdgeMerger merger = new EdgeMerger.MAX_AFFINITY_MERGER();
		final FunkyWeight weightFunc = new EdgeWeight.FunkyWeight();

		final Broadcast< long[] > dimsIntervalNoChannelsBC = sc.broadcast( dimsIntervalNoChannels );

		final ArrayList< JavaPairRDD< HashableLongArray, long[] > > labelBlocks = new ArrayList<>();
		labelBlocks.add( blocksRdd.mapToPair( new ExtractLabelsOnly<>( dimsIntervalNoChannelsBC ) ) );

		// create initial blocks
		final Tuple2< JavaPairRDD< Long, BlockDivision >, JavaPairRDD< HashableLongArray, long[] > > graphsAndBorderNodes =
				PrepareRegionMergingCutBlocks.run(
						sc,
						blocksRdd,
						sc.broadcast( dimsNoChannels ),
						sc.broadcast( dimsIntervalNoChannels ),
						merger,
						weightFunc,
						( EdgeCheck & Serializable ) e -> e.affinity() >= 0.5,
						blockIdService );
		final JavaPairRDD< Long, BlockDivision > graphs = graphsAndBorderNodes._1().cache();

		final Map< Long, HashableLongArray > blockToInitialBlockMap =
				graphsAndBorderNodes._2().flatMapToPair( t -> new IterableWithConstant<>( Arrays.asList( ArrayUtils.toObject( t._2() ) ), t._1() ) ).collectAsMap();
		final Broadcast< Map< Long, HashableLongArray > > blockToInitialBlockMapBC = sc.broadcast( blockToInitialBlockMap );

		// make sure we know which original block everything points to.
		final List< Tuple2< HashableLongArray, long[] > > blockContainsList = graphsAndBorderNodes._2().collect();
		final TLongObjectHashMap< HashableLongArray > blockContains = new TLongObjectHashMap<>();
		for ( final Tuple2< HashableLongArray, long[] > bcl : blockContainsList )
			for ( final long l : bcl._2() )
				blockContains.put( l, bcl._1() );

		final RegionMergingArrayBased rm = new RegionMergingArrayBased( merger, weightFunc );

		final ArrayList< RandomAccessibleInterval< LongType > > blockImages = new ArrayList<>();
		final Img< LongType > blockZero = labelsTarget.factory().create( labelsTarget, new LongType() );
		final TLongLongHashMap labelBlockmap = generateLabelBlockMap( graphs );

		for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( labelsTarget, blockZero ), blockZero ) )
			p.getB().set( labelBlockmap.get( p.getA().get() ) );
		blockImages.add( blockZero );
		final TLongIntHashMap blockColors = new TLongIntHashMap();
		final Random blockRng = new Random( 100 );
		final List< Long > blockIds = graphs.keys().collect();
		for ( final Long b : blockIds )
			blockColors.put( b.longValue(), blockRng.nextInt() );

		final ArrayList< RandomAccessibleInterval< LongType > > images = new ArrayList<>();

		images.add( labelsTarget );

		final TLongIntHashMap colorMap = new TLongIntHashMap();
		{
			final Random rng = new Random( 100 );
			final RandomAccessibleInterval< LongType > i0 = images.get( 0 );
			for ( final LongType i : Views.flatIterable( i0 ) )
				if ( !colorMap.contains( i.get() ) )
					colorMap.put( i.get(), rng.nextInt() );
		}

		final BdvStackSource< ARGBType > chBdv = BdvFunctions.show( Util.toColor( images.get( 0 ), colorMap ), "colored history", Util.bdvOptions( labelsTarget ) );

		final BdvStackSource< ARGBType > cbhBdv = BdvFunctions.show( Util.toColor( blockImages.get( 0 ), blockColors ), "colored block history", Util.bdvOptions( labelsTarget ) );

		System.out.println( "COLORED HISTORY SIZE " + images.size() + " COLORED BLOCK HISTORY SIZE " + blockImages.size() );

		final VisualizationVisitor rmVisitor = new VisualizationVisitor(
				sc,
				blockToInitialBlockMapBC,
				dimsIntervalNoChannels,
				labelBlocks,
				images,
				blockImages,
				colorMap,
				blockColors,
				chBdv,
				cbhBdv,
				labelsTarget.factory() );

		final double threshold = 200;

		final JavaPairRDD< Long, MergeBlocIn > graphsAfterMerging = rm.run( sc, RegionMergingArrayBased.fromBlockDivision( graphs ), threshold, rmVisitor );
		graphsAfterMerging.count();

		sc.close();

		ctx.close();


	}

	public static ArrayList< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > > createBlocks(
			final RandomAccessible< FloatType > affs,
			final RandomAccessible< LongType > labelsExtend,
			final long[] dimsNoChannels,
			final long[] dimsIntervalNoChannels,
			final TLongLongHashMap counts )
	{

		final long[] extendedBlockSize = Arrays.stream( dimsIntervalNoChannels ).map( l -> l + 2 ).toArray();
		final long[] extendedBlockSizeAffs = Util.append( extendedBlockSize, extendedBlockSize.length );

		final int numberOfElementsPerLabelBLock = ( int ) Intervals.numElements( extendedBlockSize );
		final int numberOfElementsPerAffinityBlock = ( int ) Intervals.numElements( extendedBlockSizeAffs );

		final ArrayList< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > > blocks = new ArrayList<>();
		final long[] offset = new long[ dimsNoChannels.length ];
		for ( int d = 0; d < dimsNoChannels.length; )
		{
			final long[] labelData = new long[ numberOfElementsPerLabelBLock ];
			final long[] lower = Arrays.stream( offset ).map( val -> val - 1 ).toArray();
			final Cursor< LongType > l = Views.offsetInterval( labelsExtend, lower, extendedBlockSize ).cursor();
			for ( int i = 0; l.hasNext(); ++i )
				labelData[ i ] = l.next().get();

			final float[] affsData = new float[ numberOfElementsPerAffinityBlock ];

			final Cursor< FloatType > a = Views.offsetInterval( affs, Util.append( lower, 0 ), extendedBlockSizeAffs ).cursor();
			for ( int i = 0; a.hasNext(); ++i )
				affsData[ i ] = a.next().get();

			blocks.add( new Tuple2<>( new HashableLongArray( offset.clone() ), new Tuple3<>( labelData, affsData, counts ) ) );

			for ( d = 0; d < dimsNoChannels.length; ++d )
			{
				offset[ d ] += dimsIntervalNoChannels[ d ];
				if ( offset[ d ] < dimsNoChannels[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}
		return blocks;
	}

	public static TLongLongHashMap generateLabelBlockMap( final JavaPairRDD< Long, BlockDivision > graphs )
	{
		final TLongLongHashMap labelBlockmap = new TLongLongHashMap();
		for ( final Tuple2< Long, BlockDivision > g : graphs.collect() )
		{
			final long id = g._1();
			final TLongLongHashMap cons = g._2().outsideNodes;
			final Edge e = new Edge( g._2().edges );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long f = e.from();
				final long t = e.to();
				if ( !cons.contains( f ) )
					labelBlockmap.put( f, id );
				if ( !cons.contains( t ) )
					labelBlockmap.put( t, id );
			}
		}
		return labelBlockmap;
	}


}
