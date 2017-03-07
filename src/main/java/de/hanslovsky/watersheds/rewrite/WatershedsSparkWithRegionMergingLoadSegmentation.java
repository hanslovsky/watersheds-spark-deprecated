package de.hanslovsky.watersheds.rewrite;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import bdv.img.h5.H5Utils;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeCreator;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.BlockDivision;
import de.hanslovsky.watersheds.rewrite.regionmerging.OriginalLabelData;
import de.hanslovsky.watersheds.rewrite.regionmerging.ReduceBlock;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingArrayBased;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingArrayBased.Visitor;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingInput;
import de.hanslovsky.watersheds.rewrite.regionmerging.RemappedData;
import de.hanslovsky.watersheds.rewrite.util.EdgeCheck;
import de.hanslovsky.watersheds.rewrite.util.ExtractLabelsOnly;
import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.IdServiceZMQ;
import de.hanslovsky.watersheds.rewrite.util.IterableWithConstant;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class WatershedsSparkWithRegionMergingLoadSegmentation
{

	public static interface VisitorFactory
	{
		Visitor create( final JavaSparkContext sc,
				RandomAccessibleInterval< LongType > labels,
				RandomAccessibleInterval< LongType > blocks,
				final Broadcast< Map< Long, HashableLongArray > > blockToInitialBlockMapBC,
				final ArrayList< JavaPairRDD< HashableLongArray, long[] > > labelBlocks );
	}

	public static String AFFINITY_DATASET = "main";

	public static void main( final String[] args ) throws Exception
	{


//		final int[] dimsIntervalInt = new int[] { 150, 150, 2 };
		final int[] dimsIntervalInt = new int[] { 150, 150, 2 };

		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt2D.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt-sliced-blocks.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt-sliced-blocks-only-10-in-z.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/sample_A.augmented.0-slice-100.hdf";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/sample_A.augmented.0-500x500x50+500+500+50.hdf";
//		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";
//		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5-2D.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt-yz-slice.h5";

		final IHDF5Reader f = HDF5Factory.openForReading( path );
		final long[] dims = Util.reverse( f.object().getDimensions( AFFINITY_DATASET ) );
		final int[] cs = f.object().getDataSetInformation( AFFINITY_DATASET ).tryGetChunkSizes();
		final int[] cellSize = cs == null ? Arrays.stream( dims ).mapToInt( i -> ( int ) i ).toArray() : Util.reverse( cs );
		final int[] cellSizeLabels = Util.dropLast( cellSize );

		final long[] dimsInterval = Arrays.stream( dimsIntervalInt ).mapToLong( i -> i ).toArray();
		final int[] dimsIntervalIntNoChannels = Util.dropLast( dimsIntervalInt );
		final long[] dimsIntervalNoChannels = Util.dropLast( dimsInterval );
		System.out.println( "Loading data: " + path );
		final CellImg< FloatType, ?, ? > data = H5Utils.loadFloat( f, AFFINITY_DATASET, cellSize );
		f.close();

		final long inputSize = Intervals.numElements( data );
		System.out.println( "Loaded data (" + inputSize + ")" );
		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
		final RandomAccessibleInterval< FloatType > input = Views.permuteCoordinates( data, perm, data.numDimensions() - 1 );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );
		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( affs );

		for ( final FloatType a : Views.flatIterable( data ) )
			a.set( a.get() < 0.00 ? 0.0f : a.get() > 1.0 ? 1.0f : a.get() );

		// anything that points outside must be nan
		for ( int d = 0; d < affs.numDimensions(); ++d )
		{
			final IntervalView< RealComposite< FloatType > > hs = Views.hyperSlice( affs, d, affs.max( d ) );
			for ( final RealComposite< FloatType > a : hs )
				a.get( d ).set( Float.NaN );
		}

		assert Arrays.equals( Util.dropLast( dims ), dimsNoChannels ): Arrays.toString( dims ) + " " + Arrays.toString( dimsNoChannels );

		Util.showAffinities( affs );

		System.out.println( "Loading labels: " + path );
		final Img< LongType > labelsTarget = H5Utils.loadUnsignedLong( path, "zws", cellSizeLabels );
		System.out.println( "Loaded labels." );


		// start spark server
		System.out.println( "Starting Spark server... " );
		final SparkConf conf = new SparkConf().setAppName( "Watersheds" ).setMaster( "local[1]" ).set( "spark.driver.maxResultSize", "4g" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );

		final String OPTION = "AFF_MEDIAN_LOG_COUNT";
		final EdgeMerger merger;
		final EdgeWeight weightFunc;
		final EdgeCreator edgeCreator;
		final int nBins = 100;
		final double min = 0.0;
		final double max = 1.0;
		switch ( OPTION )
		{
		case "AFF_ONLY_MIN":
			merger = new EdgeMerger.MIN_AFFINITY_MERGER();
			weightFunc = ( EdgeWeight & Serializable ) ( e, c1, c2 ) -> 1 - e.affinity();
			edgeCreator = new EdgeCreator.SerializableCreator();
			break;
		case "AFF_ONLY_MAX":
			merger = new EdgeMerger.MAX_AFFINITY_MERGER();
			weightFunc = ( EdgeWeight & Serializable ) ( e, c1, c2 ) -> 1 - e.affinity();
			edgeCreator = new EdgeCreator.SerializableCreator();
			break;
		case "AFF_ONLY_AVG":
			merger = new EdgeMerger.AVG_AFFINITY_MERGER();
			weightFunc = ( EdgeWeight & Serializable ) ( e, c1, c2 ) -> 1 - e.affinity();
			edgeCreator = new EdgeCreator.SerializableCreator();
			break;
		case "FUNKY":
			merger = new EdgeMerger.MAX_AFFINITY_MERGER();
			weightFunc = new EdgeWeight.FunkyWeight();
			edgeCreator = new EdgeCreator.SerializableCreator();
			break;
		case "LOG_COUNT":
			merger = new EdgeMerger.AVG_AFFINITY_MERGER();
			weightFunc = ( EdgeWeight & Serializable ) ( e, c1, c2 ) -> ( 1 - e.affinity() ) * Math.log10( 1 + Math.min( c1, c2 ) );
			edgeCreator = new EdgeCreator.SerializableCreator();
			break;
		case "AFF_MEDIAN":
			merger = new EdgeMerger.MEDIAN_AFFINITY_MERGER( nBins );
			weightFunc = new EdgeWeight.MedianAffinityWeight( nBins, min, max );
			edgeCreator = new EdgeCreator.AffinityHistogram( nBins, min, max );
			break;
		case "AFF_MEDIAN_LOG_COUNT":
			merger = new EdgeMerger.MEDIAN_AFFINITY_MERGER( nBins );
			weightFunc = new EdgeWeight.MedianAffinityLogCountWeight( nBins, min, max );
			edgeCreator = new EdgeCreator.AffinityHistogram( nBins, min, max );
			break;
		default:
			merger = null;
			weightFunc = null;
			edgeCreator = null;
			break;
		}

//		final EdgeCheck edgeCheck = ( EdgeCheck & Serializable ) e -> e.affinity() >= 0.97;
		final EdgeCheck edgeCheck = ( EdgeCheck & Serializable ) e -> e.weight() <= 0.01;

		final double threshold = 0.9;// 100.0;

		final VisitorFactory visFac = ( sc1, labels, blocks, blockToInitialBlockMapBC, labelBlocks ) -> {
			final VisualizationVisitor vis = new VisualizationVisitor(
					sc1,
					blockToInitialBlockMapBC,
					dimsIntervalNoChannels,
					labelBlocks,
					labelsTarget,
					blocks,
					labelsTarget.factory(),
					path );
			return vis;
		};

		System.out.println( "Counting..." );
		final TLongLongHashMap counts = countByLabel( labelsTarget );

		System.out.println( "Creating blocks... " );
		final ArrayList< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > > blocks =
				createBlocks( Views.extendValue( input, new FloatType( Float.NaN ) ), Views.extendValue( labelsTarget, new LongType( -1 ) ), dimsNoChannels, dimsIntervalNoChannels, counts );
		final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksRdd =
				sc.parallelizePairs( blocks ).cache();

		run( sc, labelsTarget, blocksRdd, dimsIntervalNoChannels, labelsTarget.factory(), edgeCreator, merger, weightFunc, edgeCheck, threshold, visFac );

	}

	public static void run(
			final JavaSparkContext sc,
			final RandomAccessibleInterval< LongType > labelsTarget,
			final JavaPairRDD< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > blocksRdd,
			final long[] dimsIntervalNoChannels,
			final ImgFactory< LongType > labelsImgFactory,
			final EdgeCreator edgeCreator,
			final EdgeMerger merger,
			final EdgeWeight weightFunc,
			final EdgeCheck edgeCheck,
			final double threshold,
			final VisitorFactory visFac ) throws InterruptedException, ExecutionException
	{

		final long[] dimsNoChannels = Intervals.dimensionsAsLongArray( labelsTarget );

		final Context ctx = ZMQ.context( 1 );

		final String blockIdAddr = "ipc://blockIdService";
		final Socket blockIdSocket = IdServiceZMQ.createServerSocket( ctx, blockIdAddr );
		final Thread blockIdThread = IdServiceZMQ.createServerThread( blockIdSocket, new AtomicLong( 0 ) );
		blockIdThread.start();
		final IdServiceZMQ blockIdService = new IdServiceZMQ( blockIdAddr );

		System.out.println( blocksRdd.count() + " initial blocks..." );

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
						edgeCreator,
						merger,
						weightFunc,
						edgeCheck,
						blockIdService );
		final JavaPairRDD< Long, BlockDivision > graphs = graphsAndBorderNodes._1();
		System.out.println( "Graphs num partitions: " + graphs.getNumPartitions() + " " + graphs.count() );

		final Map< Long, HashableLongArray > blockToInitialBlockMap =
				graphsAndBorderNodes._2().flatMapToPair( t -> new IterableWithConstant<>( Arrays.asList( ArrayUtils.toObject( t._2() ) ), t._1() ).iterator() ).collectAsMap();
		final Broadcast< Map< Long, HashableLongArray > > blockToInitialBlockMapBC = sc.broadcast( blockToInitialBlockMap );

		// make sure we know which original block everything points to.
		final List< Tuple2< HashableLongArray, long[] > > blockContainsList = graphsAndBorderNodes._2().collect();
		final TLongObjectHashMap< HashableLongArray > blockContains = new TLongObjectHashMap<>();
		for ( final Tuple2< HashableLongArray, long[] > bcl : blockContainsList )
			for ( final long l : bcl._2() )
				blockContains.put( l, bcl._1() );

		final RegionMergingArrayBased rm = new RegionMergingArrayBased( merger, weightFunc );

		final JavaPairRDD< Long, RegionMergingInput > rmIn = RegionMergingArrayBased.fromBlockDivision( graphs ).cache();

		final long nOriginalBlocks = rmIn.count();

//		final Tuple2< JavaPairRDD< Long, RegionMergingInput >, DisjointSets > msb = mergeSmallBlocks( sc, rmIn, 1 );
//
//		final JavaPairRDD< Long, RegionMergingInput > finalRmIn = msb._1().cache();
//
//		rmIn.unpersist();
//
//		final DisjointSets dj = msb._2();

//		final JavaPairRDD< Long, RegionMergingInput > finalRmIn = rmIn;
//
//		final DisjointSets dj = new DisjointSets( ( int ) finalRmIn.count() );

		final DisjointSets dj = new DisjointSets( ( int ) rmIn.count() );

		final JavaPairRDD< Long, RegionMergingInput > finalRmIn = rmIn;

//		final JavaPairRDD< Long, RegionMergingInput > finalRmIn = MergeBlocks.mergeSmallBlocks( rmIn, dj, 50 ).cache();

//		rmIn.unpersist();

		final JavaPairRDD< Integer, Integer > recordsPerPartition = Util.recordsPerPartition( finalRmIn );
		System.out.println( "finalRmIn records per partition:" );
		System.out.println( recordsPerPartition.collect() );

		final Img< LongType > blockZero = labelsImgFactory.create( labelsTarget, new LongType() );
//		final TLongLongHashMap labelBlockmap = generateLabelBlockMap( graphs );
		final TLongLongHashMap labelBlockmap = generateLabelBlockMapFromRegionMergingInput( finalRmIn, merger.dataSize() );

		for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( labelsTarget, blockZero ), blockZero ) )
			p.getB().set( labelBlockmap.get( p.getA().get() ) );

		final Visitor vis = visFac.create( sc, labelsTarget, blockZero, blockToInitialBlockMapBC, labelBlocks );

		final double tolerance = 1e32;
		final double regionRatio = 0.0;

		System.out.println( "Running region-merging" );
		final JavaPairRDD< Long, RegionMergingInput > graphsAfterMerging = rm.run( sc, finalRmIn, threshold, vis, nOriginalBlocks, tolerance, regionRatio, dj );
		graphsAfterMerging.count();

		sc.close();

		ctx.close();

	}

	public static TLongLongHashMap countByLabel( final RandomAccessibleInterval< LongType > labelsTarget ) throws InterruptedException, ExecutionException
	{
		final long dims[] = Intervals.dimensionsAsLongArray( labelsTarget );
		final int nThreads = Runtime.getRuntime().availableProcessors() - 1;
		final ExecutorService es = Executors.newFixedThreadPool( nThreads );
		final ArrayList< Callable< TLongLongHashMap > > tasks = new ArrayList<>();
		final int lastDim = labelsTarget.numDimensions() - 1;
		final long stepSize = Math.max( labelsTarget.dimension( lastDim ) / nThreads, 1 );
		for ( long z = 0; z < labelsTarget.dimension( lastDim ); z += stepSize )
		{
			final long[] zOffset = new long[ lastDim + 1 ];
			zOffset[ lastDim ] = z;
			final long[] currentDim = dims.clone();
			currentDim[ lastDim ] = Math.min( z + stepSize, dims[ lastDim ] ) - z;
			final IntervalView< LongType > oi = Views.offsetInterval( labelsTarget, zOffset, currentDim );

			tasks.add( () -> Util.countLabels( oi ) );

		}
		final List< Future< TLongLongHashMap > > futures = es.invokeAll( tasks );
		final TLongLongHashMap counts = new TLongLongHashMap();
		for ( final Future< TLongLongHashMap > fut : futures )
			for ( final TLongLongIterator futIt = fut.get().iterator(); futIt.hasNext(); )
			{
				futIt.advance();
				final long l = futIt.key();
				final long count = counts.contains( l ) ? counts.get( l ) + futIt.value() : futIt.value();
				counts.put( l, count );
			}
		System.out.println( "Got counts." );

		es.shutdown();

		return counts;
	}

	public static ArrayList< Tuple2< HashableLongArray, Tuple3< long[], float[], TLongLongHashMap > > > createBlocks(
			final RandomAccessible< FloatType > affs,
			final RandomAccessible< LongType > labelsExtend,
			final long[] dimsNoChannels,
			final long[] dimsIntervalNoChannels,
			final TLongLongHashMap globalCounts )
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

			final TLongLongHashMap counts = new TLongLongHashMap();

			final Cursor< LongType > l = Views.offsetInterval( labelsExtend, lower, extendedBlockSize ).cursor();
			for ( int i = 0; l.hasNext(); ++i )
			{
				final long lbl = l.next().get();
				labelData[ i ] = lbl;
				if ( lbl >= 0 && !counts.contains( lbl ) )
					counts.put( lbl, globalCounts.get( lbl ) );
			}

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

	public static TLongLongHashMap generateLabelBlockMap( final JavaPairRDD< Long, BlockDivision > graphs, final int edgeDataSize )
	{
		final TLongLongHashMap labelBlockmap = new TLongLongHashMap();
		for ( final Tuple2< Long, BlockDivision > g : graphs.collect() )
		{
			final long id = g._1();
			final TLongLongHashMap cons = g._2().outsideNodes;
			final Edge e = new Edge( g._2().edges, edgeDataSize );
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

	public static TLongLongHashMap generateLabelBlockMapFromRegionMergingInput( final JavaPairRDD< Long, RegionMergingInput > rmIn, final int edgeDataSize )
	{
		final TLongLongHashMap labelBlockmap = new TLongLongHashMap();
		for ( final Tuple2< Long, RegionMergingInput > g : rmIn.collect() )
		{
			final long id = g._1();
			final TLongLongHashMap cons = g._2().outsideNodes;
			final Edge e = new Edge( g._2().edges, edgeDataSize );
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

	public static final Tuple2< JavaPairRDD< Long, RegionMergingInput >, DisjointSets > mergeSmallBlocks( final JavaSparkContext sc, final JavaPairRDD< Long, RegionMergingInput > rmIn, final int nNodes, final int edgeDataSize )
	{
		final JavaPairRDD< Long, Tuple2< Long, RegionMergingInput > > smallBlocksMapping = rmIn.mapToPair( t -> {
			final long self = t._1();
			final RegionMergingInput in = t._2();
			final int nInternalNodes = in.nNodes - in.outsideNodes.size();
			final long otherBlock;
			if ( nInternalNodes > nNodes )
				otherBlock = self;
			else
			{
				final Edge e = new Edge( in.edges, edgeDataSize );
				double minWeight = Double.MAX_VALUE;
				long minBlock = self;
				for ( int i = 0; i < e.size(); ++i )
				{
					e.setIndex( i );
					final double w = e.weight();
					if ( in.outsideNodes.contains( e.from() ) && w < minWeight )
					{
						minWeight = w;
						minBlock = in.outsideNodes.get( e.from() );
					}
					else if ( in.outsideNodes.contains( e.to() ) && w < minWeight )
					{
						minWeight = w;
						minBlock = in.outsideNodes.get( e.to() );
					}
				}
				System.out.println( "Eliminating 1 sized block: " + self + " " + minBlock );
				otherBlock = minBlock;
			}
			return new Tuple2<>( self, new Tuple2<>( otherBlock, in ) );
		} ).cache();

		final JavaRDD< Tuple2< Long, Long > > merges = smallBlocksMapping.map( t -> new Tuple2<>( t._1(), t._2()._1() ) );

		final DisjointSets djhm = new DisjointSets( ( int ) rmIn.count() );

		for ( final Tuple2< Long, Long > m : merges.collect() )
		{
			final int r1 = djhm.findRoot( m._1().intValue() );
			final int r2 = djhm.findRoot( m._2().intValue() );
			if ( r1 != r2 )
				djhm.join( r1, r2 );
		}

		final Broadcast< DisjointSets > djhmBC = sc.broadcast( djhm );

		final JavaPairRDD< Long, ArrayList< RemappedData > > aggregated = smallBlocksMapping
				.mapToPair( t -> new Tuple2<>( ( long ) djhmBC.getValue().findRoot( t._1().intValue() ), t._2()._2() ) )
				.mapToPair( t -> new Tuple2<>( t._1(), new RemappedData( t._2().edges, t._2().counts, t._2().outsideNodes, new TLongArrayList(), new TLongLongHashMap() ) ) )
				.aggregateByKey( new ArrayList<>(),
						( v1, v2 ) -> {
							v1.add( v2 );
							return v1;
						},
						( v1, v2 ) -> {
							v1.addAll( v2 );
							return v1;
						} );

		final JavaPairRDD< Long, OriginalLabelData > reduced = aggregated.mapValues( new ReduceBlock( edgeDataSize ) );
		final JavaPairRDD< Long, RegionMergingInput > finalRmIn = reduced.mapToPair( t -> {
			final OriginalLabelData old = t._2();
			final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();
			final TLongIterator cIt = old.counts.keySet().iterator();
			for ( int i = 0; cIt.hasNext(); ++i )
				nodeIndexMapping.put( cIt.next(), i );
			return new Tuple2<>( t._1(), new RegionMergingInput( old.counts.size(), nodeIndexMapping, old.counts, old.outsideNodes, old.edges ) );
		} );

		return new Tuple2<>( finalRmIn, djhm );
	}


}
