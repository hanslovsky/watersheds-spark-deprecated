package de.hanslovsky.watersheds.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingArrayBased.Visitor;
import de.hanslovsky.watersheds.rewrite.util.DisjointSetsHashMap;
import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.IterableWithConstant;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.converter.Converter;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import scala.Tuple2;

public class VisualizationVisitor implements Visitor
{

	private final JavaSparkContext sc;

	private final Broadcast< Map< Long, HashableLongArray > > blockToInitialBlockMapBC;

	private final long[] dimsIntervalNoChannels;

	private final ArrayList< JavaPairRDD< HashableLongArray, long[] > > labelBlocks;

	private final List< RandomAccessibleInterval< LongType > > images;

	private final List< RandomAccessibleInterval< LongType > > blockImages;

	private BdvStackSource< LongType > coloredHistoryBdv;

	private BdvStackSource< LongType > coloredBlockHistoryBdv;

	private final ImgFactory< LongType > factory;

	private final Converter< LongType, ARGBType > conv;

	private final Converter< LongType, ARGBType > blockConv;

	public VisualizationVisitor(
			final JavaSparkContext sc,
			final Broadcast< Map< Long, HashableLongArray > > blockToInitialBlockMapBC,
			final long[] dimsIntervalNoChannels,
			final ArrayList< JavaPairRDD< HashableLongArray, long[] > > labelBlocks,
			final List< RandomAccessibleInterval< LongType > > images,
			final List< RandomAccessibleInterval< LongType > > blockImages,
			final BdvStackSource< LongType > coloredHistoryBdv,
			final BdvStackSource< LongType > coloredBlockHistoryBdv,
			final ImgFactory< LongType > factory )
	{
		super();
		this.sc = sc;
		this.blockToInitialBlockMapBC = blockToInitialBlockMapBC;
		this.dimsIntervalNoChannels = dimsIntervalNoChannels;
		this.labelBlocks = labelBlocks;
		this.images = images;
		this.blockImages = blockImages;
		this.coloredHistoryBdv = coloredHistoryBdv;
		this.coloredBlockHistoryBdv = coloredBlockHistoryBdv;
		this.factory = factory;
		this.conv = ( Converter< LongType, ARGBType > ) coloredHistoryBdv.getBdvHandle().getViewerPanel().getState().getSources().get( 0 ).getConverter();
		this.blockConv = ( Converter< LongType, ARGBType > ) coloredBlockHistoryBdv.getBdvHandle().getViewerPanel().getState().getSources().get( 0 ).getConverter();
	}

	@Override public void visit( final JavaPairRDD< Long, Tuple2< Long, MergeBlocOut > > mergedEdges, final int[] parents )
	{
		final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], parents.length );
		final TLongObjectHashMap< TLongArrayList > rootChildMap = new TLongObjectHashMap<>();

		for ( int i = 0; i < parents.length; ++i )
		{
			final long root = dj.findRoot( i );
			if ( !rootChildMap.contains( root ) )
				rootChildMap.put( root, new TLongArrayList() );
			rootChildMap.get( root ).add( i );
		}

		final Broadcast< TLongObjectHashMap< TLongArrayList > > rcmBC = sc.broadcast( rootChildMap );

		final JavaPairRDD< Long, Tuple2< TLongArrayList, long[] > > mergesAndMapping = mergedEdges.mapToPair( t -> {
			final TLongArrayList m = t._2()._2().merges;
			final long[] map = t._2()._2().indexNodeMapping;
			return new Tuple2<>( t._1(), new Tuple2<>( m, map ) );
		} );

		final Broadcast< Map< Long, HashableLongArray > > blockToInitialBlockMapBC = this.blockToInitialBlockMapBC;

		final JavaPairRDD< HashableLongArray, Tuple2< TLongArrayList, long[] > > mergesForEachBlock = mergesAndMapping
				.flatMapToPair( t -> {
					final TLongArrayList affectedChildren = rcmBC.value().get( t._1() );
					final IterableWithConstant< Long, Tuple2< TLongArrayList, long[] > > iterable =
							new IterableWithConstant<>( Arrays.asList( ArrayUtils.toObject( affectedChildren.toArray() ) ), t._2() );
					return iterable;
				} )
				.mapToPair( t -> new Tuple2<>( blockToInitialBlockMapBC.value().get( t._1() ), t._2() ) );

		final JavaPairRDD< HashableLongArray, ArrayList< Tuple2< TLongArrayList, long[] > > > mergesForEachBlockAggregated = mergesForEachBlock
				.aggregateByKey(
						new ArrayList<>(),
						( al, v ) -> {
							al.add( v );
							return al;
						},
						( al1, al2 ) -> {
							al1.addAll( al2 );
							return al1;
						} );
		final JavaPairRDD< HashableLongArray, long[] > previous = labelBlocks.get( labelBlocks.size() - 1 );
		final JavaPairRDD< HashableLongArray, long[] > current = previous
				.join( mergesForEachBlockAggregated )
				.mapToPair( t -> {
					final long[] dataArray = t._2()._1();
					final ArrayList< Tuple2< TLongArrayList, long[] > > mapping = t._2()._2();

					final DisjointSetsHashMap djBlock = new DisjointSetsHashMap();
					for ( final Tuple2< TLongArrayList, long[] > m : mapping )
					{
						final TLongArrayList m1 = m._1();
						final long[] m2 = m._2();
						for ( int i = 0; i < m1.size(); i += 4 )
						{
							final long r1 = djBlock.findRoot( m2[ ( int ) m1.get( i ) ] );
							final long r2 = djBlock.findRoot( m2[ ( int ) m1.get( i + 1 ) ] );
							if ( r1 != r2 )
								djBlock.join( r1, r2 );
						}
					}

					for ( int i = 0; i < dataArray.length; ++i )
						dataArray[ i ] = djBlock.findRoot( dataArray[ i ] );

					return new Tuple2<>( t._1(), dataArray );
				} );
		labelBlocks.add( current.cache() );

		current.count();

		labelBlocks.remove( 0 ).unpersist();

		final Img< LongType > img = factory.create( images.get( 0 ), new LongType() );
		for ( final Tuple2< HashableLongArray, long[] > currentData : current.collect() )
		{
			final long[] min = currentData._1().getData();
			final ArrayImg< LongType, LongArray > src = ArrayImgs.longs( currentData._2(), dimsIntervalNoChannels );
			final IntervalView< LongType > tgt = Views.offsetInterval( img, min, dimsIntervalNoChannels );
			for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( src, tgt ), new FinalInterval( dimsIntervalNoChannels ) ) )
				p.getB().set( p.getA() );
		}
		images.add( img );
		coloredHistoryBdv = Util.replaceSourceAndReuseConverter( coloredHistoryBdv, Views.stack( images ), conv, Util.bdvOptions( img ) );

		final Img< LongType > blockImg = factory.create( blockImages.get( 0 ), new LongType() );
		for ( final Pair< LongType, LongType > p : Views.interval( Views.pair( blockImages.get( blockImages.size() - 1 ), blockImg ), blockImg ) )
			p.getB().set( dj.findRoot( p.getA().getInteger() ) );
		blockImages.add( blockImg );
		coloredBlockHistoryBdv = Util.replaceSourceAndReuseConverter( coloredBlockHistoryBdv, Views.stack( blockImages ), blockConv, Util.bdvOptions( blockImg ) );
	}

}
