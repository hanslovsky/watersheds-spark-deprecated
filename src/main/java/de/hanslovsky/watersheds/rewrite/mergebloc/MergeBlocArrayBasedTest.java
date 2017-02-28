package de.hanslovsky.watersheds.rewrite.mergebloc;

import java.util.Random;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.Util;
import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight.FunkyWeight;
import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class MergeBlocArrayBasedTest
{

	public static void main( final String[] args ) throws Exception
	{
		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt-60x60x20-blocks.h5";
//		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt2D.h5";
//		final int[] cellSize = new int[] { 150, 150, 50, 3 };
//		final int[] cellSize = new int[] { 300, 300, 2 };
		final int[] cellSize = new int[] { 300, 300, 150, 3 };
		final int[] cellSizeLabels = Util.dropLast( cellSize );
		System.out.println( "Loading data" );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > data = Views.collapseReal( H5Utils.loadFloat( path, "main", cellSize ) );
		final Img< LongType > labels = H5Utils.loadUnsignedLong( path, "zws", cellSizeLabels );

		final TLongArrayList indexNodeMappingList = new TLongArrayList();
		final TLongIntHashMap nodeIndexMapping = new TLongIntHashMap();
		for ( final LongType l : labels )
		{
			final long lb = l.get();
			if ( !nodeIndexMapping.contains( lb ) )
			{
				nodeIndexMapping.put( lb, indexNodeMappingList.size() );
				indexNodeMappingList.add( lb );
			}
		}
		final long[] indexNodeMapping = indexNodeMappingList.toArray();
		final int nNodes = indexNodeMapping.length;


		final long[] counts = new long[ nNodes ];


		final DisjointSets dj = new DisjointSets( nNodes );
		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );

		for ( final LongType l : labels )
		{
			final long lb = l.get();
			final int idx = nodeIndexMapping.get( lb );
			++counts[ idx ];

			if ( !cmap.contains( lb ) )
				cmap.put( lb, rng.nextInt() );
		}

		final UndirectedGraphArrayBased g = new UndirectedGraphArrayBased( nNodes, new EdgeMerger.MAX_AFFINITY_MERGER() );
		final TDoubleArrayList edges = g.edges();
		final TIntIntHashMap[] nodeEdgeMap = g.nodeEdgeMap();
		final Edge edge = new Edge( edges );

		final RandomAccessible< Pair< RealComposite< FloatType >, LongType > > paired = Views.pair( data, labels );

		for ( int d = 0; d < paired.numDimensions(); ++d ) {
			final long[] max = new long[ paired.numDimensions() ];
			labels.max( max );
			max[d] -= 1;
			final IntervalView< Pair< RealComposite< FloatType >, LongType > > interval = Views.interval( paired, new long[ max.length ], max );
			final Cursor< Pair< RealComposite< FloatType >, LongType > > c = interval.cursor();
			final RandomAccess< Pair< RealComposite< FloatType >, LongType > > ra = interval.randomAccess();
			while ( c.hasNext() )
			{
				final Pair< RealComposite< FloatType >, LongType > p = c.next();
				ra.setPosition( c );
				ra.fwd( d );
				final RealComposite< FloatType > a = p.getA();
				final LongType l = p.getB();

				final int from = nodeIndexMapping.get( l.get() );
				final int to = nodeIndexMapping.get( ra.get().getB().get() );
				final float aff = a.get( d ).get();
				if ( !Float.isNaN( aff ) && from != to )
				{

					final TIntIntHashMap fMap = nodeEdgeMap[ from ];
					final TIntIntHashMap tMap = nodeEdgeMap[ to ];

					if ( fMap.contains( to ) && tMap.contains( from ) )
					{
						edge.setIndex( fMap.get( to ) );
						edge.affinity( Math.max( edge.affinity(), aff ) );
						edge.multiplicity( edge.multiplicity() + 1 );
					}
					else
					{
						final int index = edge.add( Double.NaN, aff, from, to, 1 );
						fMap.put( to, index );
						tMap.put( from, index );

					}
				}

			}
		}

		final FunkyWeight fw = new EdgeWeight.FunkyWeight();
		for ( int i = 0; i < edge.size(); ++i )
		{
			edge.setIndex( i );
			edge.weight( fw.weight( edge.affinity(), counts[ ( int ) edge.from() ], counts[ ( int ) edge.to() ] ) );
		}


		final MergeBlocArrayBased mb = new MergeBlocArrayBased( new EdgeMerger.MAX_AFFINITY_MERGER(), fw, 200.0, 0.0 );
		System.out.println( "Start edge merging" );
		final long t0 = System.currentTimeMillis();
		final MergeBlocOut out = mb.call(
				new Tuple2<>( 2l, new MergeBlocIn( g, counts, new TIntLongHashMap(), new long[ 0 ] ) ) )._2()._2();
		final long t1 = System.currentTimeMillis();
		System.out.println( "Done Edge merging: " + ( t1 - t0 ) + "ms" );

		final RandomAccessibleInterval< ARGBType > coloredStart = toColor( labels, cmap );

		final RandomAccessibleInterval< LongType > mapped = Converters.convert( ( RandomAccessibleInterval< LongType > ) labels, ( s, t ) -> {
			t.set( indexNodeMapping[ out.dj.findRoot( nodeIndexMapping.get( s.get() ) ) ] );
		}, new LongType() );

		final RandomAccessibleInterval< ARGBType > coloredMapped = toColor( mapped, cmap );

		final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredStart, "colored init" );
		BdvFunctions.show( coloredMapped, "colored mapped", BdvOptions.options().addTo( bdv ) );


	}

	public static RandomAccessibleInterval< ARGBType > toColor( final RandomAccessibleInterval< LongType > rai, final TLongIntHashMap cmap )
	{
		return Converters.convert( rai, ( s, t ) -> {
			t.set( cmap.get( s.get() ) );
		}, new ARGBType() );
	}

	public static RandomAccessibleInterval< IntType > indexBased( final RandomAccessibleInterval< LongType > rai, final TLongIntHashMap nodeIndexMapping )
	{
		return Converters.convert( rai, ( s, t ) -> {
			t.set( nodeIndexMapping.get( s.get() ) );
		}, new IntType() );
	}


}
