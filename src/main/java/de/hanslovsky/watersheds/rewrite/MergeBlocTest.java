package de.hanslovsky.watersheds.rewrite;

import java.util.Random;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.Util;
import de.hanslovsky.watersheds.rewrite.EdgeWeight.FunkyWeight;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Pair;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class MergeBlocTest
{

	public static void main( final String[] args ) throws Exception
	{
		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt2D.h5";
		final int[] cellSize = new int[] { 300, 300, 2 };
		final int[] cellSizeLabels = Util.dropLast( cellSize );
		System.out.println( "Loading data" );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > data = Views.collapseReal( H5Utils.loadFloat( path, "main", cellSize ) );
		final Img< LongType > labels = H5Utils.loadUnsignedLong( path, "zws", cellSizeLabels );

		final UndirectedGraph g = new UndirectedGraph();
		final TDoubleArrayList edges = g.edges();
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = g.nodeEdgeMap();
		final Edge edge = new Edge( edges );

		final TLongLongHashMap counts = new TLongLongHashMap();


		final DisjointSetsHashMap dj = new DisjointSetsHashMap();
		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );

		for ( final LongType l : labels )
		{
			final long lb = l.get();
			if ( !counts.contains( lb ) )
				counts.put( lb, 1 );
			else
				counts.put( lb, counts.get( lb ) + 1 );

			if ( !cmap.contains( lb ) )
				cmap.put( lb, rng.nextInt() );

		}

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

				final long from = l.get();
				final long to = ra.get().getB().get();
				final float aff = a.get( d ).get();
				if ( !Float.isNaN( aff ) && from != to )
				{
					if ( !nodeEdgeMap.contains( from ) )
						nodeEdgeMap.put( from, new TLongIntHashMap() );
					if ( !nodeEdgeMap.contains( to ) )
						nodeEdgeMap.put( to, new TLongIntHashMap() );

					final TLongIntHashMap fMap = nodeEdgeMap.get( from );
					final TLongIntHashMap tMap = nodeEdgeMap.get( to );

					if ( fMap.contains( from ) && tMap.contains( to ) )
					{
						edge.setIndex( fMap.get( from ) );
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
			edge.weight( fw.weight( edge.affinity(), counts.get( edge.from() ), edge.to() ) );
		}

		final TLongArrayList merges = new TLongArrayList();
		final MergerService ms = new MergerService()
		{

			@Override
			public void addMerge( final long n1, final long n2, final long n, final double w )
			{
				merges.add( n1 );
				merges.add( n2 );
			}

			@Override
			public void finalize()
			{

			}
		};
		final MergeBloc mb = new MergeBloc( new EdgeMerger.MAX_AFFINITY_MERGER(), fw, ms, 10 );
		mb.call( new Tuple2<>( 2l, new MergeBloc.MergeBlocData( g, counts ) ) );
		System.out.println( merges.size() );

		for ( int i = 0; i < merges.size(); i += 2 )
			dj.join( dj.findRoot( merges.get(i ) ), dj.findRoot( merges.get( i+1 ) ) );

		final RandomAccessibleInterval< ARGBType > coloredStart = toColor( labels, cmap );

		final RandomAccessibleInterval< LongType > mapped = Converters.convert( ( RandomAccessibleInterval< LongType > ) labels, ( s, t ) -> {
			t.set( dj.findRoot( s.get() ) );
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


}
