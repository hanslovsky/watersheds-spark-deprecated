package de.hanslovsky.watersheds;

import java.util.Comparator;

import org.apache.spark.api.java.function.PairFunction;

import net.imglib2.Dimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;

public class Util
{
	public static long[] getCurrentChunkDimensions( final long[] offset, final long[] dims, final int[] intervalDims )
	{
		final long[] intervalDimensionsTruncated = new long[ dims.length ];
		for ( int d = 0; d < intervalDimensionsTruncated.length; ++d )
			intervalDimensionsTruncated[ d ] = Math.min(
					dims[ d ] - offset[ d ],
					intervalDims[ d ] );
		return intervalDimensionsTruncated;
	}

	public static long positionToIndex( final HashableLongArray pos, final long[] dim )
	{
		return IntervalIndexer.positionToIndex( pos.getData(), dim );
	}

	public static class KeyAndCountsComparator< V > implements
			Comparator< Tuple2< HashableLongArray, V > >
	{
		private final long[] dims;

		public KeyAndCountsComparator( final long[] dims )
		{
			super();
			this.dims = dims;
		}

		@Override
		public int compare( final Tuple2< HashableLongArray, V > o1, final Tuple2< HashableLongArray, V > o2 )
		{
			return Long.compare( positionToIndex( o1._1(), dims ), positionToIndex( o2._1(), dims ) );
		}
	}

	public static class DropSecondValue< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, K, V1 >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 3499313015214140909L;

		@Override
		public Tuple2< K, V1 > call( final Tuple2< K, Tuple2< V1, V2 > > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t._2()._1() );
		}

	}

	public static < T extends RealType< T >, U extends RealType< U > > RandomAccessibleInterval< RealComposite< U > > prepareAffinities( final RandomAccessibleInterval< T > data, final ImgFactory< U > fac, final U u, final int... perm )
	{
		return Util.prepareAffinities( data, fac, u, ( s, t ) -> {
			t.setReal( s.getRealDouble() );
		}, perm );
	}

	public static < T extends RealType< T >, U extends RealType< U > > RandomAccessibleInterval< RealComposite< U > > prepareAffinities( final RandomAccessibleInterval< T > data, final ImgFactory< U > fac, final U u, final Converter< T, U > conv, final int... perm )
	{
		final Img< U > input = fac.create( data, u );
		for ( final Pair< T, U > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, data.numDimensions() - 1 ), input ), input ) )
			conv.convert( p.getA(), p.getB() );
	
		final CompositeIntervalView< U, RealComposite< U > > affs = Views.collapseReal( input );
		return affs;
	}

	public static int[] getFlipPermutation( final int numDimensions )
	{
		final int[] perm = new int[ numDimensions ];
		for ( int d = 0, flip = numDimensions - 1; d < numDimensions; ++d, --flip )
			perm[ d ] = flip;
		return perm;
	}

	public static int[] getStride( final Dimensions dim )
	{
		final int[] stride = new int[ dim.numDimensions() ];
		stride[ 0 ] = 1;
		for ( int d = 1; d < stride.length; ++d )
			stride[ d ] = stride[ d - 1 ] * ( int ) dim.dimension( d - 1 );
		return stride;
	}
}
