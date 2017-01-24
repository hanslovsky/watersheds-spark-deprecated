package de.hanslovsky.watersheds.rewrite.preparation;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import bdv.img.h5.H5Utils;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.list.array.TLongArrayList;
import ij.ImageJ;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.affinity.AffinityView;
import net.imglib2.algorithm.morphology.watershed.affinity.CompositeFactory;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class AffinityWatershedsBlockedDebug
{

	public static Converter< LongType, LongType > relevantBitsConverter( final long relevantBits )
	{
		return ( s, t ) -> {
			t.set( s.get() & relevantBits );
		};
	}

	public static RandomAccessibleInterval< LongType > showOnlyRelevantBits( final RandomAccessibleInterval< LongType > rai, final long relevantBits )
	{
		return Converters.convert( rai, relevantBitsConverter( relevantBits ), new LongType() );
	}

	public static void main( final String[] args ) throws InterruptedException, ExecutionException
	{
		final int[] cellSize = new int[] { 5, 3, 2 };

		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5-2D-mini.h5";

		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data =
				H5Utils.loadFloat( path, "main", cellSize );

		final long[] dims = Intervals.dimensionsAsLongArray( data );
		final long[] dimsNoChannels = Util.dropLast( dims );
		final long inputSize = Intervals.numElements( data );

		System.out.println( "Loaded data (" + inputSize + ")" );

		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
//		final CellImg< FloatType, ?, ? > input = new CellImgFactory< FloatType >( dimsIntervalInt ).create( dims, new FloatType() );
		final ArrayImg< FloatType, FloatArray > input = ArrayImgs.floats( dims );
		for ( final Pair< FloatType, FloatType > p : Views.interval( Views.pair( Views.permuteCoordinates( data, perm, data.numDimensions() - 1 ), input ), input ) )
			p.getB().set( p.getA().getRealFloat() );

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs = Views.collapseReal( input );
		for ( int d = 0; d < affs.numDimensions(); ++d )
			for ( final RealComposite< FloatType > a : Views.hyperSlice( affs, d, affs.max( d ) ) )
				a.get( d ).set( Float.NaN );

		final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( dimsNoChannels );
		final CompositeFactory< FloatType, RealComposite< FloatType > > factory = size -> Views.collapseReal( ArrayImgs.floats( 1, size ) ).randomAccess().get();
		final RealComposite< FloatType > extension = Views.collapseReal( ArrayImgs.floats( new float[] { Float.NaN, Float.NaN }, 1, 2 ) ).randomAccess().get();
		final AffinityView< FloatType, RealComposite< FloatType > > bidirectionalAffs = new AffinityView<>( Views.extendValue( affs, extension ), factory );

		final long[] dir = AffinityWatershedBlocked.generateDirectionBitmask( labels.numDimensions() );
		final long[] invDir = AffinityWatershedBlocked.generateInverseDirectionBitmask( dir );
		final Long relevantBits = Long.parseLong( new String( new char[ dir.length ] ).replace( "\0", "1" ), 2 );
		System.out.println( Arrays.toString( dir ) );
		System.out.println( Arrays.toString( invDir ) );

		final ArrayImg< LongType, LongArray > parentsGroundTruth = ArrayImgs.longs( new long[] {
				dir[ 2 ], dir[ 2 ], dir[ 2 ] | dir[ 3 ] | dir[ 1 ], dir[ 1 ] | dir[ 3 ], dir[ 3 ],
				dir[ 0 ], dir[ 2 ], dir[ 2 ] | dir[ 3 ] | dir[ 0 ], dir[ 1 ] | dir[ 3 ], dir[ 0 ] | dir[ 3 ],
				dir[ 0 ], dir[ 0 ], dir[ 0 ] | dir[ 2 ], dir[ 0 ] | dir[ 1 ], dir[ 0 ]

		}, dimsNoChannels );

		for ( final int[] m : AffinityWatershedBlocked.generateMoves( labels.numDimensions() ) )
			System.out.println( "m: " + Arrays.toString( m ) );

		new ImageJ();
		ImageJFunctions.show( parentsGroundTruth );

		System.out.println( Arrays.toString( Intervals.dimensionsAsLongArray( labels ) ) + " " + Arrays.toString( Intervals.dimensionsAsLongArray( affs ) ) );

		AffinityWatershedBlocked.findParents( bidirectionalAffs, labels, ( t1, t2 ) -> t1.get() > t2.get(), new FloatType( Float.NEGATIVE_INFINITY ), dir, new FloatType( Float.NEGATIVE_INFINITY ), new FloatType( 1.0f ), Executors.newFixedThreadPool( 1 ), 1 );

		ImageJFunctions.show( showOnlyRelevantBits( labels, relevantBits ) );

		final long highBit = 1l << 63;
		final long secondHighBit = 1l << 62;

		final long[] steps = AffinityWatershedBlocked.generateSteps( AffinityWatershedBlocked.generateStride( labels ) );
		final TLongArrayList plateauCorners = AffinityWatershedBlocked.findPlateauCorners( labels, steps, dir, invDir, secondHighBit, Executors.newFixedThreadPool( 1 ), 1 );
		System.out.println( plateauCorners );
		AffinityWatershedBlocked.removePlateaus( plateauCorners, labels, steps, dir, invDir, highBit, secondHighBit );
		ImageJFunctions.show( showOnlyRelevantBits( labels, Long.parseLong( new String( new char[ dir.length * 2 ] ).replace( "\0", "1" ), 2 ) ), "After removing plateaus" );

		final ArrayImg< LongType, LongArray > labels2 = ArrayImgs.longs( dimsNoChannels );

		final CompareBetter< FloatType > compare = ( f, s ) -> f.get() > s.get();
		AffinityWatershedBlocked.letItRain( bidirectionalAffs, labels2, compare, new FloatType( Float.NEGATIVE_INFINITY ), new FloatType( Float.NEGATIVE_INFINITY ), new FloatType( 1.0f ), Executors.newFixedThreadPool( 1 ), 1, () -> {} );
		System.out.println( "Did the watershed!" );
		ImageJFunctions.show( labels2 );

	}
}
