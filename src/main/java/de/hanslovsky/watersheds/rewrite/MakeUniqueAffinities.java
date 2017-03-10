package de.hanslovsky.watersheds.rewrite;

import java.util.stream.IntStream;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.rewrite.util.Util;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import net.mintern.primitive.Primitive;
import net.mintern.primitive.comparators.IntComparator;

public class MakeUniqueAffinities
{

	public static void main( final String[] args )
	{
		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt.h5";
//		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";
		final int[] cd = new int[] { 300, 300, 100, 3 };
		final CellImg< FloatType, ?, ? > input = H5Utils.loadFloat( path, "main", cd );

		final float[] affinities = new float[ ( int ) Intervals.numElements( input ) ];
		{
			int i = 0;
			for ( final FloatType aff : Views.flatIterable( input ) )
			{
				affinities[ i ] = aff.get();
				++i;
			}
		}

		final int[] indices = IntStream.range( 0, affinities.length ).toArray();

		final IntComparator comp = ( i1, i2 ) -> Float.compare( affinities[i1], affinities[i2] );

		Primitive.sort( indices, comp );


		final float[] outputData = new float[ affinities.length ];
		final ArrayImg< FloatType, FloatArray > output = ArrayImgs.floats( outputData, Intervals.dimensionsAsLongArray( input ) );

		final float startAff = 0f;
		final FloatType startType = new FloatType( startAff );
		float uniqueAffinity = startAff;
		for ( int i = 0; i < indices.length; ++i, uniqueAffinity = Float.intBitsToFloat( Float.floatToRawIntBits( uniqueAffinity ) + 1 ) )
			outputData[ indices[ i ] ] = uniqueAffinity;
		System.out.println( uniqueAffinity );

		final Converter< FloatType, FloatType > inputConv = ( s, t ) -> {
			t.set( s );
			t.mul( 1 << 16 );
		};

		final double norm = 1.0 / ( uniqueAffinity - startAff );
		final Converter< FloatType, FloatType > outputConv = ( s, t ) -> {
			t.set( s );
			t.sub( startType );
			t.mul( norm );
			t.mul( 1 << 16 );
		};

		final BdvStackSource< FloatType > inputBdv = BdvFunctions.show( Converters.convert( ( RandomAccessibleInterval< FloatType > ) Views.hyperSlice( input, input.numDimensions() - 1, 0 ), inputConv, new FloatType() ), "input 0", Util.bdvOptions( Views.collapse( input ) ) );
		final BdvStackSource< FloatType > outputBdv = BdvFunctions.show( Converters.convert( ( RandomAccessibleInterval< FloatType > ) Views.hyperSlice( output, input.numDimensions() - 1, 0 ), outputConv, new FloatType() ), "output 0", Util.bdvOptions( Views.collapse( output ) ) );

		for ( int i = 1; i < input.numDimensions() - 1; ++i )
		{
			BdvFunctions.show( Converters.convert( ( RandomAccessibleInterval< FloatType > ) Views.hyperSlice( input, input.numDimensions() - 1, i ), inputConv, new FloatType() ), "input " + i, BdvOptions.options().addTo( inputBdv ) );

			BdvFunctions.show( Converters.convert( ( RandomAccessibleInterval< FloatType > ) Views.hyperSlice( output, input.numDimensions() - 1, i ), outputConv, new FloatType() ), "output " + i, BdvOptions.options().addTo( outputBdv ) );
		}

//		H5Utils.saveFloat( output, path, "unique", cd );


	}

}
