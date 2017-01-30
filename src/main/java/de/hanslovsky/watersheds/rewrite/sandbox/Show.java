package de.hanslovsky.watersheds.rewrite.sandbox;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.rewrite.util.IntensityMouseOver;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;

public class Show
{

	public static void main( final String[] args )
	{

		final String path = Util.HOME_DIR + "/Dropbox/misc/excerpt-sliced-blocks.h5";
		final int nIter = 8;

		final ArrayList< RandomAccessibleInterval< LongType > > labels = new ArrayList<>();
		final ArrayList< RandomAccessibleInterval< LongType > > blocks = new ArrayList<>();

		for ( int i = 0; i < nIter; ++i )
		{
			labels.add( H5Utils.loadUnsignedLong( path, "labels-" + i, new int[] { 300, 300, 100 } ) );
			blocks.add( H5Utils.loadUnsignedLong( path, "blocks-" + i, new int[] { 300, 300, 100 } ) );
		}

		final TLongIntHashMap labelsCmap = new TLongIntHashMap();
		final TLongIntHashMap blocksCmap = new TLongIntHashMap();

		{
			final Random rng = new Random( 100 );
			for ( final LongType l : Views.flatIterable( labels.get( 0 ) ) )
				if ( !labelsCmap.contains( l.get() ) )
					labelsCmap.put( l.get(), rng.nextInt() );
		}

		{
			final Random rng = new Random( 100 );
			for ( final LongType b : Views.flatIterable( blocks.get( 0 ) ) )
				if ( !blocksCmap.contains( b.get() ) )
					blocksCmap.put( b.get(), rng.nextInt() );
		}

		final BdvStackSource< LongType > labelsBdv = BdvFunctions.show( Views.stack( labels ), "labels", Util.bdvOptions( labels.get( 0 ) ) );
		final BdvStackSource< LongType > blocksBdv = BdvFunctions.show( Views.stack( blocks ), "blocks", Util.bdvOptions( blocks.get( 0 ) ) );

		Util.replaceConverter( labelsBdv, 0, ( s, t ) -> {
			t.set( labelsCmap.get( s.get() ) );
		} );

		Util.replaceConverter( blocksBdv, 0, ( s, t ) -> {
			t.set( blocksCmap.get( s.get() ) );
		} );

		new IntensityMouseOver( labelsBdv.getBdvHandle().getViewerPanel() );
		new IntensityMouseOver( blocksBdv.getBdvHandle().getViewerPanel() );

		final int[] cellSize = new int[] { 50, 50, 50, 3 };
		System.out.println( "Loading data" );
		final CellImg< FloatType, ?, ? > data =
				H5Utils.loadFloat( path, "main", cellSize );
		final int[] perm = Util.getFlipPermutation( data.numDimensions() - 1 );
		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affs =
				Views.collapseReal( Views.permuteCoordinates( data, perm, data.numDimensions() - 1 ) );

		final RandomAccessibleInterval< FloatType > maxAff = Converters.convert( affs, ( s, t ) -> {
			Util.max( s, t, perm.length );
//			t.set( t.get() > 0.9f ? 1.0f : 0.0f );
			t.set( 1 - t.get() );
			t.mul( 1 << 16 );
		}, new FloatType() );

		BdvFunctions.show( maxAff, "maxAff" );

		BdvFunctions.show( Views.stack( Collections.nCopies( nIter, maxAff ) ), "maxAff", BdvOptions.options().addTo( labelsBdv ) );
//		BdvFunctions.show( Views.stack( Collections.nCopies( nIter, maxAff ) ), "maxAff", BdvOptions.options().addTo( blocksBdv ) );

//		Util.replaceConverter( labelsBdv, 1, ( s, t ) -> {
//			final int gray = ( int ) ( s.get() * 255 );
//			t.set( 60 << 24 | gray << 16 | gray << 8 | gray << 0 );
//		} );
	}

}