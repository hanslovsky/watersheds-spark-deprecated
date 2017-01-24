package de.hanslovsky.watersheds.rewrite.sandbox;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import bdv.img.h5.H5Utils;
import bdv.util.BdvFunctions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.rewrite.util.IntensityMouseOver;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellImg;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.view.Views;

public class Dummy2
{
	public static void main( final String[] args )
	{


		final String path = Util.HOME_DIR + "/local/affinities/tstvol-520-2-h5.h5";

		final int[] threshes = new int[] { 0, 100, 200 };

		final List< RandomAccessibleInterval< LongType > > sources = Arrays.stream( threshes ).mapToObj( t -> H5Utils.loadUnsignedLong( path, "" + t, new int[] { 432, 432, 432 } ) ).collect( Collectors.toList() );

		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );
		for ( final LongType s : Views.flatIterable( sources.get( 0 ) ) )
			if (!cmap.contains( s.get() ) )
				cmap.put( s.get(), rng.nextInt() );

		final BdvStackSource< LongType > bdv = BdvFunctions.show( Views.stack( sources ), "segs" );
		new IntensityMouseOver( bdv.getBdvHandle().getViewerPanel() );
		Util.replaceConverter( bdv, 0, ( s, t ) -> {
			t.set( cmap.get( s.get() ) );
		} );

		final CellImg< LongType, ?, ? > myData = H5Utils.loadUnsignedLong( path, "zws", new int[] { 432, 432, 432 } );
		final TLongIntHashMap myCmap = new TLongIntHashMap();
		final Random myRng = new Random( 100 );
		for ( final LongType d : myData )
			if (!myCmap.contains( d.get() ) )
				myCmap.put( d.get(), myRng.nextInt() );

		final BdvStackSource< LongType > myBdv = BdvFunctions.show( myData, "my data" );
		new IntensityMouseOver( myBdv.getBdvHandle().getViewerPanel() );
		Util.replaceConverter( myBdv, 0, ( s, t ) -> {
			t.set( myCmap.get( s.get() ) );
		} );
	}

}
