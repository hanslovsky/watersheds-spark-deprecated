package de.hanslovsky.watersheds.rewrite.sandbox;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import bdv.tools.brightness.ConverterSetup;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.viewer.SourceAndConverter;
import bdv.viewer.ViewerPanel;
import bdv.viewer.state.ViewerState;
import de.hanslovsky.watersheds.rewrite.util.IntensityMouseOver;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayCursor;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class Dummy
{
	public static void main( final String[] args )
	{

		final ArrayImg< LongType, LongArray > img = ArrayImgs.longs( 80, 60 );
		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rng = new Random( 100 );

		for ( final ArrayCursor< LongType > c = img.cursor(); c.hasNext(); )
		{
			c.next().set( c.getLongPosition( 0 ) + 1 );
			if ( !cmap.contains( c.getLongPosition( 0 ) + 1 ) )
				cmap.put( c.getLongPosition( 0 ) + 1, rng.nextInt() );
		}

		final IntervalView< LongType > img2 = Views.offset( Views.invertAxis( img, 0 ), -79, 0 );

		final BdvStackSource< LongType > bdv = BdvFunctions.show( img, "blub" );
		final ViewerPanel viewer = bdv.getBdvHandle().getViewerPanel();
		System.out.println( bdv.getSources().size() );
		final SourceAndConverter< LongType > zero = bdv.getSources().get( 0 );
		viewer.removeSource( zero.getSpimSource() );
		System.out.println( bdv.getSources().size() );
		viewer.addSource( new SourceAndConverter<>( zero.getSpimSource(), ( s, t ) -> {
			t.set( cmap.get( s.get() ) );
		} ) );
		System.out.println( bdv.getSources().size() );

		new IntensityMouseOver( viewer );

		final ArrayList< RandomAccessibleInterval< ARGBType > > al = new ArrayList<>();
		al.add( Util.toColor( img, cmap ) );
		final BdvStackSource< ARGBType > bdv2 = BdvFunctions.show( Views.stack( al ), "bleb", BdvOptions.options().is2D() );

		al.add( Util.toColor( img2, cmap ) );

		final ViewerPanel vp = bdv2.getBdvHandle().getViewerPanel();
		final ViewerState state = vp.getState();

		BdvFunctions.show( Util.toColor( img2, cmap ), "BLICK" );

		BdvFunctions.show( Util.toColor( img, cmap ), "BLICK" );

		System.out.println( state.getSources().size() + " " + bdv2.getSources().size() + " " + state.numSources() );
//		state.removeSource( state.getSources().get( state.getCurrentSource() ).getSpimSource() );
//		System.out.println( state.getSources().size() + " " + bdv2.getSources().size() + " " + state.numSources() );
//		setup = new RealARGBColorConverterSetup( bdv2.getBdvHandle().getUn, converter );
//		bdv2.removeFromBdv();
//
//		if ( converterSetups != null )
//			for ( final ConverterSetup setup : converterSetups )
//				setupAssignments.removeSetup( setup );
//
//		if ( transformListeners != null )
//			for ( final TransformListener< AffineTransform3D > l : transformListeners )
//				viewer.removeTransformListener( l );
//
//		if ( timepointListeners != null )
//			for ( final TimePointListener l : timepointListeners )
//				viewer.removeTimePointListener( l );
//
//		if ( visibilityUpdateListeners != null )
//			for ( final UpdateListener l : visibilityUpdateListeners )
//				viewer.getVisibilityAndGrouping().removeUpdateListener( l );
//
//		if ( overlays != null )
//			for ( final OverlayRenderer o : overlays )
//				viewer.getDisplay().removeOverlayRenderer( o );
//
//		if ( sources != null )
//			for ( final SourceAndConverter< ? > soc : sources )
//				viewer.removeSource( soc.getSpimSource() );

//		for ( final SourceAndConverter< ARGBType > source : bdv2.getSources() )
		vp.removeSource( bdv2.getSources().get( 0 ).getSpimSource() );

		final List< ConverterSetup > converterSetups = new ArrayList<>( bdv2.getBdvHandle().getSetupAssignments().getConverterSetups() );
		for ( final ConverterSetup setup : converterSetups )
			bdv2.getBdvHandle().getSetupAssignments().removeSetup( setup );

		vp.requestRepaint();


		Util.replaceConverter( BdvFunctions.show( Views.stack( al ), "blib", BdvOptions.options().addTo( bdv2 ).is2D() ), 0, ( s, t ) -> {
			t.setOne();
		} );

		System.out.println( state.getSources().size() + " " + bdv2.getSources().size() + " " + state.numSources() );


		bdv2.getBdvHandle().getViewerPanel().requestRepaint();


	}
}
