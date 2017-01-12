package de.hanslovsky.watersheds.rewrite.util;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;
import java.io.File;

import bdv.BigDataViewer;
import bdv.export.ProgressWriterConsole;
import bdv.viewer.Interpolation;
import bdv.viewer.Source;
import bdv.viewer.ViewerOptions;
import bdv.viewer.ViewerPanel;
import bdv.viewer.state.SourceState;
import bdv.viewer.state.ViewerState;
import net.imglib2.RealLocalizable;
import net.imglib2.RealPoint;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.numeric.RealType;
import net.imglib2.ui.OverlayRenderer;

/**
 *
 * I (Philipp Hanslovsky) copied from
 * https://gist.github.com/tpietzsch/04f75f15574bdcbfaede6b0272ec8883
 * 
 * @author Tobias Pietzsch
 *
 *
 */

public class IntensityMouseOver
{
	public static void main( final String[] args )
	{
		final String fn = "src/main/resources/openconnectome-bock11-neariso.xml";
		try
		{
			System.setProperty( "apple.laf.useScreenMenuBar", "true" );
			final BigDataViewer bdv = BigDataViewer.open(
					fn,
					new File( fn ).getName(),
					new ProgressWriterConsole(),
					ViewerOptions.options() );
			new IntensityMouseOver( bdv.getViewer() );
		}
		catch ( final Exception e )
		{
			e.printStackTrace();
		}
	}

	private final ViewerPanel viewer;

	private volatile double intensity;

	public IntensityMouseOver( final ViewerPanel viewer )
	{
		this.viewer = viewer;
		viewer.getDisplay().addHandler( new MouseOver() );
		viewer.getDisplay().addOverlayRenderer( new Overlay() );
	}

	private void printValueUnderMouse()
	{
		final RealPoint pos = new RealPoint( 3 );
		viewer.getGlobalMouseCoordinates( pos );
		final ViewerState state = viewer.getState();

		final int currentSourceIndex = state.getCurrentSource();
		final SourceState< ? > sourceState = state.getSources().get( currentSourceIndex );
		final Source< ? > spimSource = sourceState.getSpimSource();
		typedPrintValueUnderMouse( spimSource, state, pos );
	}

	private < T > void typedPrintValueUnderMouse( final Source< T > source, final ViewerState state, final RealLocalizable pos )
	{
		final int t = state.getCurrentTimepoint();
		final AffineTransform3D transform = new AffineTransform3D();
		source.getSourceTransform( t, 0, transform );
		final RealRandomAccessible< T > interpolated = source.getInterpolatedSource( t, 0, Interpolation.NEARESTNEIGHBOR );
		final RealRandomAccessible< T > transformed = RealViews.affineReal( interpolated, transform );
		final RealRandomAccess< T > access = transformed.realRandomAccess();

		access.setPosition( pos );
		final T type = access.get();
		if ( type instanceof RealType )
			intensity = ( ( RealType< ? > ) type ).getRealDouble();
		else
			intensity = -1;
	}

	private class MouseOver implements MouseMotionListener
	{
		@Override
		public void mouseDragged( final MouseEvent arg0 )
		{
			printValueUnderMouse();
		}

		@Override
		public void mouseMoved( final MouseEvent arg0 )
		{
			printValueUnderMouse();
		}
	}

	private class Overlay implements OverlayRenderer
	{
		@Override
		public void drawOverlays( final Graphics g )
		{
			final String value = String.format( "intensity = %6.3f", intensity );
			g.setFont( new Font( "Monospaced", Font.PLAIN, 12 ) );
			g.setColor( Color.red );
			g.drawString( value, ( int ) g.getClipBounds().getWidth() / 2, 38 );
		}

		@Override
		public void setCanvasSize( final int width, final int height )
		{}
	}
}
