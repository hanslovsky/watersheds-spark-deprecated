package de.hanslovsky.watersheds;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;

import bdv.viewer.ViewerPanel;
import net.imglib2.RealRandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.type.Type;
import net.imglib2.ui.OverlayRenderer;

public class ValueDisplayListener< T extends Type< T > > implements MouseMotionListener, OverlayRenderer
{

	public static interface ToString< T >
	{
		String toString( T t );
	}

	private final RealRandomAccess< T > access;

	private final ViewerPanel viewer;

	private int width = 0;

	private int height = 0;

	private T val = null;

	private final ToString< T > toString;

	public ValueDisplayListener( final RealRandomAccess< T > access, final ViewerPanel viewer, final ToString< T > toString )
	{
		super();
		this.access = access;
		this.viewer = viewer;
		this.toString = toString;
	}

	public ValueDisplayListener( final RealRandomAccess< T > access, final ViewerPanel viewer )
	{
		this( access, viewer, ( t ) -> t.toString() );
	}

	@Override
	public void mouseDragged( final MouseEvent e )
	{}

	@Override
	public void mouseMoved( final MouseEvent e )
	{
		final int x = e.getX();
		final int y = e.getY();

		this.val = getVal( x, y, access, viewer );
		viewer.getDisplay().repaint();
	}

	@Override
	public void drawOverlays( final Graphics g )
	{
		if ( val != null )
		{
			final Graphics2D g2d = ( Graphics2D ) g;

			g2d.setRenderingHint( RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON );
			g2d.setComposite( AlphaComposite.SrcOver );

			final int w = 176;
			final int h = 11;

			final int top = height - h;
			final int left = width - w;

			g2d.setColor( Color.white );
			g2d.fillRect( left, top, w, h );
			g2d.setColor( Color.BLACK );
			final String string = toString.toString( val );
			g2d.drawString( string, left + 1, top + h - 1 );
//				drawBox( "selection", g2d, top, left, fid );
		}
	}

	@Override
	public void setCanvasSize( final int width, final int height )
	{
		this.width = width;
		this.height = height;
	}

	public static < T extends Type< T > > void addValueOverlay( final RealRandomAccessible< T > accessible, final ViewerPanel panel, final ToString< T > toString )
	{
		final ValueDisplayListener< T > vdl = new ValueDisplayListener<>( accessible.realRandomAccess(), panel, toString );
		panel.getDisplay().addOverlayRenderer( vdl );
		panel.getDisplay().addMouseMotionListener( vdl );
	}

	public static < T extends Type< T > > void addValueOverlay( final RealRandomAccessible< T > accessible, final ViewerPanel panel )
	{
		addValueOverlay( accessible, panel, ( t ) -> t.toString() );
	}

	public static < T extends Type< T > > T getVal( final int x, final int y, final RealRandomAccess< T > access, final ViewerPanel viewer )
	{
		access.setPosition( x, 0 );
		access.setPosition( y, 1 );
		access.setPosition( 0, 2 );

		viewer.displayToGlobalCoordinates( access );

		return access.get();
	}

}