package de.hanslovsky.watersheds;

import java.util.Random;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.viewer.ViewerPanel;
import gnu.trove.map.hash.TLongIntHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import scala.Tuple3;

public class ShowTopLeftVisitor implements LabelsVisitor
{

	/**
	 *
	 */
	private static final long serialVersionUID = -3919117239750420905L;

	@Override
	public void act( final Tuple3< Long, Long, Long > t, final RandomAccessibleInterval< LongType > labels )
	{
		if ( t._1() == 0 && t._1() == 0 && t._1() == 0 )
		{
			final TLongIntHashMap colors = new TLongIntHashMap();
			final Random rng = new Random( 100 );
			colors.put( 0, 0 );
			for ( final LongType l : Views.flatIterable( labels ) )
			{
				final long lb = l.get();
				if ( !colors.contains( lb ) )
					colors.put( lb, rng.nextInt() );
			}

			final RandomAccessibleInterval< ARGBType > coloredLabels = Converters.convert(
					labels,
					( Converter< LongType, ARGBType > ) ( s, tt ) -> tt.set( colors.get( s.getInteger() ) ),
					new ARGBType() );
//				BdvFunctions.show( labels, "LABELS TOP LEFT " + threshold );
			final BdvStackSource< ARGBType > bdv = BdvFunctions.show( coloredLabels, "LABELS TOP LEFT " );
			final ViewerPanel vp = bdv.getBdvHandle().getViewerPanel();
			final RealRandomAccess< LongType > access = Views.interpolate( Views.extendValue( labels, new LongType( -1 ) ), new NearestNeighborInterpolatorFactory<>() ).realRandomAccess();
			final ValueDisplayListener< LongType > vdl = new ValueDisplayListener<>( access, vp );
			vp.getDisplay().addMouseMotionListener( vdl );
			vp.getDisplay().addOverlayRenderer( vdl );

			final RandomAccessibleInterval< Pair< LongType, ARGBType > > paired = Views.interval( Views.pair( labels, coloredLabels ), labels );
			final RandomAccessibleInterval< ARGBType > only2812 = Converters.convert(
					paired,
					( s, tar ) -> {
						tar.set( s.getA().get() == 2812 ? s.getB().get() : 0 );
					},
					new ARGBType() );

			BdvFunctions.show( only2812, "2812", BdvOptions.options().addTo( bdv ) );

		}
	}

}