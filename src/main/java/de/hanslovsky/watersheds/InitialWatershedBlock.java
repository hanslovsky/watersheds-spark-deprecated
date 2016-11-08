package de.hanslovsky.watersheds;

import java.io.Serializable;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executors;

import org.apache.spark.api.java.function.PairFunction;
import org.mastodon.collection.ref.RefArrayList;

import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import bdv.viewer.ViewerPanel;
import de.hanslovsky.watersheds.WatershedsSpark.ValueDisplayListener;
import gnu.trove.map.hash.TLongDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccess;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.Predicate;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.WeightedEdge;
import net.imglib2.algorithm.morphology.watershed.CompareBetter;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import net.imglib2.algorithm.morphology.watershed.affinity.AffinityView;
import net.imglib2.algorithm.morphology.watershed.affinity.CompositeFactory;
import net.imglib2.converter.Converter;
import net.imglib2.converter.Converters;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.interpolation.randomaccess.NearestNeighborInterpolatorFactory;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Pair;
import net.imglib2.view.Views;
import net.imglib2.view.composite.CompositeIntervalView;
import net.imglib2.view.composite.RealComposite;
import scala.Tuple2;
import scala.Tuple3;

public class InitialWatershedBlock implements PairFunction< Tuple2< Tuple3< Long, Long, Long >, float[] >, Tuple3< Long, Long, Long >, Tuple2< long[], long[] > >
{

	public static interface Visitor
	{
		public void act( Tuple3< Long, Long, Long > t, RandomAccessibleInterval< LongType > labels );
	}

	private final int[] intervalDimensions;

	public InitialWatershedBlock( final int[] intervalDimensions, final long[] volumeDimensions, final double threshold )
	{
		this( intervalDimensions, volumeDimensions, threshold, ( t, labels ) -> {} );
	}

	public InitialWatershedBlock( final int[] intervalDimensions, final long[] volumeDimensions, final double threshold, final Visitor visitor )
	{
		super();
		this.intervalDimensions = intervalDimensions;
		this.volumeDimensions = volumeDimensions;
		this.threshold = threshold;
		this.visitor = visitor;
	}

	private final long[] volumeDimensions;

	private final double threshold;

	private final Visitor visitor;

	private static final float[] extArr = new float[] { Float.NaN, Float.NaN, Float.NaN };

	/**
	 *
	 */
	private static final long serialVersionUID = -4252306792553120080L;

	@Override
	public Tuple2< Tuple3< Long, Long, Long >, Tuple2< long[], long[] > > call(
			final Tuple2< Tuple3< Long, Long, Long >, float[] > t ) throws Exception
	{
		final long[] o = new long[] { t._1()._1(), t._1()._2(), t._1()._3(), 3 };

		final long[] intervalDimensionsTruncated =
				Util.getCurrentChunkDimensions( o, volumeDimensions, intervalDimensions );
		intervalDimensionsTruncated[ o.length - 1 ] = o[ o.length - 1 ];

		final long[] labelsDimensions = new long[ intervalDimensionsTruncated.length - 1 ];
		System.arraycopy( intervalDimensionsTruncated, 0, labelsDimensions, 0, labelsDimensions.length );

		final ArrayImg< FloatType, FloatArray > affs = ArrayImgs.floats( t._2(), intervalDimensionsTruncated );
		final long[] labelsArr = new long[ ( int ) Intervals.numElements( labelsDimensions ) ];
		final ArrayImg< LongType, LongArray > labels = ArrayImgs.longs( labelsArr, labelsDimensions );

		final RealComposite< FloatType > extension = Views.collapseReal( ArrayImgs.floats( extArr, 1, 3 ) ).randomAccess().get();

		final CompositeIntervalView< FloatType, RealComposite< FloatType > > affsCollapsed = Views.collapseReal( affs );

		final CompositeFactory< FloatType, RealComposite< FloatType > > compFac = sz -> Views.collapseReal( ArrayImgs.floats( 1, sz ) ).randomAccess().get();

		final AffinityView< FloatType, RealComposite< FloatType > > affsView =
				new AffinityView<>( Views.extendValue( affsCollapsed, extension ), compFac );

		final long[] bidirectionalEdgesDimensions = intervalDimensionsTruncated.clone();
		bidirectionalEdgesDimensions[ bidirectionalEdgesDimensions.length - 1 ] *= 2;
		final ArrayImg< FloatType, FloatArray > affsCopy = ArrayImgs.floats( bidirectionalEdgesDimensions );

		for ( final Pair< RealComposite< FloatType >, RealComposite< FloatType > > p : Views.interval( Views.pair( affsView, Views.collapseReal( affsCopy ) ), labels ) )
			p.getB().set( p.getA() );

		final CompareBetter< FloatType > compare = ( f, s ) -> f.get() > s.get();

		final FloatType worstValue = new FloatType( -Float.MAX_VALUE );

		final long[] counts = AffinityWatershedBlocked.letItRain(
				Views.collapseReal( affsCopy ),
				labels,
				compare,
				worstValue,
				Executors.newFixedThreadPool( 1 ),
				1,
				() -> {} );

		visitor.act( t._1(), labels );

		if ( threshold <= 0 )
			return new Tuple2<>( t._1(), new Tuple2<>( labelsArr, counts ) );
		else
		{
			final TLongDoubleHashMap rg = AffinityWatershedBlocked.generateRegionGraph(
					Views.collapseReal( affsCopy ),
					labels,
					AffinityWatershedBlocked.generateSteps( AffinityWatershedBlocked.generateStride( labels ) ),
					compare, worstValue,
					1 << 63,
					1 << 62,
					counts.length );
			final RefArrayList< WeightedEdge > edgeList = AffinityWatershedBlocked.graphToList( rg, counts.length );
			Collections.sort( edgeList, Collections.reverseOrder() );
			final DisjointSets dj = AffinityWatershedBlocked.mergeRegionGraph( edgeList, counts, ( Predicate ) ( v1, v2 ) -> v1 < v2, new double[] { threshold } )[ 0 ];
			final long[] newCounts = new long[ dj.setCount() ];
			final TLongLongHashMap rootToNewIndexMap = new TLongLongHashMap();
			rootToNewIndexMap.put( 0, 0 );
			newCounts[ 0 ] = counts[ 0 ];
			for ( int i1 = 1, newIndex = 1; i1 < counts.length; ++i1 )
			{
				final int root = dj.findRoot( i1 );
				if ( !rootToNewIndexMap.contains( root ) )
				{
					rootToNewIndexMap.put( root, newIndex );
					newCounts[ newIndex ] = counts[ root ];
					++newIndex;
				}
			}

			for ( int i2 = 0; i2 < labelsArr.length; ++i2 )
				labelsArr[ i2 ] = rootToNewIndexMap.get( dj.findRoot( ( int ) labelsArr[ i2 ] ) );

			return new Tuple2<>( t._1(), new Tuple2<>( labelsArr, newCounts ) );
		}
	}

	public static class ShowTopLeftVisitor implements Visitor, Serializable
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

}
