package de.hanslovsky.watersheds.graph;

import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;

import bdv.util.Bdv;
import bdv.util.BdvFunctions;
import bdv.util.BdvOptions;
import bdv.util.BdvStackSource;
import de.hanslovsky.watersheds.graph.MergeBloc.Out;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.converter.Converters;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.LongType;
import scala.Tuple2;

public class VisTools
{

	public static Bdv show(
			final TLongLongHashMap assignments,
			final TLongLongHashMap outsideNodes,
			final RandomAccessibleInterval< LongType > labelsTarget,
			final String name1, final String name2 )
	{
		final TLongIntHashMap cmap = new TLongIntHashMap();
		final Random rg = new Random( 100 );
		final BdvStackSource< ARGBType > bdv = BdvFunctions.show(
				Converters.convert(
						labelsTarget,
						( s, t ) -> {
							final long sv = s.get();
							if ( assignments.contains( sv ) )
							{
								if ( outsideNodes.contains( sv ) )
									t.set( 125 << 16 | 125 << 8 | 125 << 0 );
								else
								{
									if ( !cmap.contains( sv ) )
										cmap.put( sv, rg.nextInt() );
									t.set( cmap.get( sv ) );
								}
							}
							else
								t.set( 0 );
						},
						new ARGBType() ),
				name1 );
		BdvFunctions.show(
				Converters.convert(
						labelsTarget,
						( s, t ) -> {
							final long sv = s.get();
							if ( assignments.contains( sv ) )
							{
								if ( !outsideNodes.contains( sv ) )
									t.set( 125 << 16 | 125 << 8 | 125 << 0 );
								else
								{
									if ( !cmap.contains( sv ) )
										cmap.put( sv, rg.nextInt() );
									t.set( cmap.get( sv ) );
								}
							}
							else
								t.set( 0 );
						},
						new ARGBType() ),
				name2, BdvOptions.options().addTo( bdv ) );
		return bdv;
	}

	public static Bdv showBlock(
			final JavaPairRDD< Tuple2< Long, Long >, Out > selection,
			final RandomAccessibleInterval< LongType > labelsTarget,
			final String name1, final String name2 )
	{
		final Out data = selection.values().collect().get( 0 );
		return show( data.assignments, data.outsideNodes, labelsTarget, name1, name2 );
	}

	public static JavaPairRDD< Tuple2< Long, Long >, Out > showBlockSelectByNode(
			final JavaPairRDD< Tuple2< Long, Long >, Out > mergedEdges,
			final RandomAccessibleInterval< LongType > labelsTarget,
			final long label )
	{
		final JavaPairRDD< Tuple2< Long, Long >, Out > selection = mergedEdges.filter( t -> t._2().g.nodeEdgeMap().contains( label ) );
		if ( selection.count() > 0 )
			showBlock( selection, labelsTarget, "1", "2" );

		return selection;
	}

}
