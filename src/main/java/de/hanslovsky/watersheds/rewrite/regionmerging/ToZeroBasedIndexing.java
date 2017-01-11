package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import scala.Tuple2;

public class ToZeroBasedIndexing< K > implements PairFunction< Tuple2< K, RegionMergingInput >, K, MergeBlocIn >
{

	@Override
	public Tuple2< K, MergeBlocIn > call( final Tuple2< K, RegionMergingInput > t ) throws Exception
	{
		final RegionMergingInput input = t._2();

		System.out.println( t._1() + " " + input.nNodes + " " + input.edges.size() + " " + input.nodeIndexMapping.size() + " " + input.counts.size() );

		final UndirectedGraphArrayBased g = new UndirectedGraphArrayBased( input.nNodes, Util.mapEdges( input.edges, input.nodeIndexMapping ) );

		return new Tuple2<>( t._1(), new MergeBlocIn(
				g,
				Util.mapCounts( input.counts, input.nodeIndexMapping ),
				Util.mapOutsideNodes( input.outsideNodes, input.nodeIndexMapping ),
				Util.mapBorderNodes( input.borderNodes, input.nodeIndexMapping ),
				Util.inverse( input.nodeIndexMapping ) ) );
	}

}