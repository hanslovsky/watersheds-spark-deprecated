package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import scala.Tuple2;

public class ToZeroBasedIndexing< K > implements PairFunction< Tuple2< K, RegionMergingInput >, K, MergeBlocIn >
{

	private final Broadcast< EdgeMerger > edgerMergeBC;

	public ToZeroBasedIndexing( final Broadcast< EdgeMerger > edgerMergeBC )
	{
		super();
		this.edgerMergeBC = edgerMergeBC;
	}

	@Override
	public Tuple2< K, MergeBlocIn > call( final Tuple2< K, RegionMergingInput > t ) throws Exception
	{
		final RegionMergingInput input = t._2();

		System.out.println( t._1() + " " + input.nNodes + " " + input.edges.size() + " " + input.nodeIndexMapping.size() + " " + input.counts.size() );

		final Edge edg = new Edge( Util.mapEdges( input.edges, input.nodeIndexMapping ) );
		final Edge eOther = new Edge( input.edges );
		if ( edg.size() > 46111 )
		{
			edg.setIndex( 46111 );
			eOther.setIndex( 46111 );
			System.out.println( edg + " " + eOther + " " + input.nodeIndexMapping.get( eOther.from() ) + " " + input.nodeIndexMapping.get( eOther.to() )
			+ " " + input.nodeIndexMapping.contains( eOther.from() ) + " " + input.nodeIndexMapping.contains( eOther.to() ) );
		}
		final UndirectedGraphArrayBased g = new UndirectedGraphArrayBased( input.nNodes, Util.mapEdges( input.edges, input.nodeIndexMapping ), edgerMergeBC.getValue() );

		return new Tuple2<>( t._1(), new MergeBlocIn(
				g,
				Util.mapCounts( input.counts, input.nodeIndexMapping ),
				Util.mapOutsideNodes( input.outsideNodes, input.nodeIndexMapping ),
				Util.inverse( input.nodeIndexMapping ) ) );
	}

}