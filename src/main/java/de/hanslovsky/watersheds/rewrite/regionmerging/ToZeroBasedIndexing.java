package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;

public class ToZeroBasedIndexing implements Function< RegionMergingInput, MergeBlocIn >
{

	private final Broadcast< EdgeMerger > edgerMergeBC;

	public ToZeroBasedIndexing( final Broadcast< EdgeMerger > edgerMergeBC )
	{
		super();
		this.edgerMergeBC = edgerMergeBC;
	}

	@Override
	public MergeBlocIn call( final RegionMergingInput input ) throws Exception
	{
		final UndirectedGraphArrayBased g = new UndirectedGraphArrayBased( input.nNodes, Util.mapEdges( input.edges, input.nodeIndexMapping ), edgerMergeBC.getValue() );

		return new MergeBlocIn(
				g,
				Util.mapCounts( input.counts, input.nodeIndexMapping ),
				Util.mapOutsideNodes( input.outsideNodes, input.nodeIndexMapping ),
				Util.inverse( input.nodeIndexMapping ) );
	}

}