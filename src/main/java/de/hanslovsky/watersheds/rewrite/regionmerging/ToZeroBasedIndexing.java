package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;

public class ToZeroBasedIndexing implements Function< RegionMergingInput, MergeBlocIn >
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

	private final Broadcast< EdgeMerger > edgerMergeBC;

	public ToZeroBasedIndexing( final Broadcast< EdgeMerger > edgerMergeBC )
	{
		super();
		this.edgerMergeBC = edgerMergeBC;
	}

	@Override
	public MergeBlocIn call( final RegionMergingInput input ) throws Exception
	{
		LOG.debug( "Mapping to zero based index (" + new Edge( input.edges ).size() + " edges)." );

		final UndirectedGraphArrayBased g = new UndirectedGraphArrayBased( input.nNodes, Util.mapEdges( input.edges, input.nodeIndexMapping ), edgerMergeBC.getValue() );

		if ( LOG.getLevel().isGreaterOrEqual( Level.TRACE ) )
		{
			final StringBuilder sb = new StringBuilder( "Logging edges after graph construction" );
			final Edge e = new Edge( g.edges() );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				sb.append( "\n" ).append( e.toString() );
			}
			LOG.trace( sb.toString() );
		}

		return new MergeBlocIn(
				g,
				Util.mapCounts( input.counts, input.nodeIndexMapping ),
				Util.mapOutsideNodes( input.outsideNodes, input.nodeIndexMapping ),
				Util.inverse( input.nodeIndexMapping ) );
	}

}