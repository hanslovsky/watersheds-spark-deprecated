package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import de.hanslovsky.watersheds.rewrite.graph.edge.Edge;
import de.hanslovsky.watersheds.rewrite.graph.edge.EdgeWeight;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class EnsureWeights implements Function< RegionMergingInput, Tuple2< RegionMergingInput, Double > >
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

	private final EdgeWeight edgeWeight;

	public EnsureWeights( final EdgeWeight edgeWeight )
	{
		super();
		this.edgeWeight = edgeWeight;
	}

	@Override
	public Tuple2< RegionMergingInput, Double > call( final RegionMergingInput in ) throws Exception
	{
		double maxWeight = Double.NEGATIVE_INFINITY;
		final Edge e = new Edge( in.edges, edgeWeight.dataSize() );
		final TLongLongHashMap outsideNodes = in.outsideNodes;
		final TLongLongHashMap counts = in.counts;
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( e.from() == e.to() )
				e.setObsolete();
			else if ( e.isValid() )
			{
				e.weight( edgeWeight.weight( e, counts.get( e.from() ), counts.get( e.to() ) ) );
				if ( !outsideNodes.contains( ( int ) e.from() ) && !outsideNodes.contains( ( int ) e.to() ) )
					maxWeight = Math.max( e.weight(), maxWeight );
			}
		}

		if ( LOG.getLevel().isGreaterOrEqual( Level.TRACE ) )
		{
			final StringBuilder sb = new StringBuilder( "Logging ensured edges: " );
			final Edge edg = new Edge( in.edges, edgeWeight.dataSize() );
			for ( int i = 0; i < edg.size(); ++i )
			{
				edg.setIndex( i );
				sb.append( "\n" ).append( edg.toString() );
			}
			LOG.trace( sb.toString() );
		}

		return new Tuple2<>( in, maxWeight );
	}

}