package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.Function;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class EnsureWeights implements Function< RegionMergingInput, Tuple2< RegionMergingInput, Double > >
{

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
		final Edge e = new Edge( in.edges );
		final TLongLongHashMap outsideNodes = in.outsideNodes;
		final TLongLongHashMap counts = in.counts;
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( e.from() == e.to() )
				e.weight( -1.0d );
			else if ( e.weight() < 0.0 )
			{

			}
			else
			{
				e.weight( edgeWeight.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
				if ( !outsideNodes.contains( ( int ) e.from() ) && !outsideNodes.contains( ( int ) e.to() ) )
					maxWeight = Math.max( e.weight(), maxWeight );
			}
		}
		return new Tuple2<>( in, maxWeight );
	}

}