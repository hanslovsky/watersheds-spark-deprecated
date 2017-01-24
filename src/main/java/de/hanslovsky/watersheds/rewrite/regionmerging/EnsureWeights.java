package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class EnsureWeights implements PairFunction< Tuple2< Long, RegionMergingInput >, Long, Tuple2< RegionMergingInput, Double > >
{

	private final EdgeWeight edgeWeight;

	public EnsureWeights( final EdgeWeight edgeWeight )
	{
		super();
		this.edgeWeight = edgeWeight;
	}

	@Override
	public Tuple2< Long, Tuple2< RegionMergingInput, Double > > call( final Tuple2< Long, RegionMergingInput > t ) throws Exception
	{
		double maxWeight = Double.NEGATIVE_INFINITY;
		final Edge e = new Edge( t._2().edges );
		final TLongLongHashMap outsideNodes = t._2().outsideNodes;
		final TLongLongHashMap counts = t._2().counts;
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( e.from() == e.to() )
				e.weight( -1.0d );
			else
			{
//				if ( Double.isNaN( e.weight() ) )
				e.weight( edgeWeight.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
				if ( !outsideNodes.contains( ( int ) e.from() ) && !outsideNodes.contains( ( int ) e.to() ) )
					maxWeight = Math.max( e.weight(), maxWeight );
			}
		}
		return new Tuple2<>( t._1(), new Tuple2<>( t._2(), maxWeight ) );
	}

}