package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import gnu.trove.map.hash.TIntLongHashMap;
import scala.Tuple2;

public class EnsureWeights implements PairFunction< Tuple2< Long, MergeBlocIn >, Long, Tuple2< MergeBlocIn, Double > >
{

	private final EdgeWeight edgeWeight;

	public EnsureWeights( final EdgeWeight edgeWeight )
	{
		super();
		this.edgeWeight = edgeWeight;
	}

	@Override
	public Tuple2< Long, Tuple2< MergeBlocIn, Double > > call( final Tuple2< Long, MergeBlocIn > t ) throws Exception
	{
		double maxWeight = Double.NEGATIVE_INFINITY;
		final Edge e = new Edge( t._2().g.edges() );
		final TIntLongHashMap outsideNodes = t._2().outsideNodes;
		final long[] counts = t._2().counts;
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( e.from() == e.to() )
				e.weight( -1.0d );
			else
			{
				if ( Double.isNaN( e.weight() ) )
					e.weight( edgeWeight.weight( e.affinity(), counts[ ( int ) e.from() ], counts[ ( int ) e.to() ] ) );
				if ( !outsideNodes.contains( ( int ) e.from() ) && !outsideNodes.contains( ( int ) e.to() ) )
					maxWeight = Math.max( e.weight(), maxWeight );
			}
		}
		return new Tuple2<>( t._1(), new Tuple2<>( t._2(), maxWeight ) );
	}

}