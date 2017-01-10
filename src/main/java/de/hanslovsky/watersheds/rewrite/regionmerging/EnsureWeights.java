package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocIn;
import scala.Tuple2;

public class EnsureWeights implements PairFunction< Tuple2< Long, MergeBlocIn >, Long, MergeBlocIn >
{

	private final EdgeWeight edgeWeight;

	public EnsureWeights( final EdgeWeight edgeWeight )
	{
		super();
		this.edgeWeight = edgeWeight;
	}

	@Override
	public Tuple2< Long, MergeBlocIn > call( final Tuple2< Long, MergeBlocIn > t ) throws Exception
	{
		final Edge e = new Edge( t._2().g.edges() );
		final long[] counts = t._2().counts;
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( Double.isNaN( e.weight() ) )
				e.weight( edgeWeight.weight( e.affinity(), counts[ ( int ) e.from() ], counts[ ( int ) e.to() ] ) );
		}
		return new Tuple2<>( t._1(), t._2() );
	}

}