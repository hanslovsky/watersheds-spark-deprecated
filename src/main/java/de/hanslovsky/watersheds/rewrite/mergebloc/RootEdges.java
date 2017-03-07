package de.hanslovsky.watersheds.rewrite.mergebloc;

import org.apache.spark.api.java.function.Function;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RootEdges implements Function< Tuple2< Long, MergeBlocOut >, Tuple2< Long, MergeBlocOut > >
{

	private final int edgeDataSize;

	public RootEdges( final int edgeDataSize )
	{
		super();
		this.edgeDataSize = edgeDataSize;
	}

	@Override
	public Tuple2< Long, MergeBlocOut > call( final Tuple2< Long, MergeBlocOut > input ) throws Exception
	{
		final Edge e = new Edge( input._2().edges, edgeDataSize );
		final DisjointSets map = input._2().dj;
		final long[] counts = input._2().counts;
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final int from = ( int ) e.from();
			final int to = ( int ) e.to();

			final int rFrom = map.findRoot( from );
			final int rTo = map.findRoot( to );

			if ( rFrom != from )
				counts[ from ] = 0;

			if ( rTo != to )
				counts[ to ] = 0;

			e.from( rFrom );
			e.to( rTo );

			if ( rFrom == rTo )
				e.setObsolete();

		}
		return input;
	}

}
