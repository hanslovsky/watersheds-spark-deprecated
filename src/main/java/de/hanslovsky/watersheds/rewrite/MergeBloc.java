package de.hanslovsky.watersheds.rewrite;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.MergeBloc.MergeBlocData;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class MergeBloc implements PairFunction< Tuple2< Long, MergeBlocData >, Tuple2< Long, Long >, MergeBlocData >
{

	public static class MergeBlocData
	{

		public final UndirectedGraph g;

		public final TLongLongHashMap counts;

		public MergeBlocData( final UndirectedGraph g, final TLongLongHashMap counts )
		{
			super();
			this.g = g;
			this.counts = counts;
		}

	}


	private final EdgeMerger edgeMerger;

	private final EdgeWeight edgeWeight;

	private final MergerService mergerService;

	private final double threshold;

	public MergeBloc( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight, final MergerService mergerService, final double threshold )
	{
		super();
		this.edgeMerger = edgeMerger;
		this.edgeWeight = edgeWeight;
		this.mergerService = mergerService;
		this.threshold = threshold;
	}



	@Override
	public Tuple2< Tuple2< Long, Long >, MergeBlocData > call( final Tuple2< Long, MergeBlocData > t ) throws Exception
	{
		final MergeBlocData in = t._2();
		final TDoubleArrayList edges = in.g.edges();
		final Edge e = new Edge( edges );

		final DisjointSetsHashMap dj = new DisjointSetsHashMap();
		for ( final TLongObjectIterator< TLongIntHashMap > it = in.g.nodeEdgeMap().iterator(); it.hasNext(); )
		{
			it.advance();
			dj.findRoot( it.key() );
		}

		final ChangeablePriorityQueue queue = new ChangeablePriorityQueue( e.size() );

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final double w = e.weight();
			if ( w > 0.0 )
				queue.push( i, w );
		}

		while ( !queue.empty() )
		{
			final int next = queue.pop();
			e.setIndex( next );
			final double w = e.weight();

			if ( w < 0.0 )
				continue;

			if ( w > threshold )
			{
				queue.push( next, w );
				break;
			}

			final long from = e.from();
			final long to = e.to();

			final long r1 = dj.findRoot( from );
			final long r2 = dj.findRoot( to );

			// if already merged go on
			if ( r1 == r2 )
				continue;

			final long n = dj.join( r1, r2 );

			assert in.counts.contains( r1 ) && in.counts.contains( r2 ): "Counts does not contain ids!";

			final long c1 = in.counts.remove( r1 );
			final long c2 = in.counts.remove( r2 );
			in.counts.put( n, c1 + c2 );

			final TLongIntHashMap discardEdges = in.g.contract( e, n, this.edgeMerger );

			for ( final TLongIntIterator it = discardEdges.iterator(); it.hasNext(); )
			{
				it.advance();
				final long id = it.key();
				final int index = it.value();
				e.setIndex( index );
				if ( e.weight() < 0.0 )
					// why is this test necessary? Bug? TODO
					if ( queue.contains( index ) )
						queue.deleteItem( index );

			}

			for ( final TLongIntIterator it = in.g.nodeEdgeMap().get( n ).iterator(); it.hasNext(); )
			{
				it.advance();
				final int index = it.value();
				e.setIndex( index );
				e.weight( edgeWeight.weight( e.affinity(), in.counts.get( e.from() ), in.counts.get( e.to() ) ) );
//				System.out.println( in.counts.get( e.from() ) + " " + in.counts.get( e.to() ) + " " + e );
			}

			mergerService.addMerge( from, to, n, w );


		}

		mergerService.finalize();

		return null;
	}

}
