package de.hanslovsky.watersheds.rewrite;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.MergeBlocArrayBased.MergeBlocData;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class MergeBlocArrayBased implements PairFunction< Tuple2< Long, MergeBlocData >, Tuple2< Long, Long >, MergeBlocData >
{

	public static class MergeBlocData
	{

		public final UndirectedGraphArrayBased g;

		public final long[] counts;

		public MergeBlocData( final UndirectedGraphArrayBased g, final long[] counts )
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

	public MergeBlocArrayBased( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight, final MergerService mergerService, final double threshold )
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

		final int nNodes = in.g.nNodes();
		final DisjointSets dj = new DisjointSets( nNodes );

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

			else if ( Double.isNaN( w ) )
			{
				final double weight = edgeWeight.weight( e.affinity(), in.counts[ dj.findRoot( ( int ) e.from() ) ], in.counts[ dj.findRoot( ( int ) e.to() ) ] );
				e.weight( weight );
				queue.push( next, weight );
				continue;
			}

			else if ( w > threshold )
			{
				queue.push( next, w );
				break;
			}

			final int from = ( int ) e.from();
			final int to = ( int ) e.to();

			final int r1 = dj.findRoot( from );
			final int r2 = dj.findRoot( to );

			// if already merged go on
			if ( r1 == r2 )
			{
				e.weight( -1.0 );
				continue;
			}

			final int n = dj.join( r1, r2 );

			final long c1 = in.counts[ r1 ];
			final long c2 = in.counts[ r2 ];


			assert c1 > 0 && c2 > 0: "Counts does not contain ids!";

			in.counts[ n ] = c1 + c2;

//			System.out.println( "Merging " + e + " " + r1 + " " + r2 + " " + n );

			final TIntIntHashMap discardEdges = in.g.contract( e, n, this.edgeMerger );

//			for ( final TIntIntIterator it = discardEdges.iterator(); it.hasNext(); )
//			{
//				it.advance();
//				final int id = it.key();
//				final int index = it.value();
//				e.setIndex( index );
//				if ( e.weight() < 0.0 )
//					// why is this test necessary? Bug? TODO
//					if ( queue.contains( index ) )
//						queue.deleteItem( index );
//
//			}
//
//			for ( final TIntIntIterator it = in.g.nodeEdgeMap()[ n ].iterator(); it.hasNext(); )
//			{
//				it.advance();
//				final int index = it.value();
//				e.setIndex( index );
////				e.weight( edgeWeight.weight( e.affinity(), in.counts[ ( int ) e.from() ], in.counts[ ( int ) e.to() ] ) );
//				System.out.println( e.from() + " " + dj.findRoot( ( int ) e.from() ) + " " + e.to() + " " + dj.findRoot( ( int ) e.to() ) );
////				e.weight( edgeWeight.weight( e.affinity(), in.counts[ dj.findRoot( ( int ) e.from() ) ], in.counts[ dj.findRoot( ( int ) e.to() ) ] ) );
////				System.out.println( in.counts.get( e.from() ) + " " + in.counts.get( e.to() ) + " " + e );
//			}
//			System.out.println();

			discardEdges.clear();

			mergerService.addMerge( from, to, n, w );


		}

		mergerService.finalize();

		return null;
	}

}
