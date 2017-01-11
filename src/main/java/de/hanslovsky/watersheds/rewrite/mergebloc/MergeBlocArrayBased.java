package de.hanslovsky.watersheds.rewrite.mergebloc;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.ChangeablePriorityQueue;
import de.hanslovsky.watersheds.rewrite.util.MergerService;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class MergeBlocArrayBased implements PairFunction< Tuple2< Long, MergeBlocIn >, Long, Tuple2< Long, MergeBlocOut > >
{

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
	public Tuple2< Long, Tuple2< Long, MergeBlocOut > > call( final Tuple2< Long, MergeBlocIn > t ) throws Exception
	{
		final MergeBlocIn in = t._2();
		final TDoubleArrayList edges = in.g.edges();
		final Edge e = new Edge( edges );

		final int nNodes = in.g.nNodes();
		final DisjointSets dj = new DisjointSets( nNodes );

		for ( int i = 0; i < e.size(); ++i )
			if ( e.from() == e.to() && e.weight() != -1.0d )
			{
				System.out.println( "WHY IS FROM EQUAL TO TO? " + e );
				System.exit( 451 );
			}

		for ( int i = 0; i < in.g.nodeEdgeMap().length; ++i )
			if ( in.g.nodeEdgeMap()[i].contains( i ) )
			{
				System.out.println( "WHY DOES NODE HAVE EDGE TO ITSELF? " + i + " " + in.g.nodeEdgeMap()[i] );
				System.exit( 123 );
			}

		final ChangeablePriorityQueue queue = new ChangeablePriorityQueue( e.size() );

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final double w = e.weight();
			if ( w > 0.0 )
				queue.push( i, w );
		}

		long pointingOutside = t._1();

		final TLongArrayList merges = new TLongArrayList();

		while ( !queue.empty() )
		{
			final int next = queue.pop();
			e.setIndex( next );
			final double w = e.weight();

			if ( w < 0.0 )
				continue;

			else if ( in.outsideNodes.contains( ( int ) e.from() ) )
			{
				pointingOutside = in.outsideNodes.get( ( int ) e.from() );
				break;
			}

			else if ( in.outsideNodes.contains( ( int ) e.to() ) )
			{
				pointingOutside = in.outsideNodes.get( ( int ) e.to() );
				break;
			}

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

//			if ( in.borderNodes.contains( from ) )
//			{
//				if ( !in.borderNodes.contains( n ) )
//					in.borderNodes.put( n, new TLongHashSet() );
//				in.borderNodes.get( n ).addAll( in.borderNodes.get( from ) );
//			}
//
//			if ( in.borderNodes.contains( to ) )
//			{
//				if ( !in.borderNodes.contains( n ) )
//					in.borderNodes.put( n, new TLongHashSet() );
//				in.borderNodes.get( n ).addAll( in.borderNodes.get( to ) );
//			}
//
//			if ( in.borderNodes.contains( r1 ) )
//			{
//				if ( !in.borderNodes.contains( n ) )
//					in.borderNodes.put( n, new TLongHashSet() );
//				in.borderNodes.get( n ).addAll( in.borderNodes.get( r1 ) );
//			}
//
//			if ( in.borderNodes.contains( r2 ) )
//			{
//				if ( !in.borderNodes.contains( n ) )
//					in.borderNodes.put( n, new TLongHashSet() );
//				in.borderNodes.get( n ).addAll( in.borderNodes.get( r2 ) );
//			}

			final long c1 = in.counts[ r1 ];
			final long c2 = in.counts[ r2 ];

			merges.add( r1 );
			merges.add( r2 );
			merges.add( n );
			merges.add( Double.doubleToLongBits( w ) );


			assert c1 > 0 && c2 > 0: "Counts does not contain ids!";

			if ( from == 3583 || to == 3583 || r1 == 3583 || r2 == 3583 )
			{
				System.out.println( "Merging " + e + " " + queue.size() + " " + r1 + " " + r2 + " " + n + " " + in.counts[ r1 ] + " " + in.counts[ r2 ] );
				System.out.println( in.g.nodeEdgeMap()[ 3583 ] );
			}

			in.counts[ n == r1 ? r2 : r1 ] = 0;
			in.counts[ n ] = c1 + c2;

//			System.out.println( "Contracting " + e + " " + n );
			final TIntIntHashMap discardEdges = in.g.contract( e, n, this.edgeMerger );
			discardEdges.clear();

//			mergerService.addMerge( r1, r2, n, w );


		}

//		mergerService.finalize();


		return new Tuple2<>( t._1(), new Tuple2<>(
				pointingOutside, new MergeBlocOut(
						in.counts,
						in.outsideNodes,
						dj,
						merges.size() > 0 || pointingOutside != t._1().longValue(),
						in.g.edges(),
						merges,
						in.indexNodeMapping ) ) );
	}

	private static TDoubleArrayList filterEdges( final TDoubleArrayList edges, final long[] counts, final EdgeWeight edgeWeight )
	{
		final TDoubleArrayList filteredEdges = new TDoubleArrayList();
		final Edge e = new Edge( edges );
		final Edge f = new Edge( filteredEdges );

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final double w = e.weight();
			if ( w < 0.0d )
				continue;

			final int from = (int) e.from();
			final int to = (int) e.to();
			f.add( Double.isNaN( w ) ? edgeWeight.weight( w, counts[ from ], counts[ to ] ) : w, e.affinity(), from, to, e.multiplicity() );

		}

		return filteredEdges;
	}

}
