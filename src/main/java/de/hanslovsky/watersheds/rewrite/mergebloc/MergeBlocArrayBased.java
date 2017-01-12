package de.hanslovsky.watersheds.rewrite.mergebloc;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.ChangeablePriorityQueue;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class MergeBlocArrayBased implements PairFunction< Tuple2< Long, MergeBlocIn >, Long, Tuple2< Long, MergeBlocOut > >
{

	private final EdgeMerger edgeMerger;

	private final EdgeWeight edgeWeight;

	private final double threshold;

	public MergeBlocArrayBased( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight, final double threshold )
	{
		super();
		this.edgeMerger = edgeMerger;
		this.edgeWeight = edgeWeight;
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

		final ChangeablePriorityQueue queue = new ChangeablePriorityQueue( e.size() );

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final double w = e.weight();
			final long from = in.indexNodeMapping[ ( int ) e.from() ], to = in.indexNodeMapping[ ( int ) e.to() ];
			if ( from == 7085 || to == 7085 )
				System.out.println( "MergeBloc: " + e + " " + from + " " + to );
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
				final int f = dj.findRoot( ( int ) e.from() );
				final int to = dj.findRoot( ( int ) e.to() );
				final double weight = edgeWeight.weight( e.affinity(), in.counts[ f ], in.counts[ to ] );
				// TODO need only weight update?
				e.from( f );
				e.to( to );
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

			merges.add( r1 );
			merges.add( r2 );
			merges.add( n );
			merges.add( Double.doubleToLongBits( w ) );


			assert c1 > 0 && c2 > 0: "Counts does not contain ids!";

			in.counts[ n == r1 ? r2 : r1 ] = 0;
			in.counts[ n ] = c1 + c2;

			final TIntIntHashMap discardEdges = in.g.contract( e, n, this.edgeMerger );
			discardEdges.clear();



		}

		final TDoubleArrayList returnEdges = new TDoubleArrayList();
		final Edge rE = new Edge( returnEdges );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			if ( Double.isNaN( e.weight() ) || e.weight() >= 0.0 )
			{
				e.from( dj.findRoot( (int ) e.from() ) );
				e.to( dj.findRoot( (int ) e.to() ) );
				rE.add( e );
			}
			else
				continue;
			final long from = in.indexNodeMapping[ ( int ) e.from() ], to = in.indexNodeMapping[ ( int ) e.to() ];
			if ( from == 7085 || to == 7085 )
				System.out.println( "MergeBlocOut: " + e + " " + from + " " + to );

		}

		return new Tuple2<>( t._1(), new Tuple2<>(
				pointingOutside, new MergeBlocOut(
						in.counts,
						in.outsideNodes,
						dj,
						merges.size() > 0 || pointingOutside != t._1().longValue(),
						returnEdges,
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
