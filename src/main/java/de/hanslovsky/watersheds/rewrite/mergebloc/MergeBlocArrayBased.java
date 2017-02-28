package de.hanslovsky.watersheds.rewrite.mergebloc;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.graph.EdgeWeight;
import de.hanslovsky.watersheds.rewrite.util.ChangeablePriorityQueue;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class MergeBlocArrayBased implements PairFunction< Tuple2< Long, MergeBlocIn >, Long, Tuple2< Long, MergeBlocOut > >
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.TRACE );
	}

	private final EdgeMerger edgeMerger;

	private final EdgeWeight edgeWeight;

	private final double threshold;

	private final double regionRatio;

	public static final int MERGERS_ENTRY_SIZE = 4;

	public MergeBlocArrayBased( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight, final double threshold )
	{
		this( edgeMerger, edgeWeight, threshold, 0.0 );
	}

	public MergeBlocArrayBased( final EdgeMerger edgeMerger, final EdgeWeight edgeWeight, final double threshold, final double regionRatio )
	{
		super();
		this.edgeMerger = edgeMerger;
		this.edgeWeight = edgeWeight;
		this.threshold = threshold;
		this.regionRatio = regionRatio;
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

		final TLongHashSet borderNodeSet = new TLongHashSet();

		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			double w = e.weight();
			final long from = e.from(), to = e.to();

			if ( Double.isNaN( w ) )
			{
				w = edgeWeight.weight( e.affinity(), in.counts[ ( int ) e.from() ], in.counts[ ( int ) e.to() ] );
				e.weight( w );
			}

			if ( w >= 0.0 )
			{

				LOG.trace( "Adding to queue: " + e.toString() + " (" + in.indexNodeMapping[ ( int ) from ] + ", " + in.indexNodeMapping[ ( int ) to ] + ")" );

				queue.push( i, w );

				if ( in.outsideNodes.contains( ( int ) from ) )
					borderNodeSet.add( to );

				if ( in.outsideNodes.contains( ( int ) to ) )
					borderNodeSet.add( from );

			}
			else
				LOG.trace( "Not adding to queue: " + e.toString() + " (" + in.indexNodeMapping[ ( int ) from ] + ", " + in.indexNodeMapping[ ( int ) to ] + ")" );
		}

		long pointingOutside = t._1();
		final long self = t._1();
		final int nInternalNodes = nNodes - in.outsideNodes.size();
		final int minNMerges = ( int ) ( nInternalNodes * regionRatio );

		final TLongArrayList merges = new TLongArrayList();

		for ( int count = 0; !queue.empty() && ( count < minNMerges || pointingOutside == self ); )
		{
			final int next = queue.pop();
			e.setIndex( next );
			final double w = e.weight();
			LOG.trace( "Looking at edge " + e + "(" + in.indexNodeMapping[ ( int ) e.from() ] + ", " + in.indexNodeMapping[ ( int ) e.to() ] + ")" );

			if ( Double.isNaN( w ) )
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

			else if ( w < 0.0 )
				continue;

			else if ( w > threshold )
			{
				queue.push( next, w );
				break;
			}

			else if ( in.outsideNodes.contains( ( int ) e.from() ) )
			{
				if ( pointingOutside == self )
					pointingOutside = in.outsideNodes.get( ( int ) e.from() );
				continue;
//				break;
			}
			else if ( in.outsideNodes.contains( ( int ) e.to() ) )
			{
				if ( pointingOutside == self )
					pointingOutside = in.outsideNodes.get( ( int ) e.to() );
				continue;
//				break;
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

//			if ( borderNodeSet.contains( r1 ) )
//			{
//				borderNodeSet.add( n );
//				borderNodeSet.add( r2 );
//			}
//
//			if ( borderNodeSet.contains( r2 ) )
//			{
//				borderNodeSet.add( n );
//				borderNodeSet.add( r2 );
//			}
			final long c1 = in.counts[ r1 ];
			final long c2 = in.counts[ r2 ];

			merges.add( r1 );
			merges.add( r2 );
			merges.add( n );
			merges.add( Double.doubleToLongBits( w ) );

			if ( LOG.getLevel().isGreaterOrEqual( Level.TRACE ) )
				LOG.trace( "Merging " + e + " into " + n + " -- ( (" + in.indexNodeMapping[ ( int ) e.from() ] + ", " + in.indexNodeMapping[ ( int ) e.to() ] + ") ->" + in.indexNodeMapping[ n ] + " )" );

			assert c1 > 0 && c2 > 0: "Counts does not contain ids!";

			in.counts[ n == r1 ? r2 : r1 ] = 0;
			in.counts[ n ] = c1 + c2;

			final TIntIntHashMap discardEdges = in.g.contract( e, n, this.edgeMerger );
			discardEdges.clear();

			dj.findRoot( r1 );
			dj.findRoot( r2 );

//			System.out.println( this.getClass().getSimpleName() + ": count: " + count + " " + minNMerges );
			++count;



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

		}

		for ( int i = 0; i < nNodes; ++i )
			if ( borderNodeSet.contains( dj.findRoot( i ) ) )
				borderNodeSet.add( i );

		final TLongLongHashMap borderNodes = new TLongLongHashMap();
		for ( final TLongIterator it = borderNodeSet.iterator(); it.hasNext(); )
		{
			final long bn = it.next();
			final int r = dj.findRoot( ( int ) bn );
			borderNodes.put( bn, r );
			if ( !borderNodes.contains( r ) )
				borderNodes.put( r, r );

		}

		return new Tuple2<>( t._1(), new Tuple2<>(
				pointingOutside, new MergeBlocOut(
						in.counts,
						in.outsideNodes,
						dj,
						merges.size() > 0 || pointingOutside != t._1().longValue(),
						returnEdges,
						merges,
						in.indexNodeMapping,
						borderNodes ) ) );
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
