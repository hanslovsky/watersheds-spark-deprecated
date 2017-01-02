package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import scala.Tuple2;

public class MergeBloc
{

	public static class In implements Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 8290046566497620074L;

		public final UndirectedGraph g;

		public final TLongLongHashMap counts;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public final TLongLongHashMap outsideNodes;

		public In( final UndirectedGraph g, final TLongLongHashMap counts, final TLongObjectHashMap< TLongHashSet > borderNodes, final TLongLongHashMap outsideNodes )
		{
			super();
			this.g = g;
			this.counts = counts;
			this.borderNodes = borderNodes;
			this.outsideNodes = outsideNodes;
		}

	}

	public static class Out implements Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -3490542120283388985L;

		public final UndirectedGraph g;

		public final TLongLongHashMap counts;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public final TLongLongHashMap outsideNodes;

		public final TLongLongHashMap assignments;

		public final TLongHashSet mergedBorderNodes;

		public Out(
				final UndirectedGraph g,
				final TLongLongHashMap counts,
				final TLongObjectHashMap< TLongHashSet > borderNodes,
				final TLongLongHashMap outsideNodes,
				final TLongLongHashMap assignments,
				final long... mergedBorderNodes )
		{
			this( g, counts, borderNodes, outsideNodes, assignments, new TLongHashSet( mergedBorderNodes ) );
		}

		public Out(
				final UndirectedGraph g,
				final TLongLongHashMap counts,
				final TLongObjectHashMap< TLongHashSet > borderNodes,
				final TLongLongHashMap outsideNodes,
				final TLongLongHashMap assignments,
				final TLongHashSet mergedBorderNodes )
		{
			super();
			this.g = g;
			this.counts = counts;
			this.borderNodes = borderNodes;
			this.outsideNodes = outsideNodes;
			this.assignments = assignments;
			this.mergedBorderNodes = mergedBorderNodes;
		}

	}

	public static class EdgesAndCounts implements Serializable {

		private static final long serialVersionUID = -5524785343491327810L;

		public static final int OUTSIDE_STEP = 3;

		public static final int COUNTS_STEP = 2;

		public static final int ASSIGNMENTS_STEP = 2;

		public final double[] edges;

		public final TLongLongHashMap counts;

		// id -> block id
		public final TLongLongHashMap outside;

		public final TLongLongHashMap assignments;

		public EdgesAndCounts( final double[] edges, final TLongLongHashMap counts, final TLongLongHashMap outside, final TLongLongHashMap assignments )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.outside = outside;
			this.assignments = assignments;
		}

	}

	public static class EdgeComparator implements IntComparator
	{

		public static interface DoubleComparator
		{
			default public int compare( final double d1, final double d2 )
			{
				return Double.compare( d1, d2 );
			}
		}

		private final DoubleComparator dComp;

		private final TDoubleArrayList weightedEdges;

		private final Edge e1;

		private final Edge e2;

		public EdgeComparator( final TDoubleArrayList weightedEdges )
		{
			this( new DoubleComparator(){}, weightedEdges );
		}

		public EdgeComparator( final DoubleComparator dComp, final TDoubleArrayList weightedEdges )
		{
			super();
			this.dComp = dComp;
			this.weightedEdges = weightedEdges;
			this.e1 = new Edge( this.weightedEdges );
			this.e2 = new Edge( this.weightedEdges );
		}

		@Override
		public int compare( final Integer i1, final Integer i2 )
		{
			return compare( i1.intValue(), i2.intValue() );
		}

		@Override
		public int compare( final int k1, final int k2 )
		{
			e1.setIndex( k1 );
			e2.setIndex( k2 );
			return dComp.compare( e1.weight(), e2.weight() );
		}

	}

	public static EdgeMerger DEFAULT_EDGE_MERGER = ( e1, e2 ) -> {
		e2.affinity( Math.max( e1.affinity(), e2.affinity() ) );
		e2.multiplicity( e1.multiplicity() + e2.multiplicity() );
		return e2;
	};

	public static class MergeBlocPairFunction2 implements PairFunction< Tuple2< Long, In >, Tuple2< Long, Long >, Out >
	{

		private static final long serialVersionUID = -1537751845300461154L;

		private final Function f;

		private final EdgeMerger merger;

		private final double threshold;

		private final IdService idService;

		private final MergerService mergerService;

		public MergeBlocPairFunction2(
				final Function f,
				final EdgeMerger merger,
				final double threshold,
				final IdService idService,
				final MergerService mergerService )
		{
			super();
			this.f = f;
			this.merger = merger;
			this.threshold = threshold;
			this.idService = idService;
			this.mergerService = mergerService;
		}



		@Override
		public Tuple2< Tuple2< Long, Long >, Out > call( final Tuple2< Long, In > t ) throws Exception
		{
			final In in = t._2();

			final UndirectedGraph g = new UndirectedGraph( in.g.edges(), merger );

			final IntHeapPriorityQueue queue = new IntHeapPriorityQueue( new EdgeComparator( g.edges() ) );
			final Edge e = new Edge( g.edges() );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				queue.enqueue( i );
			}

			boolean outsideNodeIsInvolved = false;
			long involvedOutsideBlock = -1;
			double maxWeightBeforeMerge = Double.NaN;
			final TLongHashSet mergedBorderNodes = new TLongHashSet();


			final TLongLongHashMap assignments = new TLongLongHashMap();
			final DisjointSetsHashMap dj = new DisjointSetsHashMap( assignments, new TLongLongHashMap(), 0 );
			for ( final TLongIterator it = g.nodeEdgeMap().keySet().iterator(); it.hasNext(); )
				dj.findRoot( it.next() );

//			int index = 0;
//			final String path = System.getProperty( "user.home" ) + "/git/promotion-philipp/notes/watersheds";
			while ( !queue.isEmpty() )
			{

				final int next = queue.dequeueInt();
				e.setIndex( next );
				final double w = e.weight();

				if ( w < 0 )
					continue;

				else if ( w > threshold || outsideNodeIsInvolved && w > maxWeightBeforeMerge )
					break;


				final long from = e.from();
				final long to = e.to();

				if ( in.outsideNodes.contains( from ) )
				{
					if ( !outsideNodeIsInvolved )
					{
						outsideNodeIsInvolved = true;
						involvedOutsideBlock = in.outsideNodes.get( from );
						maxWeightBeforeMerge = w;
					}
				}
				else if ( in.outsideNodes.contains( to ) )
				{
					if ( !outsideNodeIsInvolved )
					{
						outsideNodeIsInvolved = true;
						involvedOutsideBlock = in.outsideNodes.get( to );
						maxWeightBeforeMerge = w;
					}
				}
				else
				{
//					if ( from == 235 || to == 235 )
//						System.out.println( "??? " + t._1() + " " + from + " " + to + " " + dj.findRoot( from ) + " " + dj.findRoot( to ) );
//
//					if ( !g.nodeEdgeMap().contains( from ) )
//					{
//						System.out.println( t._1() + " SOMETHING WRONG WITH FROM! " + from + " " + g.nodeEdgeMap().contains( dj.findRoot( from ) ) );
//						System.exit( 123 );
//					}
//					if ( !g.nodeEdgeMap().contains( to ) )
//					{
//						System.out.println( t._1() + " SOMETHING WRONG WITH TO! " + to );
//						System.exit( 456 );
//					}
					final long r1 = dj.findRoot( from );
					final long r2 = dj.findRoot( to );
					if ( r1 == r2 || r1 != from || r2 != to )
						continue;
//					final long n = idService.requestIds( 1 );
					final long n = dj.join( r1, r2 );

					// add to list of merged border nodes if appropriate
					if ( in.borderNodes.contains( from ) )
					{
						if ( in.borderNodes.contains( n ) )
							in.borderNodes.get( n ).addAll( in.borderNodes.get( from ) );
						else
							in.borderNodes.put( n, in.borderNodes.get( from ) );
						mergedBorderNodes.add( from );
					}

					if ( in.borderNodes.contains( to ) )
					{
						if ( in.borderNodes.contains( n ) )
							in.borderNodes.get( n ).addAll( in.borderNodes.get( to ) );
						else
							in.borderNodes.put( n, in.borderNodes.get( to ) );
						mergedBorderNodes.add( to );
					}

					final long c1 = in.counts.remove( r1 );// from );
					final long c2 = in.counts.remove( r2 );// to );
					final long cn = c1 + c2;
					// TODO THIS CHECK MUST BE FIXED
					if ( cn > 1000 )
					{
						in.counts.put( r1, c1 );
						in.counts.put( r2, c2 );
					}
					else
					{
						in.counts.put( n, cn );
						final TLongIntHashMap newEdges = g.contract( next, n, in.counts, f );

						if ( newEdges == null )
						{
							System.out.println( "IS NULL!" );
							System.out.println( from + " " + to + " " + r1 + " " + r2 + " " + n + " " + c1 + " " + c2 + " " + cn );
							System.exit( 123 );
							in.counts.remove( n );
							continue;
						}


						for ( final TIntIterator it = newEdges.valueCollection().iterator(); it.hasNext(); )
						{
							final int id = it.next();
							e.setIndex( id );
							// TODO
							// should we still edges, even if larger than threshold?
							// could re-use graph with different threshold then!
//						if ( e.weight() < threshold )
							queue.enqueue( id );
						}
						mergerService.addMerge( from, to, n, w );
					}

				}

			}


//			double[] resultEdges = new double[0];
			for ( int k = e.size() - 1; k >= 0; --k )
			{
				e.setIndex( k );
				final double w = e.weight();
				if ( w == -1.0d )
					e.remove();

			}

			// make sure that everybody points to their roots
			for ( final TLongIterator k = assignments.keySet().iterator(); k.hasNext(); )
			{
				final long nxt = k.next();
				final long r = dj.findRoot( nxt );
			}

			final Out result = new Out(
					g,
					in.counts,
					in.borderNodes,
					in.outsideNodes,
					assignments,
					mergedBorderNodes );
			return new Tuple2<>( new Tuple2<>( t._1(), involvedOutsideBlock == -1 ? t._1() : involvedOutsideBlock ), result );


		}
	}

	public static long findRoot( final TLongLongHashMap assignments, final long index )
	{
		long i1 = index, i2 = index;

		while ( i1 != assignments.get( i1 ) )
			i1 = assignments.get( i1 );

		while ( i2 != assignments.get( i2 ) )
		{
			final long tmp = assignments.get( i2 );
			assignments.put( i2, i1 );
			i2 = tmp;

		}
		return i1;
	}

	public static void mergeEdges(
			final int keepNode,
			final int discardNode,
			final TIntHashSet[] nodeEdgeMap,
			final TDoubleArrayList edges,
			final IntHeapPriorityQueue queue,
			final long[] counts,
			final Function f,
			final Edge e1,
			final Edge e2,
			final EdgeMerger merger )
	{
		final TIntHashSet keepNodeEdges = nodeEdgeMap[ keepNode ];
		final TIntHashSet discardNodeEdges = nodeEdgeMap[ discardNode ];
		final TIntIntHashMap neighborToEdgeIndexMap = new TIntIntHashMap();

		addNeighborEdges( nodeEdgeMap, keepNodeEdges, keepNode, keepNode, discardNode, counts, neighborToEdgeIndexMap, f, e1, e2, merger );
		addNeighborEdges( nodeEdgeMap, discardNodeEdges, keepNode, discardNode, keepNode, counts, neighborToEdgeIndexMap, f, e1, e2, merger );

		keepNodeEdges.clear();
		discardNodeEdges.clear();

		for ( final TIntIntIterator it = neighborToEdgeIndexMap.iterator(); it.hasNext(); )
		{
			it.advance();
			final int k = it.value();
			keepNodeEdges.add( k );
			nodeEdgeMap[ it.key() ].add( k );
			queue.enqueue( k );
		}

	}

	public static void addNeighborEdges(
			final TIntHashSet[] nodeEdgeMap,
			final TIntHashSet incidentEdges,
			final int newNode,
			final int mergedNode1,
			final int mergedNode2,
			final long[] counts,
			final TIntIntHashMap neighborToEdgeIndexMap,
			final Function f,
			final Edge e1,
			final Edge e2,
			final EdgeMerger merger )
	{
		// TODO something still seems to be broken here, fix it!
		for ( final TIntIterator it = incidentEdges.iterator(); it.hasNext(); )
		{
			final int k = it.next();
			e1.setIndex( k );

			final int from = ( int ) e1.from();
			final int to = ( int ) e1.to();
			final int otherIndex = mergedNode1 == from ? to : from;

			final long multiplicity = e1.multiplicity();

			nodeEdgeMap[ otherIndex ].remove( k );

			if ( e1.weight() == -1 )
				continue;

			if ( otherIndex == mergedNode2 )
				continue;

			final long c1 = counts[ newNode ];
			final long c2 = counts[ otherIndex ];
//			edges[ k ] = -1;
			final double aff = e1.affinity();
			final double w = f.weight( aff, c1, c2 );


			if ( neighborToEdgeIndexMap.contains( otherIndex ) ) {
				final int edgeIndex = neighborToEdgeIndexMap.get( otherIndex );
				e2.setIndex( edgeIndex );
				e2.multiplicity( e1.multiplicity() + multiplicity );
				final double currentW = e2.weight();
				// why is this check necessary?
				if ( currentW != -1 )
					merger.merge( e1, e2 );
			} else {
				final int newEdgeIndex = e1.size();
				neighborToEdgeIndexMap.put( newNode, newEdgeIndex );
				neighborToEdgeIndexMap.put( otherIndex, newEdgeIndex );
				e1.add( w, aff, newNode, otherIndex, multiplicity );
				e1.setIndex( newEdgeIndex );
			}

			e1.weight( -1 );

		}
	}

	public static double ltd( final long l )
	{
		return Double.longBitsToDouble( l );
	}

	public static long dtl( final double d )
	{
		return Double.doubleToLongBits( d );
	}

	public static void main( final String[] args ) throws Exception
	{

		final TLongLongHashMap counts = new TLongLongHashMap(
				new long[] { 10, 11, 12, 13, 14, 15 },
				new long[] { 15, 20, 1, 2, 16, 4000 } );

		final double[] affinities = new double[] {
				Math.min( counts.get( 10 ), counts.get( 11 ) ) / ( 0.9 * 0.9 ), 0.9, ltd( 10 ), ltd( 11 ), ltd( 1 ),
				Math.min( counts.get( 11 ), counts.get( 12 ) ) / ( 0.1 * 0.1 ), 0.1, ltd( 11 ), ltd( 12 ), ltd( 1 ),
				Math.min( counts.get( 12 ), counts.get( 13 ) ) / ( 0.1 * 0.1 ), 0.1, ltd( 13 ), ltd( 12 ), ltd( 1 ),
				Math.min( counts.get( 10 ), counts.get( 13 ) ) / ( 0.4 * 0.4 ), 0.4, ltd( 13 ), ltd( 10 ), ltd( 1 ),
				Math.min( counts.get( 11 ), counts.get( 13 ) ) / ( 0.8 * 0.8 ), 0.8, ltd( 11 ), ltd( 13 ), ltd( 1 ),
				Math.min( counts.get( 11 ), counts.get( 14 ) ) / ( 0.9 * 0.9 ), 0.9, ltd( 11 ), ltd( 14 ), ltd( 1 ),
				Math.min( counts.get( 10 ), counts.get( 15 ) ) / ( 0.2 * 0.2 ), 0.2, ltd( 10 ), ltd( 15 ), ltd( 1 )
		};

		final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
		borderNodes.put( 10, new TLongHashSet( new long[] { 1 } ) );
		borderNodes.put( 11, new TLongHashSet( new long[] { 2 } ) );

		final TLongLongHashMap outsideNodes = new TLongLongHashMap(
				new long[] { 14, 15 },
				new long[] { 2, 1 } );

		final EdgeMerger merger = DEFAULT_EDGE_MERGER;
		final In in = new In( new UndirectedGraph( new TDoubleArrayList( affinities ), merger ), counts, borderNodes, outsideNodes );
		final Tuple2< Long, In > input = new Tuple2<>( 0l, in );



		final AtomicLong currentId = new AtomicLong( 16 );
		final IdService idService = numIds -> currentId.getAndAdd( numIds );

		final TLongArrayList merges = new TLongArrayList();
		final MergerService mergerService = ( n1, n2, n, w ) -> {
			merges.add( n1 );
			merges.add( n2 );
			merges.add( n );
			merges.add( Double.doubleToLongBits( w ) );
		};

		final MergeBlocPairFunction2 mergeBloc = new MergeBlocPairFunction2( ( a, c1, c2 ) -> Math.min( c1, c2 ) / ( a * a ), merger, 180, idService, mergerService );
		final Tuple2< Tuple2< Long, Long >, Out > test = mergeBloc.call( input );

		System.out.println( "INDEX " + test._1() );
		System.out.println( "EDGES " + test._2().g.edges() );
		System.out.println( "COUNT " + test._2().counts );
		System.out.println( "ASSGN " + test._2().assignments );
		System.out.println( "BORDR " + test._2().borderNodes );

		for ( int i = 0; i < merges.size(); i += 4 )
			System.out.println( merges.get( i ) + " " + merges.get( i + 1 ) + " " + merges.get( i + 2 ) + " " + Double.longBitsToDouble( merges.get( i + 3 ) ) );

	}

}
