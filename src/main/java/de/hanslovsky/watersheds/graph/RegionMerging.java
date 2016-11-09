package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.function.PairFunction;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TIntHashSet;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;
import scala.Tuple3;

public class RegionMerging
{

	public static class EdgesAndCounts implements Serializable {

		private static final long serialVersionUID = -5524785343491327810L;

		public final double[] edges;

		public final long[] counts;

		public final long[] outside;

		public final long[] assignments;

		public EdgesAndCounts( final double[] edges, final long[] counts, final long[] outside, final long[] assignments )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.outside = outside;
			this.assignments = assignments;
		}

	}

	public static interface Function
	{

		double weight( double affinity, long count1, long count2 );
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

	public static interface EdgeMerger
	{
		// writes into e2
		Edge merge( final Edge e1, final Edge e2 );
//		default Edge merge( final Edge e1, final Edge e2 )
//		{
//			final double w1 = e1.weight();
//			final double w2 = e2.weight();
//			if ( w1 < w2 )
//			{
//				e2.weight( w1 );
//				e2.affinity( e1.affinity() );
//			}
//
//			return e2;
//		}
	}

	public static class MergeBloc implements PairFunction< Tuple2< Long, EdgesAndCounts >, Tuple2< Long, Long >, EdgesAndCounts >
	{

		private static final long serialVersionUID = -1537751845300461154L;

		private final Function f;

		private final EdgeMerger merger;

		private final double threshold;

		public MergeBloc( final Function f, final EdgeMerger merger, final double threshold )
		{
			super();
			this.f = f;
			this.merger = merger;
			this.threshold = threshold;
		}

		@Override
		public Tuple2< Tuple2< Long, Long >, EdgesAndCounts > call( final Tuple2< Long, EdgesAndCounts > t ) throws Exception
		{
			final EdgesAndCounts edgesAndWeights = t._2();
			final int numberOfNodes = edgesAndWeights.counts.length / 2;
			final int numberOfOutsideNodes = edgesAndWeights.outside.length / 3;
			final long[] counts = new long[ numberOfNodes + numberOfOutsideNodes ];
			final TDoubleArrayList edges = new TDoubleArrayList( new double[ edgesAndWeights.edges.length ] );
			final IntHeapPriorityQueue queue = new IntHeapPriorityQueue( new EdgeComparator( edges ) );
			final Tuple3< TLongIntHashMap, long[], TIntHashSet[] > mappings =
					mapToContiguousZeroBasedIndices( edgesAndWeights, edges, counts, queue, f );

			final TLongIntHashMap fw = mappings._1();
			final long[] bw = mappings._2();
			final TIntHashSet[] nodeEdgeMap = mappings._3();

			final Edge e = new Edge( edges );
			final Edge e1 = new Edge( edges );
			final Edge e2 = new Edge( edges );

//			for ( int k = 0; k < edges.size(); k += 4 )
//				System.out.println( edges.get( k ) + " " + edges.get( k + 1 ) + " " +
//						dtl( edges.get( k + 2 ) ) + " " + dtl( edges.get( k + 3 ) ) + " WAAAS ? " );

			final TIntLongHashMap outside = new TIntLongHashMap();
			for ( int i = 0; i < edgesAndWeights.outside.length; i += 3 )
				outside.put( fw.get( edgesAndWeights.outside[ i ] ), edgesAndWeights.outside[ i + 2 ] );

			boolean pointsOutside = false;
			long pointedToOutside = -1;
			double outsideEdgeWeight = Double.NaN;

			final int[] parents = new int[ numberOfNodes ];
			final TLongLongHashMap assignments = new TLongLongHashMap();

			for ( int i = 0; i < edgesAndWeights.assignments.length; i += 2 )
				assignments.put( edgesAndWeights.assignments[i], edgesAndWeights.assignments[i+1] );

			for ( int i = 0; i < numberOfNodes; ++i )
				parents[ i ] = fw.get( assignments.get( bw[ i ] ) );

			while ( !queue.isEmpty() )
			{
				final int next = queue.dequeueInt();
				e.setIndex( next );
				final double w = e.weight();
				System.out.println( next + " .. " + w + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );

				if ( w < 0 )
					continue;

				else if ( w > threshold || pointsOutside && w > outsideEdgeWeight )
					break;

				final int from = ( int ) e.from();
				final int to = ( int ) e.to();

				if ( outside.contains( from ) )
				{
					if ( !pointsOutside )
					{
						pointsOutside = true;
						pointedToOutside = outside.get( from );
						outsideEdgeWeight = w;
					}
				}
				else if ( outside.contains( to ) )
				{
					if ( !pointsOutside ) {
						pointsOutside = true;
						pointedToOutside = outside.get( to );
						outsideEdgeWeight = w;
					}
				}
				else
				{
					mergeEdges( from, to, nodeEdgeMap, edges, queue, counts, f, e1, e2, merger );

					final long c1 = counts[ from ];
					final long c2 = counts[ to ];
					counts[ from ] += c2;
					counts[ to ] = 0;
					parents[ to ] = from;

				}

			}

//			System.out.println( Arrays.toString( parents ) );
			final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], 1 );
			for ( int i = 0; i < parents.length; ++i )
				dj.findRoot( i );
//			System.out.println( Arrays.toString( parents ) );
//			System.out.println( Arrays.toString( bw ) );

//			double[] resultEdges = new double[0];
			final TDoubleArrayList resultEdges = new TDoubleArrayList();
			final Edge re = new Edge( resultEdges );
			for ( int k = 0; k < e.size(); ++k )
			{
				e.setIndex( k );
				final double w = e.weight();
				if ( w == -1 )
					continue;

				re.add( w, e.affinity(), bw[ ( int ) e.from() ], bw[ ( int ) e.to() ], e.multiplicity() );

			}


			final long[] resultAssignments = new long[ numberOfNodes * 2 ];
			final long[] resultCounts = new long[ numberOfNodes * 2 ];
			for ( int i = 0, k = 0; i < numberOfNodes; ++i, k += 2 )
			{
				final long id = bw[ i ];
				resultAssignments[ k ] = id;
				resultAssignments[ k + 1 ] = bw[ parents[ i ] ];

				resultCounts[ k ] = id;
				resultCounts[ k + 1 ] = counts[ i ];

			}

			final EdgesAndCounts result = new EdgesAndCounts( resultEdges.toArray(), resultCounts, edgesAndWeights.outside, resultAssignments );
			return new Tuple2<>( new Tuple2<>( t._1(), pointedToOutside ), result );
		}

	}

	public static Tuple3< TLongIntHashMap, long[], TIntHashSet[] > mapToContiguousZeroBasedIndices(
			final EdgesAndCounts edgesAndWeights,
			final TDoubleArrayList edges,
			final long[] counts,
			final IntHeapPriorityQueue queue,
			final Function f
			) {
		final TLongIntHashMap mappingToZeroBasedIndexSet = new TLongIntHashMap();
		final long[] mappingToOriginalIndexSet = new long[ counts.length ];

		for ( int i = 0, k = 0; k < edgesAndWeights.counts.length; ++i, k += 2 )
		{
			final long index = edgesAndWeights.counts[ k ];
			mappingToOriginalIndexSet[ i ] = index;
			counts[ i ] = edgesAndWeights.counts[ k + 1 ];
			mappingToZeroBasedIndexSet.put( index, i );
		}

		for ( int i = edgesAndWeights.counts.length / 2, k = 0; i < counts.length; ++i, k += 3 )
		{
			final long index = edgesAndWeights.outside[ k ];
			mappingToOriginalIndexSet[ i ] = index;
			mappingToZeroBasedIndexSet.put( index, i );
			counts[ i ] = edgesAndWeights.outside[ k + 1 ];
		}
		// TDoubleArrayList creates a copy. SUPER ANNOYING!
		final Edge globalIndexingEdge = new Edge( new TDoubleArrayList( edgesAndWeights.edges ) );
		final Edge localIndexingEdge = new Edge( edges );

		final TIntHashSet[] nodeEdgeMap = new TIntHashSet[ counts.length ];
		for ( int i = 0; i < nodeEdgeMap.length; ++i )
			nodeEdgeMap[ i ] = new TIntHashSet();

		for ( int k = 0; k < globalIndexingEdge.size(); ++k )
		{
			globalIndexingEdge.setIndex( k );
			localIndexingEdge.setIndex( k );
			final double w = globalIndexingEdge.weight();
			final double a = globalIndexingEdge.affinity();
			final int i1 = mappingToZeroBasedIndexSet.get( globalIndexingEdge.from() );
			final int i2 = mappingToZeroBasedIndexSet.get( globalIndexingEdge.to() );
			localIndexingEdge.weight( Double.isNaN( w ) ? f.weight( a, counts[ i1 ], counts[ i2 ] ) : w );
			localIndexingEdge.affinity( a );
			localIndexingEdge.from( i1 );
			localIndexingEdge.to( i2 );

			queue.enqueue( k );
			nodeEdgeMap[ i1 ].add( k );
			nodeEdgeMap[ i2 ].add( k );
		}

		return new Tuple3<>( mappingToZeroBasedIndexSet, mappingToOriginalIndexSet, nodeEdgeMap );
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

//		System.out.println( edges );

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
				if ( currentW != -1 ) {
//					e1.weight( w );
//					e1.affinity( aff );
					final Edge e = merger.merge( e1, e2 );
					System.out.println( "Modified edge " + edgeIndex + ": " + e.weight() + " " + e.affinity() + " " +
							e.from() + " " + e.to() + " " + e.multiplicity() );
				}
			} else {
				final int newEdgeIndex = e1.size();
				neighborToEdgeIndexMap.put( newNode, newEdgeIndex );
				neighborToEdgeIndexMap.put( otherIndex, newEdgeIndex );
				e1.add( w, aff, newNode, otherIndex, multiplicity );
				e1.setIndex( newEdgeIndex );
				System.out.println( "Added edge " + newEdgeIndex + ": " + e1.weight() + " " + e1.affinity() + " " +
						e1.from() + " " + e1.to() + " " + e1.multiplicity() );
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
		final double[] affinities = new double[] {
				Double.NaN, 0.9, ltd( 10 ), ltd( 11 ), 1,
				Double.NaN, 0.1, ltd( 11 ), ltd( 12 ), 1,
				Double.NaN, 0.1, ltd( 13 ), ltd( 12 ), 1,
				Double.NaN, 0.4, ltd( 13 ), ltd( 10 ), 1,
				Double.NaN, 0.8, ltd( 11 ), ltd( 13 ), 1,
				Double.NaN, 0.9, ltd( 11 ), ltd( 14 ), 1,
				Double.NaN, 0.2, ltd( 10 ), ltd( 15 ), 1
		};

		final long[] counts = new long[] {
				10, 15,
				11, 20,
				12, 1,
				13, 2,
		};

		final long[] assignments = new long[] {
				10, 10,
				11, 11,
				12, 12,
				13, 13
		};

		final long[] outside = new long[] {
				14, 15, 2,
				15, 4000, 3
		};

		final EdgesAndCounts eac = new EdgesAndCounts( affinities, counts, outside, assignments );
		final Tuple2< Long, EdgesAndCounts > input = new Tuple2<>( 1l, eac );

//		final EdgeMerger merger = new EdgeMerger()
//		{};
		final EdgeMerger merger = ( e1, e2 ) -> {
			final long m1 = e1.multiplicity();
			final long m2 = e2.multiplicity();
			final long m = m1 + m2;
			final double w = ( e1.weight() * m1 + e2.weight() * m2 ) / m;
			final double a = ( e1.affinity() * m1 + e2.affinity() * m2 ) / m;
			e2.weight( w );
			e2.affinity( a );
			return e2;
		};

		final MergeBloc mergeBloc = new MergeBloc( ( a, c1, c2 ) -> Math.min( c1, c2 ) / ( a * a ), merger, 180 );
		final Tuple2< Tuple2< Long, Long >, EdgesAndCounts > test = mergeBloc.call( input );

		System.out.println( test._1() );
		System.out.println( Arrays.toString( test._2().edges ) );
		System.out.println( Arrays.toString( test._2().counts ) );
		System.out.println( Arrays.toString( test._2().assignments ) );
		System.out.println( Arrays.toString( test._2().outside ) );
	}

}
