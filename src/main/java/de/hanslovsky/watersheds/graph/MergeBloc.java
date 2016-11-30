package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.function.PairFunction;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;
import scala.Tuple3;

public class MergeBloc
{

	public static class In implements Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 8290046566497620074L;

		public final TDoubleArrayList edges;

		public final TLongLongHashMap counts;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public In( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongObjectHashMap< TLongHashSet > borderNodes )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.borderNodes = borderNodes;
		}

	}

	public static class Out implements Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -3490542120283388985L;

		public final TDoubleArrayList edges;

		public final TLongLongHashMap counts;

		public final TLongObjectHashMap< TLongHashSet > borderNodes;

		public final TLongLongHashMap assignments;

		public final TLongHashSet fragmentPointedToOutside;

		public Out(
				final TDoubleArrayList edges,
				final TLongLongHashMap counts,
				final TLongObjectHashMap< TLongHashSet > borderNodes,
				final TLongLongHashMap assignments,
				final long... fragmentPointedToOutside )
		{
			this( edges, counts, borderNodes, assignments, new TLongHashSet( fragmentPointedToOutside ) );
		}

		public Out(
				final TDoubleArrayList edges,
				final TLongLongHashMap counts,
				final TLongObjectHashMap< TLongHashSet > borderNodes,
				final TLongLongHashMap assignments,
				final TLongHashSet fragmentPointedToOutside )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.borderNodes = borderNodes;
			this.assignments = assignments;
			this.fragmentPointedToOutside = fragmentPointedToOutside;
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
//		final double w1 = e1.weight();
//		final double w2 = e2.weight();
//		if ( w1 < w2 )
//		{
//			e2.weight( w1 );
//			e2.affinity( e1.affinity() );
//		}
		e2.affinity( Math.max( e1.affinity(), e2.affinity() ) );
		e2.multiplicity( e1.multiplicity() + e2.multiplicity() );

		return e2;
	};

	public static class MergeBlocPairFunction2 implements PairFunction< Tuple2< Long, In >, Tuple2< Long, TLongHashSet >, Out >
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
		public Tuple2< Tuple2< Long, TLongHashSet >, Out > call( final Tuple2< Long, In > t ) throws Exception
		{
			final In in = t._2();

			final IntHeapPriorityQueue queue = new IntHeapPriorityQueue( new EdgeComparator( in.edges ) );
			final Edge e = new Edge( in.edges );
			for ( int i = 0; i < e.size(); ++i )
				queue.enqueue( i );

			boolean borderNodeIsInvolved = false;
			TLongHashSet involvedNeighboringBlocks = null;
			double maxWeightBeforeMerge = Double.NaN;
			long borderNodeLabel = -1;

			final UndirectedGraph g = new UndirectedGraph( in.edges, merger );
			final TLongLongHashMap assignments = new TLongLongHashMap();
			for ( final TLongIterator it = g.nodeEdgeMap().keySet().iterator(); it.hasNext(); ) {
				final long k = it.next();
				assignments.put( k, k );
			}

			while ( !queue.isEmpty() )
			{
				final int next = queue.dequeueInt();
				e.setIndex( next );
				final double w = e.weight();
				System.out.println( t._1() + ": " + next + " .. " + w + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() + " " + threshold + " " + borderNodeIsInvolved );
				if ( w < 0 )
					continue;

				else if ( w > threshold || borderNodeIsInvolved && w > maxWeightBeforeMerge )
					break;

				final int from = ( int ) e.from();
				final int to = ( int ) e.to();

				if ( in.borderNodes.contains( from ) )
				{
					System.out.println( "bn from " + from );
					if ( !borderNodeIsInvolved )
					{
						borderNodeIsInvolved = true;
						involvedNeighboringBlocks = in.borderNodes.get( from );
						maxWeightBeforeMerge = w;
						borderNodeLabel = from;
					}
				}
				else if ( in.borderNodes.contains( to ) )
				{
					System.out.println( "bn to " + from );
					if ( !borderNodeIsInvolved ) {
						borderNodeIsInvolved = true;
						involvedNeighboringBlocks = in.borderNodes.get( to );
						maxWeightBeforeMerge = w;
						borderNodeLabel = to;
					}
				}
				else
				{

//					System.out.println( "Requesting next id " + t._1() );
					final long n = idService.requestIds( 1 );
					System.out.println( "Merging " + from + " and " + to + " into " + n );
//					System.out.println( "Got new id: " + n + " " + t._1() );
//					System.out.println( "Merging " + from + " and " + to + " into " + n );

					final long c1 = in.counts.remove( from );
					final long c2 = in.counts.remove( to );
					final long cn = c1 + c2;
					in.counts.put( n, cn );
					final TLongIntHashMap newEdges = g.contract( next, n, in.counts );

					if ( newEdges == null )
					{
						in.counts.remove( n );
						continue;
					}


					for ( final TIntIterator it = newEdges.valueCollection().iterator(); it.hasNext(); )
					{
						final int id = it.next();
						e.setIndex( id );
						if ( e.weight() < threshold )
							//							System.out.println( "Adding new edge " + id + " " + e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
							queue.enqueue( id );
					}
//					if ( from == 11 || to == 11 )
//						System.out.println( "ASSGN BEFORE " + assignments );
					assignments.put( from, n );
					assignments.put( to, n );
					assignments.put( n, n );
//					if ( from == 11 || to == 11 )
//						System.out.println( "ASSGN AFTER " + assignments );
					mergerService.addMerge( from, to, n, w );

				}

			}
//			System.out.println( "Done merging locally... " + t._1() + " " + t._2().counts );


//			double[] resultEdges = new double[0];
			final TDoubleArrayList resultEdges = new TDoubleArrayList();
			final Edge re = new Edge( resultEdges );
			for ( int k = 0; k < e.size(); ++k )
			{
				e.setIndex( k );
				final double w = e.weight();
				if ( w == -1.0d )
					continue;

				re.add( w, e.affinity(), e.from(), e.to(), e.multiplicity() );

			}

			final TLongLongHashMap resultAssignments = new TLongLongHashMap();
			for ( final TLongIterator k = assignments.keySet().iterator(); k.hasNext(); )
			{
				final long id = k.next();
				final long r = findRoot( assignments, id );
			}

			final Out result = new Out(
					resultEdges,
					in.counts,
					in.borderNodes,
					assignments,
					borderNodeLabel );
//			System.out.println( "Returning result " + t._1() );
			return new Tuple2<>( new Tuple2<>( t._1(), involvedNeighboringBlocks == null ? new TLongHashSet() : involvedNeighboringBlocks ), result );


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

	public static class MergeBlocPairFunction implements PairFunction< Tuple2< Long, EdgesAndCounts >, Tuple2< Long, Long >, EdgesAndCounts >
	{

		private static final long serialVersionUID = -1537751845300461154L;

		private final Function f;

		private final EdgeMerger merger;

		private final double threshold;

		private final IdService idService;

		private final MergerService mergerService;

		public MergeBlocPairFunction(
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
		public Tuple2< Tuple2< Long, Long >, EdgesAndCounts > call( final Tuple2< Long, EdgesAndCounts > t ) throws Exception
		{
			final EdgesAndCounts edgesAndWeights = t._2();
			final int numberOfNodesIncludingOutside = edgesAndWeights.counts.size();
			final int numberOfOutsideNodes = edgesAndWeights.outside.size();
			final int numberOfNodes = numberOfNodesIncludingOutside - numberOfOutsideNodes;
			final long[] counts = new long[ numberOfNodesIncludingOutside ];
			final TDoubleArrayList edges = new TDoubleArrayList( new double[ edgesAndWeights.edges.length ] );
			final IntHeapPriorityQueue queue = new IntHeapPriorityQueue( new EdgeComparator( edges ) );
			final Tuple3< TLongIntHashMap, long[], TIntHashSet[] > mappings =
					mapToContiguousZeroBasedIndices( edgesAndWeights, edges, counts, queue, f, edgesAndWeights.outside );

			final TLongIntHashMap fw = mappings._1();
			final long[] bw = mappings._2();
			final long[] newIds = bw.clone();
			final TIntHashSet[] nodeEdgeMap = mappings._3();

			final Edge e = new Edge( edges );
			final Edge e1 = new Edge( edges );
			final Edge e2 = new Edge( edges );

//			for ( int k = 0; k < edges.size(); k += 4 )
//				System.out.println( edges.get( k ) + " " + edges.get( k + 1 ) + " " +
//						dtl( edges.get( k + 2 ) ) + " " + dtl( edges.get( k + 3 ) ) + " WAAAS ? " );

			final TIntLongHashMap outside = new TIntLongHashMap();
			System.out.println( "FW " + fw + " " + t._1() );
			System.out.println( "BW " + Arrays.toString( bw ) + " " + t._1() );
			for ( final TLongLongIterator it = edgesAndWeights.outside.iterator(); it.hasNext(); )
			{
				it.advance();
				outside.put( fw.get( it.key() ), it.value() );
			}
			System.out.println( "OUTSIDE " + t._1() + " " + edgesAndWeights.outside + " " + outside );

			boolean pointsOutside = false;
			long pointedToOutside = -1;
			double outsideEdgeWeight = Double.NaN;

			final int[] parents = new int[ numberOfNodes ];
			final TLongLongHashMap assignments = edgesAndWeights.assignments;
//
//			for ( int i = 0; i < edgesAndWeights.assignments.length; i += EdgesAndCounts.ASSIGNMENTS_STEP )
//				assignments.put( edgesAndWeights.assignments[i], edgesAndWeights.assignments[i+1] );

			for ( int i = 0; i < numberOfNodes; ++i )
				parents[ i ] = fw.get( assignments.get( bw[ i ] ) );

			while ( !queue.isEmpty() )
			{
				final int next = queue.dequeueInt();
				e.setIndex( next );
				final double w = e.weight();
//				System.out.println( next + " .. " + w + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );

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
					System.out.println( Arrays.toString( parents ) );
					parents[ to ] = from;

//					System.out.println( "Requesting next id " + t._1() );
					final long n = idService.requestIds( 1 );
//					System.out.println( "Got new id: " + n + " " + t._1() );

					final long n2 = newIds[ to ];
					newIds[ to ] = n;

					final long n1 = newIds[ from ];
					newIds[ from ] = n;

					mergerService.addMerge( n1, n2, n, w );

				}

			}

			System.out.println( "Done merging locally... " + t._1() + " " + parents.length + " " + Arrays.toString( parents ) );

//			System.out.println( Arrays.toString( parents ) );
			// circular graph here!
			final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], parents.length );
			for ( int i = 0; i < parents.length; ++i )
				dj.findRoot( i );
			System.out.println( "Found roots... " + t._1() );
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

				re.add( w, e.affinity(), newIds[ ( int ) e.from() ], newIds[ ( int ) e.to() ], e.multiplicity() );

			}

			System.out.println( "Added edges... " + t._1() );



//			final long[] resultAssignments = new long[ numberOfNodes * EdgesAndCounts.ASSIGNMENTS_STEP ];
			final TLongLongHashMap resultAssignments = new TLongLongHashMap();
			for ( int i = 0, k = 0; i < numberOfNodes; ++i, k += EdgesAndCounts.ASSIGNMENTS_STEP )
			{
				final long id = bw[ i ];
				resultAssignments.put( id, newIds[ parents[ i ] ] );
//				resultAssignments[ k ] = id;
//				resultAssignments[ k + 1 ] = newIds[ parents[ i ] ];
			}

			final int setCount = dj.setCount();
//			final long[] resultCounts = new long[ setCount * EdgesAndCounts.COUNTS_STEP ];

			final TLongLongHashMap resultCounts = new TLongLongHashMap();
			System.out.println( "CNTS " + Arrays.toString( counts ) + " " + setCount + " " + t._1() );
			System.out.println( counts.length + " " + parents.length );

			for ( int i = 0; i < parents.length; ++i )
			{
				final long c = counts[ i ];
				if ( c > 0 )
				{
					final long id = newIds[ parents[ i ] ];
					resultCounts.put( id, c );
//					resultCounts.add( id );
//					resultCounts.add( c );
//					resultCounts[ k ] = id;
//					resultCounts[ k + 1 ] = c;
//					++i;
				}
			}
			System.out.println( "Added parents" );

			final EdgesAndCounts result = new EdgesAndCounts(
					resultEdges.toArray(),
					resultCounts,
					edgesAndWeights.outside,
					resultAssignments );
			System.out.println( "Returning result " + t._1() );
			return new Tuple2<>( new Tuple2<>( t._1(), pointedToOutside ), result );
		}

	}

	public static Tuple3< TLongIntHashMap, long[], TIntHashSet[] > mapToContiguousZeroBasedIndices2(
			final In edgesAndWeights,
			final TDoubleArrayList edges,
			final long[] counts,
			final IntHeapPriorityQueue queue,
			final Function f,
			final TLongLongHashMap outside
			) {
		final TLongIntHashMap mappingToZeroBasedIndexSet = new TLongIntHashMap();
		final long[] mappingToOriginalIndexSet = new long[ counts.length ];

		final TLongLongIterator it = edgesAndWeights.counts.iterator();
		for ( int i = 0, k = counts.length - outside.size(); it.hasNext(); )
		{
			it.advance();
			final long index = it.key();
			final int target = outside.contains( index ) ? k++ : i++;
//			if ( outside.contains( index )) {
//				target = k;
//				++k
//			}
			mappingToOriginalIndexSet[ target ] = index;
			counts[ target ] = it.value();
			mappingToZeroBasedIndexSet.put( index, target );
		}

//		for ( int i = edgesAndWeights.counts.length / EdgesAndCounts.COUNTS_STEP, k = 0; i < counts.length; ++i, k += EdgesAndCounts.OUTSIDE_STEP )
//		{
//			final long index = edgesAndWeights.outside[ k ];
//			mappingToOriginalIndexSet[ i ] = index;
//			mappingToZeroBasedIndexSet.put( index, i );
//			counts[ i ] = edgesAndWeights.outside[ k + 1 ];
//		}
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
			f.weight( a, counts[ i1 ], counts[ i2 ] );
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

	public static Tuple3< TLongIntHashMap, long[], TIntHashSet[] > mapToContiguousZeroBasedIndices(
			final EdgesAndCounts edgesAndWeights,
			final TDoubleArrayList edges,
			final long[] counts,
			final IntHeapPriorityQueue queue,
			final Function f,
			final TLongLongHashMap outside
			) {
		final TLongIntHashMap mappingToZeroBasedIndexSet = new TLongIntHashMap();
		final long[] mappingToOriginalIndexSet = new long[ counts.length ];

		final TLongLongIterator it = edgesAndWeights.counts.iterator();
		for ( int i = 0, k = counts.length - outside.size(); it.hasNext(); )
		{
			it.advance();
			final long index = it.key();
			final int target = outside.contains( index ) ? k++ : i++;
//			if ( outside.contains( index )) {
//				target = k;
//				++k
//			}
			mappingToOriginalIndexSet[ target ] = index;
			counts[ target ] = it.value();
			mappingToZeroBasedIndexSet.put( index, target );
		}

//		for ( int i = edgesAndWeights.counts.length / EdgesAndCounts.COUNTS_STEP, k = 0; i < counts.length; ++i, k += EdgesAndCounts.OUTSIDE_STEP )
//		{
//			final long index = edgesAndWeights.outside[ k ];
//			mappingToOriginalIndexSet[ i ] = index;
//			mappingToZeroBasedIndexSet.put( index, i );
//			counts[ i ] = edgesAndWeights.outside[ k + 1 ];
//		}
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
			f.weight( a, counts[ i1 ], counts[ i2 ] );
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
						e1.from() + " " + e1.to() + " MULT " + e1.multiplicity() + " " + multiplicity );
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

		final Edge e = new Edge( new TDoubleArrayList( affinities ) );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			System.out.println( e.from() + " " + e.to() + " " + e.weight() + " " + e.affinity() + " " + e.multiplicity() );
		}
//		System.exit( 234 );

		final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap< TLongHashSet >();
		borderNodes.put( 14, new TLongHashSet( new long[] { 2 } ) );
		borderNodes.put( 15, new TLongHashSet( new long[] { 3 } ) );
//				new long[] { 14, 15 },
//				new long[] { 2, 3 } );

//		final long[] outside = new long[] {
//				14, 15, 2,
//				15, 4000, 3
//		};

		final In in = new In( new TDoubleArrayList( affinities ), counts, borderNodes );
		final Tuple2< Long, In > input = new Tuple2<>( 0l, in );

		final EdgeMerger merger = DEFAULT_EDGE_MERGER;


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
		final Tuple2< Tuple2< Long, TLongHashSet >, Out > test = mergeBloc.call( input );

		System.out.println( "INDEX " + test._1() );
		System.out.println( "EDGES " + test._2().edges );
		System.out.println( "COUNT " + test._2().counts );
		System.out.println( "ASSGN " + test._2().assignments );
		System.out.println( "BORDR " + test._2().borderNodes );

		for ( int i = 0; i < merges.size(); i += 4 )
			System.out.println( merges.get( i ) + " " + merges.get( i + 1 ) + " " + merges.get( i + 2 ) + " " + Double.longBitsToDouble( merges.get( i + 3 ) ) );

	}

}
