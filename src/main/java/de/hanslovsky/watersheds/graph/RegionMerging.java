package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
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

		public double[] edges;

		public long[] counts;

		public long[] outside;

		public EdgesAndCounts( final double[] edges, final long[] counts, final long[] outside )
		{
			super();
			this.edges = edges;
			this.counts = counts;
			this.outside = outside;
		}

	}

	final static int edgeSize = 3;

	public static void run(
			final JavaSparkContext sc,
			final JavaPairRDD< Long, Tuple3< double[], long[], long[] > > input,
			final double threshold )
	{
		input.mapToPair( t -> {
			final long idx = t._1();
			final double[] edgesWithAffinities = t._2()._1();

			final TLongLongHashMap pointingOutside = new TLongLongHashMap();
			final long[] outsidePointsList = t._2()._2();
			final long[] countsArray = t._2()._3();

			for ( int i = 0; i < outsidePointsList.length; i += 2 )
				pointingOutside.put( outsidePointsList[i],  outsidePointsList[i+1] );

			final TLongLongHashMap counts = new TLongLongHashMap();

			final TLongLongHashMap parents = new TLongLongHashMap();
			final TLongLongHashMap ranks = new TLongLongHashMap();

			final TLongObjectHashMap< TIntArrayList > nodeEdgeMap = new TLongObjectHashMap<>();
			for ( int i = 0; i < countsArray.length; ++i )
			{
				final long id = countsArray[ i ];
				counts.put( id, countsArray[ i + 1 ] );
				nodeEdgeMap.put( id, new TIntArrayList() );
				parents.put( id, id );
				ranks.put( id, 0 );
			}

			final HashSetsDisjointSets unionFind = new HashSetsDisjointSets( parents, ranks, parents.size() );

			final TDoubleArrayList edges = new TDoubleArrayList();
			for ( int i = 0; i < edgesWithAffinities.length; i += 3 )
			{
				final double aff = edgesWithAffinities[ i ];
				final double id1 = edgesWithAffinities[ i + 1 ];
				final double id2 = edgesWithAffinities[ i + 2 ];
				final long c1 = counts.get( Double.doubleToLongBits( id1 ) );
				final long c2 = counts.get( Double.doubleToLongBits( id2 ) );

				final double weight = aff; // Math.min( c1, c2 ) / ( aff * aff
				// );

				edges.add( weight );
				edges.add( id1 );
				edges.add( id2 );
			}

			final IntComparator comparator = new IntComparator()
			{

				@Override
				public int compare( final Integer o1, final Integer o2 )
				{
					return compare( o1.intValue(), o2.intValue() );
				}

				@Override
				public int compare( final int k1, final int k2 )
				{
					final double aff1 = edges.get( k1 );
					final double aff2 = edges.get( k2 );
					final long c1 = Math.min( counts.get( ( long ) edges.get( k1 + 1 ) ), counts.get( ( long ) edges.get( k1 + 2 ) ) );
					final long c2 = Math.min( counts.get( ( long ) edges.get( k2 + 1 ) ), counts.get( ( long ) edges.get( k2 + 2 ) ) );
					return Double.compare( c1 / ( aff1 * aff1 ), c2 / ( aff2 * aff2 ) );
				}
			};
			final IntHeapPriorityQueue queue = new IntHeapPriorityQueue( comparator );


			for ( int i = 0; i < edges.size(); i += edgeSize )
			{
				queue.enqueue( i );

				final long id1 = Double.doubleToLongBits( edges.get( i + 1 ) );
				if ( !pointingOutside.contains( id1 ) )
					nodeEdgeMap.get( id1 ).add( i );

				final long id2 = Double.doubleToLongBits( edges.get( i + 1 ) );
				if ( !pointingOutside.contains( id2 ) )
					nodeEdgeMap.get( id2 ).add( i );
			}



			boolean pointsOutside = false;
			double outsideValue = Double.NaN;
			long outsideIndex = -1;

			while( queue.isEmpty() ) {
				final int next = queue.dequeueInt();

				final double weight = edges.get( next );

				if ( pointsOutside && weight > outsideValue || weight > threshold )
					break;

				final long from = Double.doubleToLongBits( edges.get( next + 1 ) );
				final long to = Double.doubleToLongBits( edges.get( next + 2 ) );

				final TIntArrayList edgeList1 = nodeEdgeMap.get( from );
				final TIntArrayList edgeList2 = nodeEdgeMap.get( to );

				if ( edgeList1 == null || edgeList2 == null )
					continue;

				if ( pointingOutside.contains( from ) )
				{
					pointsOutside = true;
					outsideValue = weight;
					outsideIndex = from;
				}
				else if ( pointingOutside.contains( to ) )
				{
					pointsOutside = true;
					outsideValue = weight;
					outsideIndex = to;
				}
				else
				{
					final long r1 = unionFind.findRoot( from );
					final long r2 = unionFind.findRoot( to );

					if ( r1 == r2 || r1 == 0 || r2 == 0 )
						continue;

					final long r = unionFind.join( r1, r2 );
					final long c1 = counts.get( r1 );
					final long c2 = counts.get( r2 );

					updateCountsAndEdges( r, r == r1 ? r1 : r2, nodeEdgeMap, counts, queue, edges );
				}

			}

			// compress all paths
			for ( final long k : parents.keys() )
				unionFind.findRoot( k );

			final long[] countsOut = new long[ 2 * counts.size() ];
			final TLongLongIterator countsIt = counts.iterator();
			for ( int i = 0; countsIt.hasNext(); ++i ) {
				countsIt.advance();
				countsOut[i] = countsIt.key();
				countsOut[i+1] = countsIt.value();
			}

			final TDoubleArrayList edgesOut = new TDoubleArrayList();


			// TODO Auto-generated method stub
			return null;
		});
	}

	public static void updateCountsAndEdges(
			final long newRoot,
			final long oldRoot,
			final TLongObjectHashMap< TIntArrayList > nodeEdgeMap,
			final TLongLongHashMap counts,
			final IntHeapPriorityQueue queue,
			final TDoubleArrayList edges )
	{
		counts.put( newRoot, counts.get( newRoot ) + counts.get( oldRoot ) );
		counts.remove( oldRoot );

		final TIntArrayList outgoingEdgesFromOldRoot = nodeEdgeMap.remove( oldRoot );
		final TIntArrayList outgoingEdgesFromNewRoot = nodeEdgeMap.remove( newRoot );
		final TIntArrayList newEdges = new TIntArrayList();

		for ( int i = 0; i < outgoingEdgesFromOldRoot.size(); ++i )
		{
			final int index = outgoingEdgesFromOldRoot.get( i );
			final long id1 = Double.doubleToLongBits( edges.get( index+1 ) );
			final long id2 = Double.doubleToLongBits( edges.get( index+2 ) );
			if ( id1 == newRoot && id2 == oldRoot || id1 == oldRoot && id2 == newRoot )
				continue;
			final double w = edges.get( index );
			final int edgeIndex = edges.size() / 3;
			queue.enqueue( edgeIndex );
			edges.add( w );
			edges.add( Double.longBitsToDouble( id1 ) );
			edges.add( Double.longBitsToDouble( id2 ) );
			newEdges.add( edgeIndex );
		}

		for ( int i = 0; i < outgoingEdgesFromNewRoot.size(); ++i )
		{
			final int index = outgoingEdgesFromOldRoot.get( i );
			final long id1 = Double.doubleToLongBits( edges.get( index+1 ) );
			final long id2 = Double.doubleToLongBits( edges.get( index+2 ) );
			if ( id1 == newRoot && id2 == oldRoot || id1 == oldRoot && id2 == newRoot )
				continue;
			final double w = edges.get( index );
			final int edgeIndex = edges.size() / 3;
			queue.enqueue( edgeIndex );
			edges.add( w );
			edges.add( Double.longBitsToDouble( id1 ) );
			edges.add( Double.longBitsToDouble( id2 ) );
			newEdges.add( edgeIndex );
		}

		nodeEdgeMap.put( newRoot, newEdges );

	}

	public static interface Function
	{

		double weight( double affinity, long count1, long count2 );
	}

	public static interface DoubleComparator
	{
		default public int compare( final double d1, final double d2 )
		{
			return Double.compare( d1, d2 );
		}
	}

	public static class EdgeComparator implements IntComparator
	{



		private final DoubleComparator dComp;

		private final TDoubleArrayList weightedEdges;

		public EdgeComparator( final TDoubleArrayList weightedEdges )
		{
			this( new DoubleComparator(){}, weightedEdges );
		}

		public EdgeComparator( final DoubleComparator dComp, final TDoubleArrayList weightedEdges )
		{
			super();
			this.dComp = dComp;
			this.weightedEdges = weightedEdges;
		}

		@Override
		public int compare( final Integer i1, final Integer i2 )
		{
			return compare( i1.intValue(), i2.intValue() );
		}

		@Override
		public int compare( final int k1, final int k2 )
		{
			return dComp.compare( weightedEdges.get( k1 ), weightedEdges.get( k2 ) );
		}

	}

	public static class MergeBloc< K > implements PairFunction< Tuple2< K, EdgesAndCounts >, Tuple2< K, K >, EdgesAndCounts >
	{

		private final Function f;

		private final double threshold;

		public MergeBloc( final Function f, final double threshold )
		{
			super();
			this.f = f;
			this.threshold = threshold;
		}

		@Override
		public Tuple2< Tuple2< K, K >, EdgesAndCounts > call( final Tuple2< K, EdgesAndCounts > t ) throws Exception
		{
			final EdgesAndCounts edgesAndWeights = t._2();
			final double[] affinityEdges = new double[ edgesAndWeights.edges.length ];
			final long[] counts = new long[ edgesAndWeights.counts.length / 2 ];
			final Tuple2< TLongIntHashMap, long[] > mappings = mapToContiguousZeroBasedIndices( edgesAndWeights, affinityEdges, counts );

			final TLongIntHashMap fw = mappings._1();
			final long[] bw = mappings._2();

			final TDoubleArrayList edges = new TDoubleArrayList();
			final IntHeapPriorityQueue queue = new IntHeapPriorityQueue( new EdgeComparator( edges ) );
			final TIntArrayList[] nodeEdgeMap = new TIntArrayList[ counts.length ];
			for ( int i = 0; i < counts.length; ++i )
				nodeEdgeMap[i] = new TIntArrayList();

			for ( int k = 0, i = 0; k < affinityEdges.length; k += 3, i += 4 )
			{
				final int i1 = (int) Double.doubleToLongBits( affinityEdges[ k + 1 ] );
				final int i2 = ( int ) Double.doubleToLongBits( affinityEdges[ k + 2 ] );
				final double a = affinityEdges[ k ];
				final double w = f.weight( a, counts[ i1 ], counts[ i2 ] );
//				if ( w < threshold )
//				{
				edges.add( w );
				edges.add( a );
				edges.add( Double.longBitsToDouble( i1 ) );
				edges.add( Double.longBitsToDouble( i2 ) );
				System.out.println( i + " " + i1 + " " + i2 );
				queue.enqueue( i );
				nodeEdgeMap[ i1 ].add( i );
				nodeEdgeMap[ i2 ].add( i );
//				}
			}

			for ( int k = 0; k < edges.size(); k += 4 )
				System.out.println( edges.get( k ) + " " + edges.get( k + 1 ) + " " +
						Double.doubleToLongBits( edges.get( k + 2 ) ) + " " + Double.doubleToLongBits( edges.get( k + 3 ) ) + " WAAAS ? " );

			final TIntHashSet outside = new TIntHashSet();
			for ( final long o : edgesAndWeights.outside )
				outside.add( fw.get( o ) );

			boolean pointsOutside = false;
			int pointedToOutside = -1;
			double outsideEdgeWeight = Double.NaN;

			final int[] parents = new int[ counts.length ];
			for ( int i = 0; i < parents.length; ++i )
				parents[ i ] = i;

			final TIntArrayList removedEdges = new TIntArrayList();

			while ( !queue.isEmpty() )
			{
				final int next = queue.dequeueInt();
				final double w = edges.get( next );
				System.out.println(
						next + " .. " + w + " " + edges.get( next + 1 ) + " " + Double.doubleToLongBits( edges.get( next + 2 ) ) + " " +
								Double.doubleToLongBits( edges.get( next + 3 ) ) );

				if ( w < 0 )
					continue;

				else if ( w > threshold || pointsOutside && w > outsideEdgeWeight )
					break;

				final int id1 = ( int ) Double.doubleToLongBits( edges.get( next + 2 ) );
				final int id2 = ( int ) Double.doubleToLongBits( edges.get( next + 3 ) );

				if ( outside.contains( id1 ) )
				{
					if ( !pointsOutside )
					{
						pointsOutside = true;
						pointedToOutside = id1;
						outsideEdgeWeight = w;
					}
				}
				else if ( outside.contains( id2 ) )
				{
					if ( !pointsOutside ) {
						pointsOutside = true;
						pointedToOutside = id2;
						outsideEdgeWeight = w;
					}
				}
				else
				{
//					removedEdges.add( next );
					final long c1 = counts[ id1 ];
					final long c2 = counts[ id2 ];
					counts[ id1 ] += c2;
					counts[ id2 ] = 0;
					parents[ id2 ] = id1;

					mergeEdges( id1, id2, nodeEdgeMap, edges, queue, counts, f );

				}

			}

			System.out.println( Arrays.toString( parents ) );
			final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], 1 );
			for ( int i = 0; i < parents.length; ++i )
				dj.findRoot( i );
			System.out.println( Arrays.toString( parents ) );
			System.out.println( Arrays.toString( bw ) );

			return null;
		}

	}

	public static Tuple2< TLongIntHashMap, long[] > mapToContiguousZeroBasedIndices(
			final EdgesAndCounts edgesAndWeights,
			final double[] edges,
			final long[] counts
			) {
		final TLongIntHashMap mappingToZeroBasedIndexSet = new TLongIntHashMap();
		final long[] mappingToOriginalIndexSet = new long[ counts.length ];
		for ( int i = 0, k = 0; i < mappingToOriginalIndexSet.length; ++i, k += 2 )
		{
			final long index = edgesAndWeights.counts[ k ];
			mappingToOriginalIndexSet[ i ] = index;
			counts[ i ] = edgesAndWeights.counts[ k + 1 ];
			mappingToZeroBasedIndexSet.put( index, i );
		}

		for ( int i = 0; i < edges.length; i += 3 )
		{
			edges[i] = edgesAndWeights.edges[i];
			edges[ i + 1 ] = Double.longBitsToDouble( mappingToZeroBasedIndexSet.get( Double.doubleToLongBits( edgesAndWeights.edges[ i + 1 ] ) ) );
			edges[ i + 2 ] = Double.longBitsToDouble( mappingToZeroBasedIndexSet.get( Double.doubleToLongBits( edgesAndWeights.edges[ i + 2 ] ) ) );
		}

		return new Tuple2<>( mappingToZeroBasedIndexSet, mappingToOriginalIndexSet );
	}

	public static void mergeEdges(
			final int keepNode,
			final int discardNode,
			final TIntArrayList[] nodeEdgeMap,
			final TDoubleArrayList edges,
			final IntHeapPriorityQueue queue,
			final long[] counts,
			final Function f )
	{
		final TIntArrayList keepNodeEdges = nodeEdgeMap[ keepNode ];
		final TIntArrayList discardNodeEdges = nodeEdgeMap[ discardNode ];
		final TIntIntHashMap neighborToEdgeIndexMap = new TIntIntHashMap();

		addNeighborEdges( keepNodeEdges, keepNode, keepNode, discardNode, edges, counts, neighborToEdgeIndexMap, f );
		addNeighborEdges( discardNodeEdges, keepNode, discardNode, keepNode, edges, counts, neighborToEdgeIndexMap, f );

		for ( final TIntIntIterator it = neighborToEdgeIndexMap.iterator(); it.hasNext(); )
		{
			it.advance();
			queue.enqueue( it.value() );
		}

		System.exit( 1 );

	}

	public static void addNeighborEdges(
			final TIntArrayList incidentEdges,
			final int newIndex,
			final int oldIndex,
			final int otherNode,
			final TDoubleArrayList edges,
			final long[] counts,
			final TIntIntHashMap neighborToEdgeIndexMap,
			final Function f )
	{
		final int size = incidentEdges.size();
		for ( int i = 0; i < size; ++i ) {
			final int k = incidentEdges.get( i );
			edges.set( k, -1 );
			final int from = ( int ) Double.doubleToLongBits( edges.get( k + 2 ) );
			final int to = ( int ) Double.doubleToLongBits( edges.get( k + 3 ) );
			final int otherIndex = oldIndex == from ? to : from;
			if ( otherIndex == otherNode )
				continue;
			final long c1 = counts[ ( int ) Double.doubleToLongBits( edges.get( k + 2 ) ) ];
			final long c2 = counts[ ( int ) Double.doubleToLongBits( edges.get( k + 3 ) ) ];
//			edges[ k ] = -1;
			final double aff = edges.get( k + 1  );
			final double w = f.weight( aff, c1, c2 );


			if ( neighborToEdgeIndexMap.contains( otherIndex ) ) {
				final int edgeIndex = neighborToEdgeIndexMap.get( otherIndex );
				final double currentW = edges.get( edgeIndex );
				if ( w < currentW )
				{
					edges.set( edgeIndex, w );
					edges.set( edgeIndex + 1, aff );
				}
			} else {
				final int newEdgeIndex = edges.size();
				neighborToEdgeIndexMap.put( otherIndex, newEdgeIndex );
				edges.add( w );
				edges.add( aff );
				edges.add( Double.longBitsToDouble( newIndex ) );
				edges.add( Double.longBitsToDouble( otherIndex ) );
			}

		}
	}

	public static void main( final String[] args ) throws Exception
	{
//		final SparkConf conf = new SparkConf().setAppName( "merge" ).setMaster( "local[*]" );
//		final JavaSparkContext sc = new JavaSparkContext( conf );

		final double[] affinities = new double[] {
				0.9, Double.longBitsToDouble( 10 ), Double.longBitsToDouble( 11 ),
				0.1, Double.longBitsToDouble( 11 ), Double.longBitsToDouble( 12 ),
				0.1, Double.longBitsToDouble( 13 ), Double.longBitsToDouble( 12 ),
				0.4, Double.longBitsToDouble( 13 ), Double.longBitsToDouble( 10 ),
				0.8, Double.longBitsToDouble( 11 ), Double.longBitsToDouble( 13 )
		};

		final long[] counts = new long[] {
				10, 15,
				11, 20,
				12, 1,
				13, 2,
		};

		final EdgesAndCounts eac = new EdgesAndCounts( affinities, counts, new long[ 0 ] );
		final Tuple2< Long, EdgesAndCounts > input = new Tuple2<>( 1l, eac );

		final MergeBloc< Long > mergeBloc = new MergeBloc<>( ( a, c1, c2 ) -> Math.min( c1, c2 ) / ( a * a ), 100 );
		mergeBloc.call( input );
	}

}
