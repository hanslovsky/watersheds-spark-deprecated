package de.hanslovsky.watersheds.graph;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.graph.MergeBloc.EdgeMerger;
import de.hanslovsky.watersheds.graph.MergeBloc.EdgesAndCounts;
import de.hanslovsky.watersheds.graph.MergeBloc.Function;
import gnu.trove.list.array.TLongArrayList;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RegionMerging
{

	private final MergeBloc.Function f;

	private final MergeBloc.EdgeMerger merger;

	public RegionMerging( final Function f, final EdgeMerger merger )
	{
		super();
		this.f = f;
		this.merger = merger;
	}

	public void run( final JavaSparkContext sc, final JavaPairRDD< Long, EdgesAndCounts > rddIn, final double threshold )
	{
		final List< Long > indices = rddIn.keys().collect();
		final int[] parents = new int[ indices.size() ];
		for ( final Long m : indices )
			parents[ m.intValue() ] = m.intValue();
		final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], parents.length );

		JavaPairRDD< Long, EdgesAndCounts > rdd = rddIn;

		for ( boolean hasChanged = true; hasChanged; )
		{

			final JavaPairRDD< Tuple2< Long, Long >, EdgesAndCounts > mergedEdges =
					rdd.mapToPair( new MergeBloc.MergeBlocPairFunction( f, merger, threshold ) ).cache();

			final List< Tuple2< Long, Long > > mergers = mergedEdges.keys().collect();
			boolean pointsOutside = false;
			for ( final Tuple2< Long, Long > m : mergers )
			{
				if ( m._1() != m._2() )
					pointsOutside = true;
				final int r1 = dj.findRoot( m._1().intValue() );
				final int r2 = dj.findRoot( m._2().intValue() );
				dj.join( r1, r2 );
			}
			hasChanged = !pointsOutside;

			final Broadcast< int[] > parentsBC = sc.broadcast( parents );

			System.out.println( Arrays.toString( parents ) );

			rdd = mergedEdges.mapToPair( new SetKeyToRoot( parentsBC ) ).reduceByKey( new MergeByKey( parentsBC ) );

			// assume zero based, contiguuous indices?

		}


	}

	public static class SetKeyToRoot implements PairFunction< Tuple2< Tuple2< Long, Long >, EdgesAndCounts >, Long, EdgesAndCounts >
	{

		private static final long serialVersionUID = -6206815670426308405L;

		private final Broadcast< int[] > parents;

		public SetKeyToRoot( final Broadcast< int[] > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, EdgesAndCounts > call( final Tuple2< Tuple2< Long, Long >, EdgesAndCounts > t ) throws Exception
		{
			final long k = parents.getValue()[ t._1()._1().intValue() ];
			return new Tuple2<>( k, t._2() );
		}

	}

	public static class MergeByKey implements Function2< EdgesAndCounts, EdgesAndCounts, EdgesAndCounts >
	{

		private final Broadcast< int[] > parents;

		public MergeByKey( final Broadcast< int[] > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public EdgesAndCounts call( final EdgesAndCounts eac1, final EdgesAndCounts eac2 ) throws Exception
		{
			final long[] assignments = new long[ eac1.assignments.length + eac2.assignments.length ];
			System.arraycopy( eac1.assignments, 0, assignments, 0, eac1.assignments.length );
			System.arraycopy( eac2.assignments, 0, assignments, eac1.assignments.length, eac2.assignments.length );

			final double[] edges = new double[ eac1.edges.length + eac2.edges.length ];
			System.arraycopy( eac1.edges, 0, edges, 0, eac1.edges.length );
			System.arraycopy( eac2.edges, 0, edges, eac1.edges.length, eac2.edges.length );

			final long[] counts = new long[ eac1.counts.length + eac2.counts.length ];
			System.arraycopy( eac1.counts, 0, counts, 0, eac1.counts.length );
			System.arraycopy( eac2.counts, 0, counts, eac1.counts.length, eac2.counts.length );

			final TLongArrayList outside = new TLongArrayList();
			for ( int i = 0; i < eac1.assignments.length; i += 3 )
			{

			}

			return null;
		}

	}


}
