package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import de.hanslovsky.watersheds.graph.MergeBloc.Out;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import scala.Tuple2;

public class RegionMerging
{

	public static interface Visitor
	{

		void visit( TLongLongHashMap parents );

	}

	private final Function f;

	private final EdgeMerger merger;

	private final IdService idService;

	private final MergerService mergerService;

	public RegionMerging( final Function f, final EdgeMerger merger, final IdService idService, final MergerService mergerService )
	{
		super();
		this.f = f;
		this.merger = merger;
		this.idService = idService;
		this.mergerService = mergerService;
	}

	public JavaPairRDD< Long, MergeBloc.In > run( final JavaSparkContext sc, final JavaPairRDD< Long, MergeBloc.In > rddIn, final double threshold )
	{
		return run( sc, rddIn, threshold, ( parents ) -> {} );
	}

	public JavaPairRDD< Long, MergeBloc.In > run( final JavaSparkContext sc, final JavaPairRDD< Long, MergeBloc.In > rddIn, final double threshold, final Visitor visitor )
	{
		final List< Long > indices = rddIn.keys().collect();
		final TLongLongHashMap parents = new TLongLongHashMap();
		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, new TLongLongHashMap(), 0 );
		for ( final Long m : indices )
			dj.findRoot( m.longValue() );
//			parents[ m.intValue() ] = m.intValue();

//		final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], parents.length );

		JavaPairRDD< Long, MergeBloc.In > rdd = rddIn;

		for ( boolean hasChanged = true; hasChanged; )
		{

			final JavaPairRDD< Tuple2< Long, TLongHashSet >, Out > mergedEdges =
					rdd
					.mapToPair( new MergeBloc.MergeBlocPairFunction2( f, merger, threshold, idService, mergerService ) )
					.cache();

			final List< Tuple2< Long, TLongHashSet > > mergers = mergedEdges.keys().collect();
			boolean pointsOutside = false;
			for ( final Tuple2< Long, TLongHashSet > m : mergers )
			{
				final long index1 = m._1();
				for ( final TLongIterator it = m._2().iterator(); it.hasNext(); )
				{
					final long index2 = it.next();
					if ( index2 != index1 )
						pointsOutside = true;
					final long r1 = dj.findRoot( index1 );
					final long r2 = dj.findRoot( index2 );
					System.out.println( index1 + " " + index2 + " " + r1 + " " + r2 );
					dj.join( r1, r2 );
				}
			}

			hasChanged = pointsOutside;

			for ( final TLongIterator it = parents.keySet().iterator(); it.hasNext(); )
				dj.findRoot( it.next() );
			final Broadcast< TLongLongHashMap > parentsBC = sc.broadcast( parents );

			rdd = mergedEdges
					.mapToPair( new SetKeyToRoot<>( parentsBC ) )
					.reduceByKey( new MergeByKey( parentsBC ) )
					.mapToPair( new RemoveFromValue<>() )
					.mapToPair( new UpdateEdgesAndCounts( f ) )
					.cache();
			rdd.count();
			System.out.println( rdd.keys().collect() );
			System.out.println( "Merged blocks, now " + rdd.count() + " blocks" );
			if ( rdd.count() == 0 )
				hasChanged = false;

			visitor.visit( parents );

		}

		return rdd;
	}

	public static class CountOverSquaredSize implements Function, Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -2382379605891324313L;

		@Override
		public double weight( final double a, final long c1, final long c2 )
		{
			return Math.min( c1, c2 ) / ( a * a );
		}

	}

	public static class SetKeyToRoot< V > implements PairFunction< Tuple2< Tuple2< Long, TLongHashSet >, V >, Long, Tuple2< Long, V > >
	{

		private static final long serialVersionUID = -6206815670426308405L;

		private final Broadcast< TLongLongHashMap > parents;

		public SetKeyToRoot( final Broadcast< TLongLongHashMap > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, Tuple2< Long, V > > call( final Tuple2< Tuple2< Long, TLongHashSet >, V > t ) throws Exception
		{
			final long k = parents.getValue().get( t._1()._1().longValue() );
			return new Tuple2<>( k, new Tuple2<>( t._1()._1(), t._2() ) );
		}

	}

	public static class AddKeyToValue< K, V > implements PairFunction< Tuple2< K, V >, K, Tuple2< K, V > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -8129563585405199540L;

		@Override
		public Tuple2< K, Tuple2< K, V > > call( final Tuple2< K, V > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t );
		}

	}

	public static class RemoveFromValue< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, K, V2 >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -5495876386051788944L;

		@Override
		public Tuple2< K, V2 > call( final Tuple2< K, Tuple2< V1, V2 > > t ) throws Exception
		{
			// TODO Auto-generated method stub
			return new Tuple2<>( t._1(), t._2()._2() );
		}


	}

	public static class MergeByKey implements Function2< Tuple2< Long, MergeBloc.Out >, Tuple2< Long, MergeBloc.Out >, Tuple2< Long, MergeBloc.Out > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1428628232921208948L;

		private final Broadcast< TLongLongHashMap > parents;

		public MergeByKey( final Broadcast< TLongLongHashMap > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, MergeBloc.Out > call(
				final Tuple2< Long, MergeBloc.Out > eacWithKey1,
				final Tuple2< Long, MergeBloc.Out > eacWithKey2 ) throws Exception
		{
			final TLongLongHashMap parents = this.parents.getValue();
			final long r = parents.get( eacWithKey1._1().longValue() );
			final MergeBloc.Out eac1 = eacWithKey1._2();
			final MergeBloc.Out eac2 = eacWithKey2._2();
			final TLongLongHashMap assignments = new TLongLongHashMap();
			assignments.putAll( eac1.assignments );
//			assignments.putAll( eac2.assignments );

			for ( final TLongLongIterator it = eac2.assignments.iterator(); it.hasNext(); )
			{
				it.advance();
				final long k = it.key();
				final long v = it.value();
				if ( assignments.contains( k ) )
				{
					if ( k != v )
						assignments.put( k, v );
				}
				else
					assignments.put( k, v );
			}

			final TDoubleArrayList edges = eac1.edges;
			edges.addAll( eac2.edges );

			final TLongLongHashMap counts = new TLongLongHashMap();
			addCounts( eac1.counts, counts, assignments );// eac1.outside );
			addCounts( eac2.counts, counts, assignments );// eac2.outside );

			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap< TLongHashSet >();
			addOutside( eac1.borderNodes, borderNodes, r, parents );
			addOutside( eac2.borderNodes, borderNodes, r, parents );

			eac1.fragmentPointedToOutside.addAll( eac2.fragmentPointedToOutside );

			return new Tuple2<>( eacWithKey1._1(), new MergeBloc.Out( edges, counts, borderNodes, assignments, eac1.fragmentPointedToOutside ) );
		}

	}

	public static class UpdateEdgesAndCounts implements PairFunction< Tuple2< Long, MergeBloc.Out >, Long, MergeBloc.In >
	{

		private final Function f;

		public UpdateEdgesAndCounts( final Function f )
		{
			super();
			this.f = f;
		}

		/**
		 *
		 */
		private static final long serialVersionUID = 2239780753041141629L;

		@Override
		public Tuple2< Long, MergeBloc.In > call( final Tuple2< Long, MergeBloc.Out > t ) throws Exception
		{
			final TDoubleArrayList edges = t._2().edges;
			final TLongLongHashMap counts = t._2().counts;
			final TLongLongHashMap assignments = t._2().assignments;
			final TLongHashSet candidates = t._2().fragmentPointedToOutside;
			final Edge e = new Edge( edges );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long from = assignments.get( e.from() );
				final long to = assignments.get( e.to() );
				e.from( from );
				e.to( to );

				if ( candidates.contains( from ) || candidates.contains( to ) )
					e.weight( f.weight( e.affinity(), counts.get( from ), counts.get( to ) ) );
			}


			final MergeBloc.In result = new MergeBloc.In( edges, t._2().counts, t._2().borderNodes );

			return new Tuple2<>( t._1(), result );
		}

	}

	public static void addCounts( final TLongLongHashMap source, final TLongLongHashMap target, final TLongLongHashMap assignments )
	{
		for ( final TLongLongIterator it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final long k = it.key();
			final long v = it.value();
			target.put( k, target.contains( k ) ? Math.max( v, target.get( k ) ) : v );
		}
	}

	public static void addOutside( final TLongObjectHashMap< TLongHashSet > source, final TLongObjectHashMap< TLongHashSet > target, final long root, final TLongLongHashMap parents )
	{
		for ( final TLongObjectIterator< TLongHashSet > it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final TLongHashSet hs = new TLongHashSet();
			final TLongHashSet v = it.value();
			for ( final TLongIterator vIt = v.iterator(); vIt.hasNext(); )
			{
				final long vV = vIt.next();
				if ( parents.get( vV ) == root )
					continue;
				else
					hs.add( vV );
			}
			if ( hs.size() > 0 )
				target.put( it.key(), hs );
		}
	}

	public static void main( final String[] args ) throws InterruptedException
	{
		final SparkConf conf = new SparkConf()
				.setAppName( "merging" )
				.setMaster( "local[1]" )
				.set( "log4j.logger.org", "OFF" );
		final JavaSparkContext sc = new JavaSparkContext( conf );
		Logger.getRootLogger().setLevel( Level.ERROR );

		final CountOverSquaredSize func = new CountOverSquaredSize();

		final ArrayList< Tuple2< Long, MergeBloc.In > > al = new ArrayList<>();
		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.9, Edge.ltd( 10 ), Edge.ltd( 11 ), Edge.ltd( 1 ),
					Double.NaN, 0.1, Edge.ltd( 11 ), Edge.ltd( 12 ), Edge.ltd( 1 ),
					Double.NaN, 0.1, Edge.ltd( 13 ), Edge.ltd( 12 ), Edge.ltd( 1 ),
					Double.NaN, 0.4, Edge.ltd( 13 ), Edge.ltd( 10 ), Edge.ltd( 1 ),
					Double.NaN, 0.8, Edge.ltd( 11 ), Edge.ltd( 13 ), Edge.ltd( 1 ),
					Double.NaN, 0.9, Edge.ltd( 11 ), Edge.ltd( 14 ), Edge.ltd( 1 ),
					Double.NaN, 0.2, Edge.ltd( 10 ), Edge.ltd( 15 ), Edge.ltd( 1 )
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 10, 11, 12, 13, 14, 15 },
					new long[] { 15, 20, 1, 2, 16, 4000 } );

			final Edge e = new Edge( affinities );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			}

			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap< TLongHashSet >();
			borderNodes.put( 11, new TLongHashSet( new long[] { 1l } ) );
			borderNodes.put( 10, new TLongHashSet( new long[] { 2l } ) );

			al.add( new Tuple2<>( 0l, new MergeBloc.In( affinities, counts, borderNodes ) ) );
		}

		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.9, Edge.ltd( 11 ), Edge.ltd( 14 ), Edge.ltd( 1 ),
					Double.NaN, 1.0, Edge.ltd( 14 ), Edge.ltd( 16 ), Edge.ltd( 1 ),
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 14, 16, 11 },
					new long[] { 16, 30, 20 } );

			final Edge e = new Edge( affinities );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			}

			final TLongObjectHashMap< TLongHashSet > outside = new TLongObjectHashMap< TLongHashSet >();
			outside.put( 14, new TLongHashSet( new long[] { 0 } ) );
			al.add( new Tuple2<>( 1l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.2, Edge.ltd( 10 ), Edge.ltd( 15 ), Edge.ltd( 1 )
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 15, 10 },
					new long[] { 4000, 15 } );

			final Edge e = new Edge( affinities );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			}

			final TLongObjectHashMap< TLongHashSet > outside = new TLongObjectHashMap< TLongHashSet >();
			outside.put( 15, new TLongHashSet( new long[] { 0 } ) );
			al.add( new Tuple2<>( 2l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		final JavaPairRDD< Long, MergeBloc.In > rdd = sc.parallelizePairs( al );
		final EdgeMerger em = MergeBloc.DEFAULT_EDGE_MERGER;

		final AtomicLong currentId = new AtomicLong( 17 );
		final Context ctx = ZMQ.context( 1 );
		final Socket idSocket = ctx.socket( ZMQ.REP );
		final String idServiceAddr = "ipc://IdService";
		idSocket.bind( idServiceAddr );
		final Thread t = IdServiceZMQ.createServerThread( idSocket, currentId );

		t.start();

		final IdService idService = new IdServiceZMQ( idServiceAddr );

		final TLongArrayList merges = new TLongArrayList();
		final String mergesAddr = "ipc://merges";
		final Socket mergesSocket = ctx.socket( ZMQ.PULL );
		mergesSocket.bind( mergesAddr );
		final Thread mergesThread = MergerServiceZMQ.createServerThread( mergesSocket, new MergerServiceZMQ.MergeActionAddToList( merges ) );
		mergesThread.start();
		final MergerService mergerService = new MergerServiceZMQ( mergesAddr );

		final RegionMerging rm = new RegionMerging( func, em, idService, mergerService );

		final List< Tuple2< Long, MergeBloc.In > > result = rm.run( sc, rdd, 99 ).collect();

		sc.close();
		System.out.println( "SZ " + merges.size() );
		for ( int i = 0; i < merges.size(); i += 4 )
			System.out.println( merges.get( i ) + " " + merges.get( i + 1 ) + " " + merges.get( i + 2 ) + " " + Double.longBitsToDouble( merges.get( i + 3 ) ) );

		t.interrupt();
		final Socket sk = ctx.socket( ZMQ.REQ );
		sk.connect( idServiceAddr );
		sk.send( new byte[ 0 ], 0 );
		t.join();
//		sk.close();
//		ctx.close();

		final MultiGraph g = new MultiGraph( "graph" );

		final TLongHashSet roots = new TLongHashSet();
		final TLongLongHashMap parents = new TLongLongHashMap();
		for ( final Tuple2< Long, MergeBloc.In > r : result )
		{
//			System.out.println( r._1() + " " + r._2().assignments );
//			for ( final TLongLongIterator it = r._2().assignments.iterator(); it.hasNext(); )
//			{
//				it.advance();
//				parents.put( it.key(), it.value() );
//			}
		}
		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
			final Node n = g.addNode( "" + it.key() );
			n.addAttribute( "ui.label", "" + it.key() );
		}

		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
//			System.out.println( "Adding edge " + it.key() + " " + it.value() );
//			g.addEdge( id, from, to, directed )
			g.addEdge( it.key() + "-" + it.value(), "" + it.key(), "" + it.value(), true );
		}

		g.display( true );

	}

}
