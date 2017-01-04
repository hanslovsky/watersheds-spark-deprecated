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
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.MergeBloc.Out;
import de.hanslovsky.watersheds.graph.RegionMerging.MergeByKey.MergeData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.LongType;
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

	public JavaPairRDD< Long, MergeBloc.In > run( final JavaSparkContext sc, final JavaPairRDD< Long, MergeBloc.In > rddIn, final double threshold, final RandomAccessibleInterval< LongType > labelsTarget )
	{
		return run( sc, rddIn, threshold, ( parents ) -> {}, labelsTarget );
	}

	public JavaPairRDD< Long, MergeBloc.In > run( final JavaSparkContext sc, final JavaPairRDD< Long, MergeBloc.In > rddIn, final double threshold, final Visitor visitor, final RandomAccessibleInterval< LongType > labelsTarget )
	{
		final List< Long > indices = rddIn.keys().collect();
		final TLongLongHashMap parents = new TLongLongHashMap();
		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, new TLongLongHashMap(), 0 );
		for ( final Long m : indices )
			dj.findRoot( m.longValue() );

		JavaPairRDD< Long, MergeBloc.In > rdd = rddIn;


		int iteration = 0;
		final int targetIteration = 0;
		double actualThreshold = threshold;
		for ( boolean hasChanged = true; hasChanged; ++iteration, actualThreshold *= 2 )
		{

			final JavaPairRDD< Tuple2< Long, Long >, Out > mergedEdges =
					rdd
					.mapToPair( new MergeBloc.MergeBlocPairFunction2( f, merger, actualThreshold, idService, mergerService ) )
					.cache();

			hasChanged = mergedEdges.filter( t -> t._2().hasChanged ).count() > 0;

			final List< Tuple2< Long, Long > > mergers = mergedEdges.keys().collect();
			for ( final Tuple2< Long, Long > m : mergers )
			{
				final long index1 = m._1();
				final long index2 = m._2();
				final long r1 = dj.findRoot( index1 );
				final long r2 = dj.findRoot( index2 );
				dj.join( r1, r2 );
			}

			for ( final TLongIterator it = parents.keySet().iterator(); it.hasNext(); )
				dj.findRoot( it.next() );
			final Broadcast< TLongLongHashMap > parentsBC = sc.broadcast( parents );

			final JavaPairRDD< Long, MergeData > merged = mergedEdges
					.mapToPair( new SetKeyToRoot<>( parentsBC ) )
					.mapToPair( t -> new Tuple2<>( t._1(), new Tuple2<>( t._2()._1(), new MergeByKey.MergeData( t._2()._2() ) ) ) )
					.reduceByKey( new MergeByKey( parentsBC ) )
					.mapToPair( new RemoveFromValue<>() )
					.cache();
			rdd = merged
					.mapToPair( new UpdateEdgesAndCounts( f, merger, parentsBC ) )
					.cache();
			rdd.count();
			System.out.println( rdd.keys().collect() );
			System.out.println( "Merged blocks, now " + rdd.count() + " blocks" );

			if ( rdd.count() == 0 )
				hasChanged = false;

			visitor.visit( parents );


			System.out.println( "CNT: " + rdd.count() + " " + hasChanged );

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

	public static class SetKeyToRoot< V > implements PairFunction< Tuple2< Tuple2< Long, Long >, V >, Long, Tuple2< Long, V > >
	{

		private static final long serialVersionUID = -6206815670426308405L;

		private final Broadcast< TLongLongHashMap > parents;

		public SetKeyToRoot( final Broadcast< TLongLongHashMap > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, Tuple2< Long, V > > call( final Tuple2< Tuple2< Long, Long >, V > t ) throws Exception
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

	public static class MergeByKey implements Function2< Tuple2< Long, MergeByKey.MergeData >, Tuple2< Long, MergeByKey.MergeData >, Tuple2< Long, MergeByKey.MergeData > >
	{

		public static class MergeData implements Serializable
		{
			public final TDoubleArrayList edges;

			public final TLongLongHashMap counts;

			public final TLongObjectHashMap< TLongHashSet > borderNodes;

			public final TLongLongHashMap assignments;

			public final TLongHashSet mergedBorderNodes;

			public final TLongLongHashMap outsideNodes;

			public MergeData( final MergeBloc.Out out )
			{
				this( out.g.edges(), out.counts, out.borderNodes, out.assignments, out.mergedBorderNodes, out.outsideNodes );
			}

			public MergeData( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongObjectHashMap< TLongHashSet > borderNodes, final TLongLongHashMap assignments, final TLongHashSet mergedBorderNodes, final TLongLongHashMap outsideNodes )
			{
				super();
				this.edges = edges;
				this.counts = counts;
				this.borderNodes = borderNodes;
				this.assignments = assignments;
				this.mergedBorderNodes = mergedBorderNodes;
				this.outsideNodes = outsideNodes;
			}
		}

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
		public Tuple2< Long, MergeByKey.MergeData > call(
				final Tuple2< Long, MergeByKey.MergeData > mdWithKey1,
				final Tuple2< Long, MergeByKey.MergeData > mdWithKey2 ) throws Exception
		{
			final TLongLongHashMap parents = this.parents.getValue();
			final long r = parents.get( mdWithKey1._1().longValue() );
			final MergeData md1 = mdWithKey1._2();
			final MergeData md2 = mdWithKey2._2();

			final TLongLongHashMap assignments = new TLongLongHashMap();
			addAssignments( md1.assignments, assignments, md1.outsideNodes.keySet() );
			addAssignments( md2.assignments, assignments, md2.outsideNodes.keySet() );

			final TDoubleArrayList edges = new TDoubleArrayList();
			edges.addAll( md1.edges );
			edges.addAll( md2.edges );

			final TLongLongHashMap counts = new TLongLongHashMap();
			addCounts( md1.counts, counts );
			addCounts( md2.counts, counts );

			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
			borderNodes.putAll( md1.borderNodes );
			borderNodes.putAll( md2.borderNodes );

			final TLongHashSet mergedBorderNodes = new TLongHashSet();
			mergedBorderNodes.addAll( md1.mergedBorderNodes );
			mergedBorderNodes.addAll( md2.mergedBorderNodes );

			final TLongLongHashMap outsideNodes = new TLongLongHashMap();
			addOutside( md1.outsideNodes, outsideNodes, r, parents );
			addOutside( md2.outsideNodes, outsideNodes, r, parents );

			return new Tuple2<>(
					mdWithKey1._1(),
					new MergeByKey.MergeData( edges, counts, borderNodes, assignments, md1.mergedBorderNodes, outsideNodes ) );
		}

	}

	public static class UpdateEdgesAndCounts implements PairFunction< Tuple2< Long, MergeByKey.MergeData >, Long, MergeBloc.In >
	{

		private final Function f;

		private final EdgeMerger edgeMerger;

		private final Broadcast< TLongLongHashMap > parents;

		public UpdateEdgesAndCounts( final Function f, final EdgeMerger edgeMerger, final Broadcast< TLongLongHashMap > parents )
		{
			super();
			this.f = f;
			this.edgeMerger = edgeMerger;
			this.parents = parents;
		}

		/**
		 *
		 */
		private static final long serialVersionUID = 2239780753041141629L;

		@Override
		public Tuple2< Long, MergeBloc.In > call( final Tuple2< Long, MergeByKey.MergeData > t ) throws Exception
		{
			final TDoubleArrayList edges = t._2().edges;
			final TLongLongHashMap counts = t._2().counts;
			final TLongLongHashMap assignments = t._2().assignments;
			final TLongHashSet candidates = t._2().mergedBorderNodes;
			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
			addBorderNodes( t._2().borderNodes, borderNodes, t._1(), parents.getValue(), assignments );
			final UndirectedGraph g = new UndirectedGraph( edges, edgeMerger );

			final TLongObjectHashMap< TLongObjectHashMap< TIntHashSet > > updateEdges =
					collectUpdateEdges( g, candidates, assignments, borderNodes );

			updateEdges( g, updateEdges, edgeMerger );

			final Edge source = new Edge( g.edges() );
			final Edge target = new Edge( new TDoubleArrayList() );
			for ( int i = 0; i < source.size(); ++i )
			{
				source.setIndex( i );
				final double w = source.weight();
				if ( w >= 0.0 )
					target.add( w, source.affinity(), source.from(), source.to(), source.multiplicity() );
			}



			new UndirectedGraph( target.data(), edgeMerger );
			final MergeBloc.In result = new MergeBloc.In( g, counts, borderNodes, t._2().outsideNodes );

			return new Tuple2<>( t._1(), result );
		}

	}

	public static void updateEdges(
			final UndirectedGraph g,
			final TLongObjectHashMap< TLongObjectHashMap< TIntHashSet > > updateEdges,
			final EdgeMerger edgeMerger )
	{
		final Edge e1 = new Edge( g.edges() );
		final Edge e2 = new Edge( g.edges() );
		for ( final TLongObjectIterator< TLongObjectHashMap< TIntHashSet > > outerIt = updateEdges.iterator(); outerIt.hasNext(); )
		{
			outerIt.advance();
			final long r1 = outerIt.key();
			for ( final TLongObjectIterator< TIntHashSet > innerIt = outerIt.value().iterator(); innerIt.hasNext(); ) {
				innerIt.advance();
				final long r2 = outerIt.key();
				{
					final TIntIterator edgesIt = innerIt.value().iterator();
					e2.setIndex( e2.add( e1.weight(), e1.affinity(), r1, r2, e1.multiplicity() ) );
					for ( ; edgesIt.hasNext(); )
					{
						final int i = edgesIt.next();
						e1.setIndex( i );
						edgeMerger.merge( e1, e2 );
						e1.weight( -1.0 );
					}
				}
			}
		}
	}

	public static TLongObjectHashMap< TLongObjectHashMap< TIntHashSet > > collectUpdateEdges(
			final UndirectedGraph g,
			final TLongHashSet mergedBorderNodes,
			final TLongLongHashMap assignments,
			final TLongObjectHashMap< TLongHashSet > borderNodes )
	{
		final TLongObjectHashMap< TLongObjectHashMap< TIntHashSet > > updateEdges = new TLongObjectHashMap<>();
		final Edge e = new Edge( g.edges() );
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = g.nodeEdgeMap();

		for ( final TLongIterator it = mergedBorderNodes.iterator(); it.hasNext(); )
		{
			final long id1 = it.next();
			final long r1 = assignments.get( id1 );
			// need r1 or id1 here?
			final TLongIntHashMap edges = nodeEdgeMap.get( r1 );

			System.out.println( id1 + " " + r1 + " " + assignments.contains( id1 ) + " " + edges );
			if ( edges == null )
				System.out.println( "WAS IST DA LOS?" );
			for ( final TLongIntIterator eIt = edges.iterator(); eIt.hasNext(); )
			{
				eIt.advance();
				final long id2 = eIt.key();
				if ( borderNodes.contains( id2 ) )
				{
					final long r2 = assignments.get( id2 );
					final long from = Math.min( r1, r2 );
					final long to = Math.max( r1, r2 );
					if ( !updateEdges.contains( from ) )
						updateEdges.put( from, new TLongObjectHashMap<>() );
					final TLongObjectHashMap< TIntHashSet > m1 = updateEdges.get( from );
					if ( !m1.contains( to  ) )
						m1.put( to, new TIntHashSet() );
					m1.get( to ).add( eIt.value() );

				}
			}
		}

		return updateEdges;
	}

	public static void addAssignments( final TLongLongHashMap source, final TLongLongHashMap target, final TLongSet outsideNodes )
	{
		for ( final TLongLongIterator it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final long k = it.key();
			final long v = it.value();
			// TODO wrong logic here! cannot use outsideNodes
//			if ( !outsideNodes.contains( k ) )
			if ( target.contains( k ) )
			{
				if ( k != v )
					target.put( k, v );
			}
			else
				target.put( k, v );
		}
	}

	public static void addCounts( final TLongLongHashMap source, final TLongLongHashMap target )
	{
		for ( final TLongLongIterator it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final long k = it.key();
			final long v = it.value();
			if ( v > 0 )
				target.put( k, v );
		}
	}

	public static void addOutside(
			final TLongLongHashMap source,
			final TLongLongHashMap target,
			final long root,
			final TLongLongHashMap parents )
	{
		for ( final TLongLongIterator it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final long p = parents.get( it.value() );
			if ( p != root )
				target.put( it.key(), p );
		}
	}

	public static void addBorderNodes(
			final TLongObjectHashMap< TLongHashSet > source,
			final TLongObjectHashMap< TLongHashSet > target,
			final long root,
			final TLongLongHashMap parents,
			final TLongLongHashMap assignments ) {
		for ( final TLongObjectIterator< TLongHashSet > it = source.iterator(); it.hasNext(); ) {
			it.advance();
			final long k = it.key();
			if ( k == assignments.get( k ) )
			{
				final TLongHashSet neighborBlocks = new TLongHashSet();
				for ( final TLongIterator bIt = it.value().iterator(); bIt.hasNext(); )
				{
					final long p = parents.get( bIt.next() );
					if ( p != root )
						neighborBlocks.add( p );
				}
				if ( neighborBlocks.size() > 0 )
					if ( source.contains( k ) )
						source.get( k ).addAll( neighborBlocks );
					else
						source.put( k, neighborBlocks );
			}
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

			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
			borderNodes.put( 10, new TLongHashSet( new long[] { 2 } ) );
			borderNodes.put( 11, new TLongHashSet( new long[] { 1 } ) );

			final TLongLongHashMap outsideNodes = new TLongLongHashMap(
					new long[] { 14, 15 },
					new long[] { 1, 2 } );

			al.add( new Tuple2<>( 0l, new MergeBloc.In( new UndirectedGraph( affinities, MergeBloc.DEFAULT_EDGE_MERGER ), counts, borderNodes, outsideNodes ) ) );
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

			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
			borderNodes.put( 14, new TLongHashSet( new long[] { 0 } ) );

			final TLongLongHashMap outside = new TLongLongHashMap(
					new long[] { 11 },
					new long[] { 0 } );

			al.add( new Tuple2<>( 1l, new MergeBloc.In( new UndirectedGraph( affinities, MergeBloc.DEFAULT_EDGE_MERGER ), counts, borderNodes, outside ) ) );
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

			final TLongObjectHashMap< TLongHashSet > borderNodes = new TLongObjectHashMap<>();
			borderNodes.put( 15, new TLongHashSet( new long[] { 0 } ) );

			final TLongLongHashMap outside = new TLongLongHashMap(
					new long[] { 10 },
					new long[] { 0 } );

			al.add( new Tuple2<>( 2l, new MergeBloc.In( new UndirectedGraph( affinities, MergeBloc.DEFAULT_EDGE_MERGER ), counts, borderNodes, outside ) ) );
		}

		for ( final Tuple2< Long, In > a : al )
		{
			System.out.println( "Edges for " + a._1() );
			final Edge e = new Edge( a._2().g.edges() );
			for ( int i = 0; i < e.size(); e.setIndex( ++i ) )
				System.out.println( e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
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

		final List< Tuple2< Long, MergeBloc.In > > result = rm.run( sc, rdd, 99, null ).collect();
		for ( final Tuple2< Long, In > r : result )
		{
			System.out.println( r._1() );
			final Edge e = new Edge( r._2().g.edges() );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				System.out.println( e.weight() + " " + e.affinity() + " " + e.from() + " " + e.to() + " " + e.multiplicity() );
			}
		}

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





	}

}
