package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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

import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.MergeBloc.Out;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RegionMerging
{

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
		final List< Long > indices = rddIn.keys().collect();
		final int[] parents = new int[ indices.size() ];
		for ( final Long m : indices )
			parents[ m.intValue() ] = m.intValue();
		final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], parents.length );

		JavaPairRDD< Long, MergeBloc.In > rdd = rddIn;

//		final boolean[] isDone = new boolean[ parents.length ];

		for ( boolean hasChanged = true; hasChanged; )
		{

			final JavaPairRDD< Tuple2< Long, TLongHashSet >, Out > mergedEdges =
					rdd
					.mapToPair( new MergeBloc.MergeBlocPairFunction2( f, merger, threshold, idService, mergerService ) )
//					.mapToPair( new RemoveOutOfScopeAssignmentsAndCounts<>() )
					.cache();

			final List< Tuple2< Long, TLongHashSet > > mergers = mergedEdges.keys().collect();
			boolean pointsOutside = false;
			for ( final Tuple2< Long, TLongHashSet > m : mergers )
			{
//				if ( m._2().size() == 0 )
//					continue;
				final long index1 = m._1();
				for ( final TLongIterator it = m._2().iterator(); it.hasNext(); )
				{
					final long index2 = it.next();
					if ( index2 != index1 )
						pointsOutside = true;
					final int r1 = dj.findRoot( ( int ) index1 );
					final int r2 = dj.findRoot( ( int ) index2 );
					dj.join( r1, r2 );
				}
			}

			hasChanged = pointsOutside;

			final Broadcast< int[] > parentsBC = sc.broadcast( parents );

//			final JavaPairRDD< Long, Tuple2< Long, Out > > rdd1 = mergedEdges.mapToPair( new SetKeyToRoot<>( parentsBC ) ).cache();
//			rdd1.count();
//			final JavaPairRDD< Long, Tuple2< Long, Out > > rdd2 = rdd1; // rdd1.mapToPair(
//			// new
//			// AddKeyToValue<>()
//			// ).cache();
//			rdd2.count();
//			final JavaPairRDD< Long, Tuple2< Long, Out > > rdd3 = rdd2.reduceByKey( new MergeByKey( parentsBC ) ).cache();
//			rdd3.count();
//			final JavaPairRDD< Long, Out > rdd4 = rdd3.mapToPair( new RemoveFromValue<>() ).cache();
//			rdd4.count();
//			final JavaPairRDD< Long, In > rdd5 = rdd4.mapToPair( new UpdateEdgesAndCounts( f ) ).cache();
//			rdd5.count();
//			rdd = rdd5.cache();
//			rdd.count();
//			System.out.println( "Merged blocks, now " + rdd.count() + " blocks" );

			rdd = mergedEdges
					.mapToPair( new SetKeyToRoot<>( parentsBC ) )
//					.mapToPair( new AddKeyToValue<>() )
					.reduceByKey( new MergeByKey( parentsBC ) )
					.mapToPair( new RemoveFromValue<>() )
					.mapToPair( new UpdateEdgesAndCounts( f ) )
					.cache();
			rdd.count();
			System.out.println( "Merged blocks, now " + rdd.count() + " blocks" );
//			System.exit( 1 );

			// assume zero based, contiguuous indices?

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
//			final long[] counts = new long[] {
//					14, 15,
//					16, 30
//			};

			final Edge e = new Edge( affinities );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			}

			final TLongLongHashMap assignments = new TLongLongHashMap(
					new long[] { 14, 16 },
					new long[] { 14, 16 } );
//			final long[] assignments = new long[] {
//					14, 14,
//					16, 16
//			};

			final TLongObjectHashMap<TLongHashSet> outside = new TLongObjectHashMap< TLongHashSet >();
			outside.put( 14, new TLongHashSet( new long[] { 0 } ) );
//					new long[] { 11 }, new long[] { 0 } );
//			final long[] outside = new long[] {
//					11, 20, 0
//			};
			al.add( new Tuple2<>( 1l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.2, Edge.ltd( 10 ), Edge.ltd( 15 ), Edge.ltd( 1 )
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 15, 10 },
					new long[] { 4000, 15 } );
//			final long[] counts = new long[] {
//					15, 4000
//			};

			final Edge e = new Edge( affinities );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				e.weight( func.weight( e.affinity(), counts.get( e.from() ), counts.get( e.to() ) ) );
			}

			final TLongLongHashMap assignments = new TLongLongHashMap( new long[] { 15 }, new long[] { 15 } );
//			final long[] assignments = new long[] {
//					15, 15
//			};

			final TLongObjectHashMap< TLongHashSet > outside = new TLongObjectHashMap< TLongHashSet >();
			outside.put( 15, new TLongHashSet( new long[] { 0 } ) );
//					new long[] { 10 }, new long[] { 0 } );
//			final long[] outside = new long[] {
//					10, 15, 0
//			};
			al.add( new Tuple2<>( 2l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		final JavaPairRDD< Long, MergeBloc.In > rdd = sc.parallelizePairs( al );
		final EdgeMerger em = MergeBloc.DEFAULT_EDGE_MERGER;

		final AtomicLong currentId = new AtomicLong( 17 );
		final Context ctx = ZMQ.context( 1 );
		final Socket idSocket = ctx.socket( ZMQ.REP );
		final String idServiceAddr = "ipc://IdService";
		idSocket.bind( idServiceAddr );
		final Thread t = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] msg = idSocket.recv();
				final ByteBuffer bb = ByteBuffer.wrap( msg );
				if ( msg.length == 0 )
					continue;
				final long id = currentId.getAndAdd( bb.getLong() );
				bb.rewind();
				bb.putLong( id );
				idSocket.send( msg, 0 );
			}
		} );
		t.start();

		final IdService idService = ( IdService & Serializable ) numIds -> {
			final Context ctx2 = ZMQ.context( 1 );
			final Socket socket = ctx2.socket( ZMQ.REQ );
			socket.connect( idServiceAddr );
			final byte[] data = new byte[ Long.BYTES ];
			final ByteBuffer bb = ByteBuffer.wrap( data );
			bb.putLong( numIds );
			socket.send( data, 0 );
			final byte[] msg = socket.recv();
			final long id = ByteBuffer.wrap( msg ).getLong();
			socket.close();
			ctx2.close();
			return id;
		};

		final TLongArrayList merges = new TLongArrayList();
		final String mergesAddr = "ipc://merges";
		final Socket mergesSocket = ctx.socket( ZMQ.PULL );
		mergesSocket.bind( mergesAddr );
		final Thread mergesThread = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] msg = mergesSocket.recv();
				System.out.println( "REceived message " + Arrays.toString( msg ) );
				System.out.println( merges );
				if ( msg.length == 0 )
					continue;
				final ByteBuffer bb = ByteBuffer.wrap( msg );
				merges.add( bb.getLong() );
				merges.add( bb.getLong() );
				merges.add( bb.getLong() );
				merges.add( Edge.dtl( bb.getDouble() ) );
			}
		} );
		mergesThread.start();
		final MergerService mergerService = ( MergerService & Serializable ) ( n1, n2, n, w ) -> {
			System.out.println( "Merged " + n1 + " and " + n2 + " into " + n );
			final Context localCtx = ZMQ.context( 1 );
			final Socket localSoc = localCtx.socket( ZMQ.PUSH );
			localSoc.connect( mergesAddr );
			final byte[] bytes = new byte[ 32 ];
			final ByteBuffer bb = ByteBuffer.wrap( bytes );
			bb.putLong( n1 );
			bb.putLong( n2 );
			bb.putLong( n );
			bb.putDouble( w );
			localSoc.send( bytes, 0 );
			localSoc.close();
			localCtx.close();
		};

		final RegionMerging rm = new RegionMerging(
				func, // ( Function & Serializable ) ( a,
				// c1, c2 ) -> Math.min( c1, c2 ) /
				// ( a * a ),
				em,
				idService,
				mergerService );

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

		System.out.println( result );
		for ( final Tuple2< Long, In > rr : result )
			System.out.println( rr._1() + " " + rr._2().counts );
		System.exit( 234 );

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

	public static class SetKeyToRoot< V > implements PairFunction< Tuple2< Tuple2< Long, TLongHashSet >, V >, Long, Tuple2< Long, V > >
	{

		private static final long serialVersionUID = -6206815670426308405L;

		private final Broadcast< int[] > parents;

		public SetKeyToRoot( final Broadcast< int[] > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, Tuple2< Long, V > > call( final Tuple2< Tuple2< Long, TLongHashSet >, V > t ) throws Exception
		{
			final long k = parents.getValue()[ t._1()._1().intValue() ];
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
		private final Broadcast< int[] > parents;

		public MergeByKey( final Broadcast< int[] > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, MergeBloc.Out > call(
				final Tuple2< Long, MergeBloc.Out > eacWithKey1,
				final Tuple2< Long, MergeBloc.Out > eacWithKey2 ) throws Exception
		{
			final int[] parents = this.parents.getValue();
			final int r = parents[ eacWithKey1._1().intValue() ];
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

//	public static class RemoveOutOfScopeAssignmentsAndCounts< K > implements PairFunction< Tuple2< K, MergeBloc.Out >, K, MergeBloc.Out >
//	{
//
//		/**
//		 *
//		 */
//		private static final long serialVersionUID = -7394523116174827558L;
//
//		@Override
//		public Tuple2< K, Out > call( final Tuple2< K, Out > t ) throws Exception
//		{
//			final TLongLongHashMap outside = t._2().outside;
//			final TLongLongHashMap assignments = t._2().assignments;
//			final TLongLongHashMap resultAssignments = new TLongLongHashMap();
//			final TLongLongHashMap counts = t._2().counts;
//			final TLongLongHashMap resultCounts = new TLongLongHashMap();
//			for ( final TLongLongIterator it = assignments.iterator(); it.hasNext(); )
//			{
//				it.advance();
//				final long k = it.key();
//				if ( !outside.contains( k ) )
//				{
//					resultAssignments.put( k, it.value() );
//					final long c = counts.get( k );
//					if ( c > 0 )
//						resultCounts.put( k, c );
//				}
//			}
//			return new Tuple2<>(
//					t._1(),
//					new MergeBloc.Out( t._2().edges, resultCounts, outside, resultAssignments, t._2().fragmentPointedToOutside ) );
//		}
//
//	}

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

	public static void addOutside( final TLongObjectHashMap< TLongHashSet > source, final TLongObjectHashMap< TLongHashSet > target, final int root, final int[] parents )
	{
		for ( final TLongObjectIterator< TLongHashSet > it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final TLongHashSet hs = new TLongHashSet();
			final TLongHashSet v = it.value();
			for ( final TLongIterator vIt = v.iterator(); vIt.hasNext(); )
			{
				final long vV = vIt.next();
				if ( parents[ ( int ) vV ] == root )
					continue;
				else
					hs.add( vV );
			}
			if ( hs.size() > 0 )
				target.put( it.key(), hs );
		}
	}
}
