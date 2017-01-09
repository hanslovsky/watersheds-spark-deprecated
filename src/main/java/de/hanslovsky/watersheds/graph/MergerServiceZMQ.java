/**
 *
 */
package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;

/**
 * @author hanslovskyp
 *
 */
public class MergerServiceZMQ implements MergerService, Serializable
{

	private final String addr;

	private final TLongArrayList data;

	public MergerServiceZMQ( final String addr )
	{
		super();
		this.addr = addr;
		this.data = new TLongArrayList();
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 1841897060412421701L;

	@Override
	public void addMerge( final long n1, final long n2, final long n, final double w )
	{
		data.add( n1 );
		data.add( n2 );
		data.add( n );
		data.add( Double.doubleToLongBits( w ) );
//		final Context ctx = ZMQ.context( 1 );
//		final Socket socket = ctx.socket( ZMQ.PUSH );
//		socket.connect( addr );
//		final byte[] bytes = new byte[ 32 ];// data.size() * Long.BYTES ];
//		final ByteBuffer bb = ByteBuffer.wrap( bytes );
//		bb.putLong( n1 );
//		bb.putLong( n2 );
//		bb.putLong( n );
//		bb.putDouble( w );
//		socket.send( bytes, 0 );
//		socket.close();
//		ctx.close();
//		data.clear();
	}

	@Override
	public void finalize()
	{
		final Context ctx = ZMQ.context( 1 );
		final Socket socket = ctx.socket( ZMQ.PUSH );
		socket.connect( addr );
		final byte[] bytes = new byte[ data.size() * Long.BYTES ];
		final ByteBuffer bb = ByteBuffer.wrap( bytes );
		for ( int i = 0; i < data.size(); i += 4 )
		{
			bb.putLong( data.get( i ) );
			bb.putLong( data.get( i + 1 ) );
			bb.putLong( data.get( i + 2 ) );
			bb.putDouble( Double.longBitsToDouble( data.get( i + 3 ) ) );
		}
//		bb.putLong( n1 );
//		bb.putLong( n2 );
//		bb.putLong( n );
//		bb.putDouble( w );
		socket.send( bytes, 0 );
		socket.close();
		ctx.close();
		data.clear();
	}

	public static interface MergeAction
	{

		public void add( long n1, long n2, long n, double w );

	}

	public static class MergeActionAddToList implements MergeAction
	{

		private final TLongArrayList list;

		public MergeActionAddToList( final TLongArrayList list )
		{
			super();
			this.list = list;
		}

		@Override
		public synchronized void add( final long n1, final long n2, final long n, final double w )
		{
//			System.out.println( "Adding " + n1 + " " + n2 + " " + n + " " + w );
			list.add( n1 );
			list.add( n2 );
			list.add( n );
			list.add( Edge.dtl( w ) );
		}

	}

	public static class MergeActionParentMap implements MergeAction
	{

		private final TLongLongHashMap parents;

		public MergeActionParentMap( final TLongLongHashMap parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public synchronized void add( final long n1, final long n2, final long n, final double w )
		{
			parents.put( n1, n );
			parents.put( n2, n );
			parents.put( n, n );

		}

	}

	public static Thread createServerThread( final Socket socket, final MergeAction action )
	{
		final Thread t = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] msg = socket.recv();
				if ( msg.length == 0 )
					continue;
				final ByteBuffer bb = ByteBuffer.wrap( msg );
				while ( bb.position() < bb.limit() )
				{
					final long n1 = bb.getLong();
					final long n2 = bb.getLong();
					final long n = bb.getLong();
					final double w = Double.longBitsToDouble( bb.getLong() );
//					System.out.println( "ADDDDDING " + n1 + " " + n2 + " " + n + " " + w );
					action.add( n1, n2, n, w );
				}
			}
		} );
		return t;
	}

	public static Socket createServerSocket( final Context ctx, final String addr )
	{
		final Socket socket = ctx.socket( ZMQ.PULL );
		socket.bind( addr );
		return socket;
	}

}
