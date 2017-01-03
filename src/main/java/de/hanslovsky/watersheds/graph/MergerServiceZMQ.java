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

	public MergerServiceZMQ( final String addr )
	{
		super();
		this.addr = addr;
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 1841897060412421701L;

	@Override
	public void addMerge( final long n1, final long n2, final long n, final double w )
	{
		final Context ctx = ZMQ.context( 1 );
		final Socket socket = ctx.socket( ZMQ.PUSH );
		socket.connect( addr );
		final byte[] bytes = new byte[ 32 ];
		final ByteBuffer bb = ByteBuffer.wrap( bytes );
		bb.putLong( n1 );
		bb.putLong( n2 );
		bb.putLong( n );
		bb.putDouble( w );
		socket.send( bytes, 0 );
		socket.close();
		ctx.close();
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
				action.add( bb.getLong(), bb.getLong(), bb.getLong(), bb.getDouble() );
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
