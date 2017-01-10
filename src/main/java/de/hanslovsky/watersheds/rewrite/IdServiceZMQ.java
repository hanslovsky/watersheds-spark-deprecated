package de.hanslovsky.watersheds.rewrite;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class IdServiceZMQ implements IdService, Serializable
{

	private static final long serialVersionUID = 650824418827837241L;
	private final String addr;

	public IdServiceZMQ( final String addr )
	{
		super();
		this.addr = addr;
	}

	@Override
	public long requestIds( final long numIds )
	{
		final Context ctx = ZMQ.context( 1 );
		final Socket socket = ctx.socket( ZMQ.REQ );
		socket.connect( addr );
		final byte[] data = new byte[ Long.BYTES ];
		final ByteBuffer bb = ByteBuffer.wrap( data );
		bb.putLong( numIds );
		socket.send( data, 0 );
		final byte[] msg = socket.recv();
		final long id = ByteBuffer.wrap( msg ).getLong();
		socket.close();
		ctx.close();
		return id;
	}

	public static Thread createServerThread( final Socket socket, final AtomicLong atomicId )
	{

		final Thread t = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] msg = socket.recv();
				final ByteBuffer bb = ByteBuffer.wrap( msg );
				if ( msg.length == 0 )
					continue;
				final long n = bb.getLong();
				final long id = atomicId.getAndAdd( n );
				bb.rewind();
				bb.putLong( id );
				socket.send( msg, 0 );
			}
		} );

		return t;

	}

	public static Socket createServerSocket( final Context ctx, final String addr )
	{
		final Socket socket = ctx.socket( ZMQ.REP );
		socket.bind( addr );
		return socket;
	}

}
