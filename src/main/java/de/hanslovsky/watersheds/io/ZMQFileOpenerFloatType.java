package de.hanslovsky.watersheds.io;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;

import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;

public class ZMQFileOpenerFloatType implements FileOpener< FloatType >, Serializable
{

	private final String addr;

	public ZMQFileOpenerFloatType( final String addr )
	{
		super();
		this.addr = addr;
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 2756273175026546011L;

	@Override
	public void open( final long[] o, final long[] d, final Iterable< FloatType > target )
	{
		final ZContext context = new ZContext();
		final Socket requester = context.createSocket( ZMQ.REQ );
		requester.connect( addr );
		final long[] req = new long[ o.length + d.length ];
		System.arraycopy( o, 0, req, 0, o.length );
		System.arraycopy( d, 0, req, o.length, d.length );
		final Gson gson2 = new Gson();
		requester.send( gson2.toJson( req ).toString(), 0 );
		final byte[] response = requester.recv();
		final ByteBuffer bb = ByteBuffer.wrap( response );

		final long size = Intervals.numElements( d );
		assert size == response.length * Float.BYTES;

		System.out.println( size + " " + response.length * Float.BYTES );

		for ( final FloatType t : target )
			t.set( bb.getFloat() );

		context.close();
	}

}
