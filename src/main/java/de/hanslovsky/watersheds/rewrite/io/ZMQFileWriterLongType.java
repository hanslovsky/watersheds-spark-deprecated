package de.hanslovsky.watersheds.rewrite.io;

import java.io.Serializable;
import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.util.IntervalIndexer;
import net.imglib2.util.Intervals;

public class ZMQFileWriterLongType implements FileWriter< LongType >, Serializable
{

	private static final long serialVersionUID = -6597932001747040186L;

	private final String addr;

	public ZMQFileWriterLongType( final String addr )
	{
		super();
		this.addr = addr;
	}

	@Override
	public void write( final long[] offset, final long[] dims, final Iterable< LongType > target )
	{
		final long[] max = new long[ offset.length ];
		for ( int d = 0; d < max.length; ++d )
			max[ d ] = offset[ d ] + dims[ d ] - 1;
		final int size = ( int ) Intervals.numElements( dims );
		final byte[] bytes = new byte[ size * Long.BYTES ];
		final ByteBuffer bb = ByteBuffer.wrap( bytes );
		for ( final LongType l : target )
			bb.putLong( l.get() );

		final Gson localGson = new Gson();
		final JsonObject obj = new JsonObject();
		obj.add( "min", localGson.toJsonTree( offset ) );
		obj.add( "max", localGson.toJsonTree( max ) );
		obj.add( "data", localGson.toJsonTree( DatatypeConverter.printBase64Binary( bytes ) ) );
		obj.addProperty( "id", IntervalIndexer.positionToIndex( offset, dims ) );

		final ZContext context = new ZContext();
		final Socket socket = context.createSocket( ZMQ.REQ );
		socket.connect( addr );
		socket.send( obj.toString() );
		socket.recv();
		context.close();
	}

}
