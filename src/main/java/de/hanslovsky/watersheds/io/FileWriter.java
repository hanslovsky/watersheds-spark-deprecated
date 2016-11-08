package de.hanslovsky.watersheds.io;

public interface FileWriter< T >
{
	public void write( long[] offset, long[] dims, Iterable< T > target );
}
