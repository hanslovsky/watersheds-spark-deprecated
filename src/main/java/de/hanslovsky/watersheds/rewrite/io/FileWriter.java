package de.hanslovsky.watersheds.rewrite.io;

public interface FileWriter< T >
{
	public void write( long[] offset, long[] dims, Iterable< T > target );
}
