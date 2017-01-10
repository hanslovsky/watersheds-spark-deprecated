package de.hanslovsky.watersheds.rewrite.io;

public interface FileOpener< T >
{
	public void open( long[] offset, long[] dims, Iterable< T > target );
}
