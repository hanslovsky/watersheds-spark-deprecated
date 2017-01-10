package de.hanslovsky.watersheds.rewrite;

public interface MergerService
{
	public void addMerge( long n1, long n2, long n, double w );

	public void finalize();

}