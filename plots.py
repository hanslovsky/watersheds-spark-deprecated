#!/usr/bin/env python
# coding: utf-8

import matplotlib.pyplot as plt
import numpy as np

countsAndBlocks = (
	(6200,812),
	(3956,114),
	(779,20),
	(330,6),
	(217,1) )
	
countsAndBlocks2 = (
	(6200,317),
	(3536,48),
	(321,6),
	(233,1) )
	
countsAndBlocks3 = (
	(6200,317),
	(3500,305),
	(329,305),
	(306,305),
	(305,305) )

countsAndBlocks4 = (
	(6200,317),
	(3463,98),
	(259,70),
	(204,67),
	(142,67) )
	
ratios = np.array( [ c[0] / c[1] for c in countsAndBlocks ] )
ratios2 = np.array( [ c[0] / c[1] for c in countsAndBlocks2 ] )
ratios3 = np.array( [ c[0] / c[1] for c in countsAndBlocks3 ] )
ratios4 = np.array( [ c[0] / c[1] for c in countsAndBlocks4 ] )
plt.plot( ratios, label='aff and count, block split based on weight' )
plt.plot( ratios2, label='aff and count, block split based on aff' )
plt.plot( ratios3, label='only aff' )
plt.plot( ratios4, label='aff and log count' )
plt.legend()

plt.show()
