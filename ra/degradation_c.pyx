# cython: boundscheck=False
# cython: language_level=3

from ra import raster_worker
import numpy as np


class Degradation(object):
    def __init__(self, *args, **kwargs):
        #self.zone_raster_path = kwargs['zone_raster']

        # TODO check for banded raster vs list of rasters
        #self.data_raster_path = kwargs['data_raster']
        return


    def degradation(self, int[:, :, :] data):

        cdef int I, J, K
        cdef int i, j, k
        cdef double val

        I = data.shape[0]
        J = data.shape[1]
        K = data.shape[2]

        output = np.empty((I, J, K), dtype='int16')

        cdef int[:, :, :] output_view = output

        with nogil:
            for i in range(I):
                for j in range(J):
                    for k in range(K):
                        val = <double>data[i, j, k]
                        output_view[i, j, k] = <int>val
        return output


    def run(self):
        pass
