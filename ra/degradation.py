import numpy as np
import rasterio


class Degradation(object):
    def __init__(self, *args, **kwargs):
        # self.zone_raster_path = kwargs['zone_raster']

        # TODO check for banded raster vs list of rasters
        # self.data_raster_path = kwargs['data_raster']
        return

    def degradation(self, data):

        I = data.shape[0]
        J = data.shape[1]
        K = data.shape[2]
        output = np.empty((I, J, K))

        for i in range(I):
            for j in range(J):
                for k in range(K):
                    val = data[i, j, k]
                    output[i, j, k] = val
        return output

    # def degradation_q(self, data, window):
    #     return window, self.degradation(data)
    #
    # def degradation_r(self, lock, src_ds, window):
    #     '''Read the raster here instead of the main thread'''
    #
    #     #print(f"Processing data: window={window}")
    #
    #     with lock:
    #         data = src_ds.read(window=window)
    #
    #     output = self.degradation(data)
    #
    #     return window, output
