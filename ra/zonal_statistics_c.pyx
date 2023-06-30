import numpy as np


class Stat(object):
    def __init__(self, zone):
        self.zone = 0
        self.mean = 0
        self.std = 0
        self.n = 0
        self.data = list()

    def add_data(self, data):
        self.data.append(data)

    def calc_stats(self):
        self.data = np.array(self.data)
        self.mean = np.mean(self.data)
        self.std = np.std(self.data)
        self.n = len(self.data)

        # Clean up the object to reduce memory footprint
        del self.data


class ZonalStatistics(object):
    def __init__(self):

        return

    def local_statistics(self, int[:,:,:] zone_data, int[:,:,:] val_data):

        stats = dict()

        cdef int I, J, K
        cdef int i, j, k
        cdef int zone
        cdef int[] data

        I = zone_data.shape[0]
        J = zone_data.shape[1]
        K = zone_data.shape[2]

        for i in range(I):
            for j in range(J):
                for k in range(K):
                    zone = zone_data[i, j, k]
                    data = val_data[:, j, k]

                    if zone > 0:
                        if zone not in stats:
                            stats[zone] = Stat(zone)
                        stats[zone].add_data(data)

        for z in stats:
            stats[z].calc_stats()

        return stats

    def combine_local_statistics(self, statistics):
        return
