import concurrent.futures
import numpy as np
from statsmodels.regression.linear_model import OLS, GLSAR
from statsmodels.stats.multitest import fdrcorrection, multipletests
from scipy.stats import t as ttest
import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=UserWarning)


class Stat(object):
    def __init__(self, zone):
        self.zone = zone
        self.mean = 0
        self.std = 0
        self.n = 0
        self.data = list()

    def add_data(self, data):
        self.data.append(data)

    def calc_stats(self):
        if self.data:
            self.data = np.array(self.data)
            self.data = np.ma.masked_where(self.data < 0, self.data)
            self.mean = np.ma.mean(self.data, axis=0)
            self.std = np.ma.std(self.data, axis=0)
            self.n = np.ma.count(self.data, axis=0)
            self.n = np.ma.masked_where(self.n <= 0, self.n)  # n of 0 screws up combining groups later

            # Clean up the object to reduce memory footprint
            del self.data


class StatAccumulator(object):
    def __init__(self, update_size=500):
        # .statistics will be a collection keyed by zone that references a list of Stat objects. As data is collected
        # and the Stat object gets to a determined size, statistics will be calculated and the data will be deleted.
        # New data will go in the accumulator as a new stat object. Rinse and repeat.
        self.statistics = dict()
        self._update_size = update_size
        self.merged_stats = dict()

    def update(self, zone, new_stats, force=False):
        if zone not in self.statistics:
            self.statistics[zone] = list()
            self.statistics[zone].append(new_stats)
        else:
            stats_col = self.statistics[zone]
            old_stats = stats_col[-1]  # Get the latest stat collection

            # If the latest record has over x records, create a new object
            if len(old_stats.data) > self._update_size or force:
                old_stats.calc_stats()  # Clean up the memory footprint
                self.statistics[zone].append(new_stats)
            else:
                [old_stats.data.append(d) for d in new_stats.data]
        return

    def update_cochrane(self, zone, new_stats):
        if zone not in self.statistics:
            self.statistics[zone] = new_stats
        else:
            old_stats = self.statistics[zone]

            tn = old_stats.n + new_stats.n
            tmean = np.ma.add(old_stats.n * old_stats.mean, new_stats.n * new_stats.mean) / tn

            # tsd = np.ma.sqrt(((old_stats.n-1) * np.power(old_stats.std, 2) + (new_stats.n - 1) * np.power(new_stats.std, 2) + old_stats.n * new_stats.n / (old_stats.n + new_stats.n) * (np.power(old_stats.mean, 2) + np.power(new_stats.mean, 2) - 2 * old_stats.mean * new_stats.mean)) / (old_stats.n + new_stats.n - 1))

            # N1 - 1 * SD1^2
            t1 = np.ma.add(old_stats.n, -1)
            t2 = np.ma.power(old_stats.std, 2)
            tr = np.ma.multiply(t1, t2)

            # N2 - 1 * SD2^2
            t1 = np.ma.add(new_stats.n, -1)
            t2 = np.ma.power(new_stats.std, 2)
            ts = np.ma.add(t1, t2)

            # (N1*N2)/(N1+N2)
            t1 = np.ma.multiply(old_stats.n, new_stats.n)
            t2 = np.ma.add(old_stats.n, new_stats.n)
            tt = np.ma.divide(t1, t2)

            # (M1^2 + M2^2)
            t1 = np.ma.power(old_stats.mean, 2)
            t2 = np.ma.power(new_stats.mean, 2)
            tu = np.ma.add(t1, t2)

            # 2*M1*M2
            tv = np.ma.multiply(np.ma.multiply(old_stats.mean, new_stats.mean), 2)

            # N1 + N2 -1
            tx = np.ma.add(np.ma.add(old_stats.n, new_stats.n), -1)

            xr = np.ma.add(tr, ts)
            xs = np.ma.subtract(tu, tv)
            xt = np.ma.multiply(tt, xs)
            xu = np.ma.add(xr, xt)

            z = np.ma.divide(xu, tx)

            tsd = np.ma.sqrt(z)

            new_stats.n = tn
            new_stats.mean = tmean
            new_stats.std = tsd
            self.statistics[zone] = new_stats

    def update_multiple(self, zone):
        """
        Iterates over the statistics collection, merging mean, std, and n
        :param zone:
        :param new_stats:
        :return:
        """

        # Update the last of the stat objects
        self.statistics[zone][-1].calc_stats()

        def collect(data, tn, tx, txx):
            n = data.n
            mean = data.mean
            sd = data.std
            x = n * mean
            xx = sd**2 * (n - 1) + x**2 / n
            tn = tn + n
            tx = tx + x
            txx = txx + xx

            return tn, tx, txx

        tn, tx, txx = 0, 0, 0
        for data in self.statistics[zone]:
            tn, tx, txx = collect(data, tn, tx, txx)

        tmean = tx / tn
        tsd = np.ma.sqrt(np.ma.divide(np.ma.subtract(txx, np.ma.divide(np.ma.power(tx, 2), tn)), np.ma.add(tn, -1)))

        old_stats = self.statistics[zone][0]
        old_stats.mean = tmean
        old_stats.std = tsd
        old_stats.n = tn

        self.merged_stats[zone] = old_stats

        return

    def merge(self):
        self.merged_stats = dict()

        def chunk_gen(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i+n]

        def merge_chunk(indexes):
            key_list = list(self.statistics.keys())
            for i in indexes:
                zone = key_list[i]
                self.update_multiple(zone)

        # with concurrent.futures.ProcessPoolExecutor() as executor:
        #     futures = set()
        #     n = 100
        #     i_list = list(range(len(self.statistics)))
        #     chunks = chunk_gen(i_list, n)
        #     [futures.add(executor.submit(merge_chunk, c)) for c in chunks]
        #     _, __ = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

        [self.update_multiple(z) for z in self.statistics]
        self.statistics = self.merged_stats
        del self.merged_stats
        return

    def write(self, path='/home/robb/Documents/zone_stats.csv'):

        with open(path, 'w') as f:
            header = 'zone, year, mean, std, n\n'
            f.write(header)

            for z in self.statistics:
                data = self.statistics[z]
                for i in range(len(data.mean)):
                    line = '{0}, {1}, {2}, {3}, {4}\n'.format(z, i, data.mean[i], data.std[i], data.n[i])
                    f.write(line)


class ZonalStatistics(object):
    def __init__(self):

        return

    def data_collector(self, *args, **kwargs):

        stats = dict()

        I = args[0]['zone_data'].shape[0]
        J = args[0]['zone_data'].shape[1]
        K = args[0]['zone_data'].shape[2]

        for i in range(I):
            for j in range(J):
                for k in range(K):
                    zone = args[0]['zone_data'][i, j, k]
                    data = args[0]['val_data'][:, j, k]

                    if zone > 0:
                        if zone not in stats:
                            stats[zone] = Stat(zone)
                        stats[zone].add_data(data)


        return stats

    def t_test(self, *args, **kwargs):

        I = args[0]['zone_data'].shape[0]
        J = args[0]['zone_data'].shape[1]
        K = args[0]['zone_data'].shape[2]

        nodata = -32768

        output = np.empty((4, J, K), dtype='int16')
        output.fill(nodata)

        for i in range(I):
            for j in range(J):
                for k in range(K):
                    zone = args[0]['zone_data'][i, j, k]
                    data = args[0]['val_data'][:, j, k]

                    if zone > 0:
                        stat = args[0]['statistics'].statistics[zone]
                        vals = self._t_test_strict_r_logic(stat, data)
                        if vals is None:
                            vals = (nodata, nodata, nodata, nodata)
                        output[:, j, k] = np.array(vals)

        # This should be done all at once at the end, as it uses relative magnitudes of p to correct
        # output = np.ma.masked_where(output == nodata, output)

        # _, adj_p = multipletests(output[1, :, :], method='fdr_bh')
        # output[1, :, :] = adj_p
        # _, adj_p = multipletests(output[3, :, :], method='fdr_bh')
        # output[3, :, :] = adj_p
        # # adj_p = fdrcorrection(adj_p)

        return output

    def _t_test_orig_logic(self, stat, data):
        # Mask out NoData pixels
        data = np.ma.masked_where(data < 0, data)
        # Get the individual stats
        i_mean = np.ma.mean(data)  # This is a single value (mean over time)

        # Bail early if there's not data
        if np.ma.is_masked(i_mean):
            return None

        i_std = np.ma.std(data)
        i_n = np.ma.count(data)
        i_se = i_std / np.ma.sqrt(i_n)

        # Adjust population stats to remove point
        #### NEED TO ADJUST FOR WEIGHTED TEMPORAL MEAN
        # The -1 operations are because we removed a datapoint... maybe make this a variable
        p_n_adj = stat.n - 1
        p_mean_list = ((stat.mean * p_n_adj) - data) / p_n_adj  # This value is a list

        p_n_sum = np.ma.sum(p_n_adj)
        p_weights = np.ma.divide(stat.n, p_n_sum)
        p_weighted_mean = p_mean_list * p_weights

        p_mean = np.ma.sum(p_weighted_mean)
        p_std = np.ma.std(p_weighted_mean)
        p_n = np.ma.sum(p_n_adj)
        p_se = p_std / np.ma.sqrt(p_n)

        # Get the mean difference
        mean_diff = i_mean - p_mean
        # Standard error difference
        se = np.ma.sqrt(i_se ** 2, p_se ** 2)

        # t test
        t = mean_diff / se

        # degrees of freedom
        df = i_n + p_n - 2

        # p value
        # p = stats.t.cdf(np.abs(t), df=df) * 2

        # Adjust p for FDR

        years = list(range(1, len(p_mean_list)))
        pop_trend_model = OLS(p_mean_list, years)
        pop_trend_result = pop_trend_model.fit()

        ind_trend_model = GLSAR(data, years)
        ind_trend_result = ind_trend_model.fit()

        i = 1

    def _t_test_strict_r_logic(self, stat, data):
        # Mask out NoData pixels
        data = np.ma.masked_where(data < 0, data)
        # Get the individual stats
        i_mean = np.ma.mean(data)  # This is a single value (mean over time)

        # Bail early if there's not data
        if np.ma.is_masked(i_mean):
            return None

        i_mean_model = GLSAR(data, missing='drop')
        i_mean_result = i_mean_model.fit()
        i_se = i_mean_result.HC0_se

        # Adjust population stats to remove point
        #### NEED TO ADJUST FOR WEIGHTED TEMPORAL MEAN
        # The -1 operations are because we removed a datapoint... maybe make this a variable
        p_n_adj = stat.n - 1
        p_mean_list = ((stat.mean * p_n_adj) - data) / p_n_adj  # This value is a list
        p_mean = np.ma.mean(p_mean_list)  # This is a single value

        # Get the mean difference
        mean_diff = i_mean - p_mean

        # t test
        t = mean_diff / i_se

        # degrees of freedom
        df = i_mean_result.nobs - len(i_mean_result.params)

        # p value
        p = ttest.cdf(np.abs(t), df=df) * 2

        years = list(range(len(p_mean_list)))
        pop_trend_model = OLS(p_mean_list, years, missing='drop')
        pop_trend_result = pop_trend_model.fit()

        ind_trend_model = GLSAR(data, years, missing='drop')
        ind_trend_result = ind_trend_model.fit()

        pop_slope = pop_trend_result.params[0]
        ind_slope = ind_trend_result.params[0]

        slope_diff = ind_slope - pop_slope
        slope_se = ind_trend_result.HC0_se
        slope_t = slope_diff / slope_se

        df = ind_trend_result.nobs - len(ind_trend_result.params)

        slope_p = ttest.cdf(np.abs(slope_t), df=df) * 2

        vals = np.array([t[0], p[0], slope_t[0], slope_p[0]])
        vals = vals * 1000

        return vals.astype(int)


