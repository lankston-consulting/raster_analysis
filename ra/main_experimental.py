# from bounded_pool_executor.bounded_pool_executor import BoundedProcessPoolExecutor
# import concurrent.futures
# import rasterio
#
# from ra import degradation
#
#
# zone_raster_path = '/mnt/hgfs/MCR/BPS_PrvOwn_Cmb/BpsPrvOwnCmb.tif'
# data_raster_path = '/mnt/hgfs/MCR/RPMS_Stack8419_TIF/rpms8419.tif'
# out_path = '/mnt/hgfs/MCR/Cowcaster/test.tif'
#
# BLOCKSIZE = 2048
#
#
# def main(task, infile, outfile, num_workers=20):
#     '''This works but doesn't limit memory footprint. Both ProcessPool and ThreadPool work.'''
#     deg = degradation_chunk.Degradation()
#
#     if task == 'degradation':
#         func = deg.degradation
#     else:
#         func = deg.degradation
#
#     with rasterio.Env():
#         with rasterio.open(infile) as src:
#             profile = src.profile
#             profile.update(blockxsize=BLOCKSIZE, blockysize=BLOCKSIZE, tiled=True)
#
#             with rasterio.open(outfile, 'w', **profile) as dst:
#                 windows = [window for ij, window in dst.block_windows()]
#
#                 # This right here doesn't block
#                 data_gen = (src.read(window=window) for window in windows)
#
#                 with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
#                     for window, result in zip(windows, executor.map(func, data_gen)):
#                         dst.write(result, window=window)
#
#     return
#
#
# def main_futures(task, infile, outfile):
#     '''This is based on the example here https://gist.github.com/sgillies/b90a79917d7ec5ca0c074b5f6f4857e3#file-cfrw-py
#     But it doens't use CHUNK right... it never advanced the slice'''
#     deg = degradation.Degradation()
#     func = deg.degradation_r
#
#     queued_windows = []
#     finished_windows = []
#
#     with rasterio.Env():
#         with rasterio.open(infile) as src:
#             profile = src.profile
#             profile.update(blockxsize=BLOCKSIZE, blockysize=BLOCKSIZE, tiled=True)
#
#             with rasterio.open(outfile, 'w', **profile) as dst:
#                 windows = [window for ij, window in dst.block_windows()]
#                 print('Window Count:', len(windows))
#
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#
#                     ITER = 0
#                     STARTCHUNK = CHUNK
#                     ENDCHUNK = STARTCHUNK + CHUNK - 1
#
#                     group = islice(windows, STARTCHUNK, ENDCHUNK)
#
#                     read_lock = threading.Lock()
#                     write_lock = threading.Lock()
#
#
#                     futures = {executor.submit(func, read_lock, src, window) for window in group}
#
#                     while futures:
#                         done, futures = concurrent.futures.wait(
#                             futures, return_when=concurrent.futures.FIRST_COMPLETED
#                         )
#
#                         for future in done:
#                             window, data = future.result()
#                             #finished_windows.append(window)
#                             # print(f"Writing data: window={window}")
#                             with write_lock:
#                                 dst.write(data, window=window)
#
#                         ITER = ITER + 1
#                         STARTCHUNK = CHUNK * ITER
#                         ENDCHUNK = STARTCHUNK + CHUNK - 1
#
#                         group = islice(windows, STARTCHUNK, ENDCHUNK)
#                         print('ITER finished', ITER)
#                         for window in group:
#                             futures.add(executor.submit(func, read_lock, src, window))
#
#     s_q = sorted(queued_windows)
#     s_f = sorted(finished_windows)
#
#     print("Queued:", len(queued_windows))
#     print("Finished and written:", len(finished_windows))
#
#     return
#
#
# def main_futures_queue(task, infile, outfile, queue_size=20):
#     """This does work, but it fails out when the generator is empty. Also it uses degradation_r
#     with 2 returns, (window, output) which might not be the current state of the code."""
#     deg = degradation_chunk.Degradation()
#     func = deg.degradation_r
#
#     with rasterio.Env():
#         with rasterio.open(infile) as src:
#             profile = src.profile
#             profile.update(blockxsize=BLOCKSIZE, blockysize=BLOCKSIZE, tiled=True)
#
#             with rasterio.open(outfile, 'w', **profile) as dst:
#                 windows = [window for ij, window in dst.block_windows()]
#                 print('Window Count:', len(windows))
#
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#
#                     def stream():
#                         yield from iter(windows)
#
#                     read_lock = threading.Lock()
#                     write_lock = threading.Lock()
#
#                     streamer = stream()
#                     futures = {executor.submit(func, read_lock, src, next(streamer)) for w in range(queue_size)}
#
#                     for w in streamer:
#                         futures.add(executor.submit(func, read_lock, src, w))
#
#                         done, futures = concurrent.futures.wait(
#                             futures, return_when=concurrent.futures.FIRST_COMPLETED
#                         )
#
#                         for future in done:
#                             window, data = future.result()
#                             print(f"Writing data: window={window}")
#                             with write_lock:
#                                 dst.write(data, window=window)
#
#                     done, futures = concurrent.futures.wait(
#                         futures, return_when=concurrent.futures.ALL_COMPLETED
#                     )
#
#                     for future in done:
#                         window, data = future.result()
#                         print(f"Writing data: window={window}")
#                         with write_lock:
#                             dst.write(data, window=window)
#
#     return
#
#
# def main_futures_better_queue(task, infile, outfile, queue_size=20):
#     deg = degradation_chunk.Degradation()
#
#     with rasterio.Env():
#         with rasterio.open(infile) as src:
#             profile = src.profile
#             profile.update(blockxsize=BLOCKSIZE, blockysize=BLOCKSIZE, tiled=True)
#
#             with rasterio.open(outfile, 'w', **profile) as dst:
#                 windows = [window for ij, window in dst.block_windows()]
#                 # print('Window Count:', len(windows))
#
#                 window_queue = queue.Queue(maxsize=queue_size)
#
#                 def process():
#                     for w in windows:
#                         window_queue.put(w)
#
#                 with concurrent.futures.ThreadPoolExecutor() as executor:
#                     read_lock = threading.Lock()
#                     write_lock = threading.Lock()
#
#                     # futures = {executor.submit(func, read_lock, src, next(streamer))}
#                     futures = {executor.submit(process): 'FEEDER DONE'}
#
#                     while futures:
#                         done, futures = concurrent.futures.wait(
#                             futures, return_when=concurrent.futures.FIRST_COMPLETED
#                         )
#
#                         while not window_queue.empty():
#                             window = window_queue.get()
#                             futures[executor.submit(deg.degradation_r, read_lock, src, window)] = window
#
#                         for future in done:
#                             data = futures[future].result()
#                             print(f"Writing data: window={window}")
#                             with write_lock:
#                                 dst.write(data, window=window)
#
#                             del futures[future]
#
#
# def main_locking(task, infile, outfile, num_workers=20):
#     deg = degradation.Degradation()
#
#     with rasterio.Env():
#         with rasterio.open(infile) as src:
#             profile = src.profile
#             profile.update(blockxsize=BLOCKSIZE, blockysize=BLOCKSIZE, tiled=True)
#
#             with rasterio.open(outfile, 'w', **profile) as dst:
#                 windows = [window for ij, window in dst.block_windows()]
#
#                 read_lock = threading.Lock()
#                 write_lock = threading.Lock()
#
#                 def process(window):
#                     with read_lock:
#                         src_array = src.read(window=window)
#                     result = deg.degradation(src_array)
#                     with write_lock:
#                         dst.write(result, window=window)
#
#                 with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
#                     executor.map(process, windows)
