from bounded_pool_executor import BoundedProcessPoolExecutor
import concurrent.futures
from datetime import datetime
import os
import pickle
import rasterio
import asyncio

from ra import degradation
from ra import zonal_statistics


zone_name = "BpsZonRobGb_wgs84_nc"
gcs_degradation_path = "gs://fuelcast-data/degradation/"
gcs_rpms_path = "gs://fuelcast-data/rpms/"

zone_raster_path = f"{gcs_degradation_path}{zone_name}/{zone_name}.tif" 
data_raster_path = f"./data/{zone_name}/rpms_stack.tif"
dummy_path = "./test.tif"

out_path = [
    f"./output/{zone_name}_mean_t.tif",
    f"./output/{zone_name}_mean_p_adj.tif",
    f"./output/{zone_name}_slope_t.tif",
    f"./output/{zone_name}_slope_p_adj.tif",
]

if not os.path.exists("./output/"):
    os.makedirs("./output/")

stats_pickle_path = f"./output/{zone_name}_zs.pkl"


BLOCKSIZE = 1024

nodata = -3.4e38


def main_process(task, zone_file, data_file, out_file, queue_size=1):
    """
    This function constantly tries to write to the destination raster, which makes it suitable for tasks that are
    done in one pass of the data. For things requiring multiple passes, use a function that doesn't write until
    the data processing is done.
    :param task:
    :param zone_file:
    :param data_file:
    :param out_file:
    :param queue_size:
    :return:
    """
    deg = degradation.Degradation()

    func = deg.degradation

    # Start rasterio and set environment variables as needed
    with rasterio.Env():
        with rasterio.open(zone_file) as zone_src:
            # Copy the zone_src dataset parameters for the output dataset, and set up tiles
            profile = zone_src.profile
            profile.update(blockxsize=BLOCKSIZE, blockysize=BLOCKSIZE, tiled=True)

            with rasterio.open(out_file, "w", **profile) as dst:
                with rasterio.open(data_file) as data_src:

                    zone_windows = [window for ij, window in dst.block_windows()]
                    # This will track remaining zone_windows. Might
                    window_count = len(zone_windows)
                    print("Window Count:", window_count)

                    # BoundedProcessPoolExecutor expands concurrent.futures.ProcessPoolExecutor to include a semaphore
                    # that blocks process creation when max_workers are active. This keeps memory footprint low.
                    with BoundedProcessPoolExecutor(max_workers=queue_size) as executor:

                        # Create a streaming iterator for zone_windows
                        def stream():
                            yield from iter(zone_windows)

                        # Just a compact function for reading the zone_src dataset
                        def read_zone_ds(window):
                            return zone_src.read(window=window)

                        def read_data(z_window):
                            bounds = zone_src.window_bounds(z_window)
                            data_window = data_src.window(*bounds)
                            return data_src.read(window=data_window)

                        streamer = stream()

                        # This gets redefined every time the streamer iterates, and the object is a set of not
                        # done futures
                        futures = set()

                        # This is our own collection that remembers what window the future object used. It's not reset
                        # every iteration so del finished futures to keep memory low
                        futures_and_windows = dict()

                        # Note that using two collections effectively double active memory footprint... maybe there's
                        # a way around this. But, a set is what the concurrent.futures.wait returns, and that's how you
                        # write finished data and move on.

                        # Process each window
                        for w in streamer:
                            # Multiple zone_windows can finish simultaneously (see below), so attempt to fill the
                            # semaphore every time
                            for i in range(queue_size - len(futures)):
                                try:
                                    window = next(streamer)
                                    ex = executor.submit(
                                        func, read_zone_ds(window), read_data(window)
                                    )
                                    futures_and_windows[ex] = window
                                    futures.add(ex)
                                except StopIteration:
                                    pass

                            # Add the window from the original streamer generator
                            ex = executor.submit(
                                func, read_zone_ds(w), read_data(window)
                            )
                            futures_and_windows[ex] = w
                            futures.add(ex)

                            # When at least one future finishes, get the completed data and do what you want to
                            done, futures = concurrent.futures.wait(
                                futures, return_when=concurrent.futures.FIRST_COMPLETED
                            )

                            for future in done:
                                data = future.result()
                                window = futures_and_windows[future]

                                window_count -= 1
                                print(
                                    f"Remaining: {window_count} || {window} || Size of futures: {len(futures)}"
                                )

                                dst.write(data, window=window)
                                del futures_and_windows[future]

                        # Finish remaining tasks after all zone_windows have been assigned
                        done, futures = concurrent.futures.wait(
                            futures, return_when=concurrent.futures.ALL_COMPLETED
                        )

                        for future in done:
                            data = future.result()
                            window = futures_and_windows[future]
                            print(f"Writing data: window={window}")
                            # with write_lock:
                            dst.write(data, window=window)

                            del futures_and_windows[ex]
    return


def main_statistics(
    task, zone_file, data_file, out_files, queue_size=10, *args, **kwargs
):
    zs = zonal_statistics.ZonalStatistics()

    if task == "collect":
        accumulator = zonal_statistics.StatAccumulator()
        func = zs.data_collector
    elif task == "degradation":
        if "acc" in kwargs:
            accumulator = kwargs["acc"]
        else:
            raise ValueError()
        func = zs.t_test

    # Start rasterio and set environment variables as needed
    with rasterio.Env():
        with rasterio.open(zone_file) as zone_src:
            # Copy the zone_src dataset parameters for the output dataset, and set up tiles
            profile = zone_src.profile
            profile.update(
                blockxsize=BLOCKSIZE,
                blockysize=BLOCKSIZE,
                tiled=True,
                dtype="float32",
                compress="DEFLATE",
                nodata=nodata,
            )

            if task == "degradation":
                mean_t_raster = rasterio.open(out_files[0], "w", **profile)
                mean_p_raster = rasterio.open(out_files[1], "w", **profile)
                slope_t_raster = rasterio.open(out_files[2], "w", **profile)
                slope_p_raster = rasterio.open(out_files[3], "w", **profile)

            dummy = rasterio.open(dummy_path, "w", **profile)

            with rasterio.open(data_file) as data_src:

                zone_windows = [window for ij, window in dummy.block_windows()]
                # This will track remaining zone_windows. Might
                window_count = len(zone_windows)
                print("Window Count:", window_count)

                # BoundedProcessPoolExecutor expands concurrent.futures.ProcessPoolExecutor to include a semaphore
                # that blocks process creation when max_workers are active. This keeps memory footprint low.
                with BoundedProcessPoolExecutor(max_workers=queue_size) as executor:

                    # Create a streaming iterator for zone_windows
                    def stream():
                        yield from iter(zone_windows)

                    # Just a compact function for reading the zone_src dataset
                    def read_zone_ds(z_window):
                        return zone_src.read(window=z_window)

                    # For handling different extents (bot not projections!), pass the zone window, convert to
                    # lat/long, and get the appropriate window for the data
                    def read_data(z_window):
                        bounds = zone_src.window_bounds(z_window)
                        d_window = data_src.window(*bounds)
                        return data_src.read(window=d_window), d_window

                    # Set up a window generator
                    streamer = stream()

                    # This gets redefined every time the streamer iterates, and the object is a set of not
                    # done futures
                    futures = set()

                    # This is our own collection that remembers what window the future object used. It's not reset
                    # every iteration so del finished futures to keep memory low
                    futures_and_windows = dict()

                    # Process each window
                    for w in streamer:
                        # Multiple zone_windows can finish simultaneously (see below), so attempt to fill the
                        # semaphore every time
                        for i in range(queue_size - len(futures)):
                            try:
                                stream_window = next(streamer)
                                zone = read_zone_ds(stream_window)
                                data, data_window = read_data(stream_window)

                                func_args = {"zone_data": zone, "val_data": data}

                                if task == "degradation":
                                    func_args["statistics"] = accumulator

                                ex = executor.submit(func, func_args)
                                futures.add(ex)
                                futures_and_windows[ex] = stream_window
                            except StopIteration:
                                pass

                        # Add the window from the original streamer generator
                        data, data_window = read_data(w)
                        zone = read_zone_ds(w)

                        func_args = {"zone_data": zone, "val_data": data}

                        if task == "degradation":
                            func_args["statistics"] = accumulator

                        ex = executor.submit(func, func_args)
                        futures.add(ex)
                        futures_and_windows[ex] = w

                        # When at least one future finishes, get the completed data and do what you want to
                        done, futures = concurrent.futures.wait(
                            futures, return_when=concurrent.futures.FIRST_COMPLETED
                        )

                        for future in done:
                            data = future.result()
                            window = futures_and_windows[future]

                            if task == "collect":
                                [accumulator.update(zone, data[zone]) for zone in data]
                            else:
                                data = [
                                    data[i, :, :].reshape(
                                        1, data.shape[1], data.shape[2]
                                    )
                                    for i in range(4)
                                ]
                                mean_t_raster.write(data[0], window=window)
                                mean_p_raster.write(data[1], window=window)
                                slope_t_raster.write(data[2], window=window)
                                slope_p_raster.write(data[3], window=window)

                            window_count -= 1
                            print(
                                f"Remaining: {window_count} || {window} || Size of futures: {len(futures)}"
                            )

                            del futures_and_windows[future]

                    # Finish remaining tasks after all zone_windows have been assigned
                    done, futures = concurrent.futures.wait(
                        futures, return_when=concurrent.futures.ALL_COMPLETED
                    )

                    for future in done:
                        data = future.result()
                        window = futures_and_windows[future]

                        if task == "collect":
                            [accumulator.update(zone, data[zone]) for zone in data]
                        else:
                            data = [
                                data[i, :, :].reshape(1, data.shape[1], data.shape[2])
                                for i in range(4)
                            ]
                            mean_t_raster.write(data[0], window=window)
                            mean_p_raster.write(data[1], window=window)
                            slope_t_raster.write(data[2], window=window)
                            slope_p_raster.write(data[3], window=window)

                        window_count -= 1
                        print(
                            f"Remaining: {window_count} || {window} || Size of futures: {len(futures)}"
                        )

                        del futures_and_windows[future]

                    if task == "collect":
                        # Merge the collected statistic objects
                        print("Merging collected statistics")
                        accumulator.merge()

                        print("Writing statistics to file")
                        accumulator.write()

                        with open(stats_pickle_path, "wb") as f:
                            pickle.dump(accumulator, f)

                    else:
                        mean_t_raster.close()
                        mean_p_raster.close()
                        slope_t_raster.close()
                        slope_p_raster.close()
                        dummy.close()

                        os.remove(dummy_path)

        return accumulator

# async def raster_stacker(in_ds, out_ds, bounds):
def raster_stacker(in_ds, out_ds, bounds):
    with rasterio.open(in_ds, chunks=(1, 1024, 1024), lock=False) as src_ds:
        win = src_ds.window(
            bottom=bounds.bottom,
            right=bounds.right,
            top=bounds.top,
            left=bounds.left,
        )
        print(f"in: {in_ds} || {win}")
        out_ds.write_band(id, src_ds.read(1, window=win))


async def main_run():
    with rasterio.Env(GDAL_NUM_THREADS="ALL_CPUS", verbose=2, GOOGLE_APPLICATION_CREDENTIALS=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "fuelcast-storage-credentials.json")):
        zone_ds = rasterio.open(zone_raster_path, chunks=(1024, 1024))
        bounds = zone_ds.bounds
        profile = zone_ds.profile
        profile.update(
            blockxsize=1024,
            blockysize=1024,
            tiled=True,
            compress="DEFLATE",
            predictor=2,
            BIGTIFF="Yes",
        )

        od = f"./data/{zone_name}"
        if not os.path.exists(od):
            os.makedirs(od)

        files = list()
        for y in range(1985, 2022):
            if y == 2012:
                continue
            # f = f"./data/{zone_name}/rpms_{y}_mean.tif"
            f = f"gs://fuelcast-data/rpms/{y}/rpms_{y}.tif"
            files.append(f)

        meta = zone_ds.meta
        meta.update(count=len(files))
        profile.update(count=len(files))

        print("Stacking raster")

        stack_path = f"./data/{zone_name}/rpms_stack.tif"

        if os.path.exists(stack_path):
            print(f"Stacked raster {stack_path} already exists.")
        else:
            with BoundedProcessPoolExecutor(max_workers=20) as executor:
                with rasterio.open(stack_path, "w", **profile) as dst:
                    print(f"out: {dst} || {dst.bounds}")

                    background_tasks = set()

                    for id, layer in enumerate(files, start=1):
                        print(f"in: {layer}")
                        future = executor.submit(raster_stacker, layer, dst, bounds)
                        
                        # Add task to the set. This creates a strong reference.
                        background_tasks.add(future)

                    concurrent.futures.wait(background_tasks)
                

        print("Calculating zonal statistics")
        acc = main_statistics(
            "collect", zone_raster_path, data_raster_path, out_path, 60
        )

        with open(stats_pickle_path, "rb") as f:
            acc = pickle.load(f)

        print("Running degradation")
        start = datetime.now()
        main_statistics(
            "degradation", zone_raster_path, data_raster_path, out_path, 60, acc=acc
        )
        stop = datetime.now()
        print("Total runtime:", (stop - start).seconds / 60, "minutes")

        print("Finished")

if __name__ == "__main__":
    asyncio.run(main_run())
    
