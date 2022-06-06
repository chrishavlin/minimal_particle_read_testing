# test : load directly from hdf chunks with dask

# is the serialization time just the np array? or is it the yt objs?

# maybe dask read only if planning to do reductions. the serialization cost for a simple read is just too high.
import h5py
import yt
import numpy as np
import dask
from dask.distributed import as_completed


def read_chunk(file, group, key, start_index, end_index):
    with h5py.File(file, "r") as fi:
        data = fi[group][key][start_index:end_index].astype(np.float64)
    return data


def get_yt_chunks(file):
    # e.g., datafiles = get_yt_chunks("snapshot_033/snap_033.0.hdf5")
    # pulls out a simplified list of files with start/end index for all_data of a ds
    ds = yt.load(file)
    ad = ds.all_data()
    ad.index._identify_base_chunk(ad)
    chunks = ad.index._chunk_io(ad, cache=False)
    chunk_list = list(ds.index.io._sorted_chunk_iterator(chunks))
    return [{'file': df.filename,
             'start_index': df.start,
             "end_index": df.end} for df in chunk_list]


def read_all_and_concat(file, field_type, field):
    # e.g., data = read_all_and_concat("snapshot_033/snap_033.0.hdf5", "PartType0", "Density")
    datafiles = get_yt_chunks(file)
    return serial_read_concat(datafiles, field_type, field)


def get_data_list_in_mem(datafiles, field_type, field):
    data_list = []
    for df in datafiles:
        i0 = df["start_index"]
        i1 = df["end_index"]
        data_list.append(read_chunk(df["file"], field_type, field, i0, i1))
    return data_list


def serial_read_concat(datafiles, field_type, field):
    data_list = get_data_list_in_mem(datafiles, field_type, field)
    data = np.concatenate(data_list, axis=0)
    return data


def read_delayed_then_concat(file, field_type, field):
    # data = read_delayed_then_concat("snapshot_033/snap_033.0.hdf5", "PartType0", "Density")
    datafiles = get_yt_chunks(file)
    return delay_then_concat(datafiles, field_type, field)


def delay_then_concat(datafiles, field_type, field):
    data_delayed = []
    for df in datafiles:
        i0 = df["start_index"]
        i1 = df["end_index"]
        data_delayed.append(dask.delayed(read_chunk)(df["file"], field_type, field, i0, i1))

    data_list = dask.compute(*data_delayed)
    data = np.concatenate(data_list, axis=0)
    return data


def read_delayed_array(datafiles, field_type, field):
    data_delayed = []
    for df in datafiles:
        i0 = df["start_index"]
        i1 = df["end_index"]
        d = dask.delayed(read_chunk)(df["file"], field_type, field, i0, i1)
        d_array = dask.array.from_delayed(d, shape=(np.nan,), dtype=np.float64)
        data_delayed.append(d_array)

    darray_val = dask.array.concatenate(data_delayed, axis=0)
    return darray_val


def sum_chunk(file, field_type, field, start_index, end_index):
    data = read_chunk(file, field_type, field, start_index, end_index)
    return data.sum()


def manual_sum_by_chunk(datafiles, field_type, field):
    val = np.float64(0.)
    for df in datafiles:
        i0 = df["start_index"]
        i1 = df["end_index"]
        sum_of_chunk = sum_chunk(df["file"], field_type, field, i0, i1)
        val += sum_of_chunk

    return val


def in_mem_from_dask_futures(client, datafiles, field_type, field):
    # return an in-memory array where chunks are read using dask futures
    data_list = []
    for df in datafiles:
        data_list.append(client.submit(read_chunk, df['file'], field_type, field, df['start_index'], df['end_index']))

    result = client.gather(data_list)
    data = np.concatenate(result, axis=0)
    return data


def sum_of_futures(client, datafiles, field_type, field):
    # a reduction example using dask futures
    future_sums = []
    for df in datafiles:
        i0 = df["start_index"]
        i1 = df["end_index"]
        future_sums.append(client.submit(sum_chunk, df["file"], field_type, field, i0, i1))

    vals = []
    for future in as_completed(future_sums):
        vals.append(future.result())
    val = np.sum(vals)
    return val
