import yt
from yt.config import ytcfg

ytcfg.set("yt", "internals", "dask_allowed", False)
ytcfg.set("yt", "internals", "dask_enabled", True)

ds = yt.load("snapshot_033/snap_033.0.hdf5")
ad = ds.all_data()
data = ad[("PartType0", "Density")]

import yt
from yt.config import ytcfg

ytcfg.set("yt", "internals", "dask_allowed", True)
ytcfg.set("yt", "internals", "dask_enabled", False)

ds = yt.load("snapshot_033/snap_033.0.hdf5")
ad = ds.all_data()
data = ad[("PartType0", "Density")]


import yt
from yt.config import ytcfg

ytcfg.set("yt", "internals", "dask_allowed", True)
ytcfg.set("yt", "internals", "dask_enabled", True)

ds = yt.load("snapshot_033/snap_033.0.hdf5")
ad = ds.all_data()
data = ad[("PartType0", "Density")]


import yt
from yt.config import ytcfg

ytcfg.set("yt", "internals", "dask_allowed", True)
ytcfg.set("yt", "internals", "dask_enabled", True)

ds = yt.load("snapshot_033/snap_033.0.hdf5")
ad = ds.sphere(ds.domain_center, ds.domain_width.min()/4.)
data = ad[("PartType0", "Density")]
