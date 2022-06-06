from typing import Optional

from yt.config import ytcfg
import yt
import argparse
import numpy as np

# yt : internals config options:
# dask_allowed switches between original and daskified reader
# dask_enabled will use dask in new reader, otherwise passthrough funcs


class TestContainer:
    def __init__(self,
                 fn: str,
                 selector_type: str,
                 test_type: str,
                 field_type: str,
                 field: str,
                 store_selection: Optional[bool] = False,
                 storage_file: Optional[str] = None,
                 ):
        self.fn = fn
        self.selector_type = selector_type
        self.test_type = test_type
        self.field = (field_type, field)
        self.store_selection = store_selection
        if storage_file is None and store_selection:
            raise RuntimeError("Must provide storage_file if store_selection is True")
        self.storage_file = storage_file
        self.setup_test_type()

    def setup_test_type(self):
        if self.test_type == "original":
            ytcfg.set("yt", "internals", "dask_allowed", False)
        elif self.test_type == "dask_enabled":
            ytcfg.set("yt", "internals", "dask_allowed", False)
            ytcfg.set("yt", "internals", "dask_enabled", True)
        elif self.test_type == "dask_disabled":
            ytcfg.set("yt", "internals", "dask_allowed", False)
            ytcfg.set("yt", "internals", "dask_enabled", False)

    def run_test(self):
        ds = yt.load(self.fn)
        selection = self.get_selection(ds)
        data = selection[self.field]
        if self.store_selection:
            np.save(self.storage_file, data)

    def get_selection(self, ds):
        if self.selector_type == "all_data":
            return ds.all_data()
        elif self.selector_type == "sphere":
            min_dim = 0.5 * ds.domain_width.min()
            return ds.sphere(ds.domain_center, min_dim)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    choices = ["original", "dask_enabled", "dask_disabled"]
    parser.add_argument("test", help="test type", type=str, choices=choices)
    parser.add_argument("selector", help="selector", type=str, choices=["all_data", "sphere"])
    parser.add_argument("field_type", help="particle type", type=str)
    parser.add_argument("field", help="field", type=str)
    parser.add_argument("--fn", help="file to test", type=str, default="IsolatedGalaxy/galaxy0030/galaxy0030")
    parser.add_argument("--store_selection", help="store the selected data in a file", type=int, default=0)

    args = parser.parse_args()
    test = TestContainer(
        args.fn,
        args.selector,
        args.test,
        args.field_type,
        args.field,
        store_selection=bool(args.store_selection),
        storage_file=args.storage_file,
    )
    test.run_test()




