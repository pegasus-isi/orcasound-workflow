#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path
from typing import List
from Pegasus.api import *

def parse_args(args: List[str] = sys.argv[1:]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="site catalog generator....")

    parser.add_argument(
        "--edge-only",
        default=False,
        action="store_true",
        help="Use site catalog for edge only scenario"
    )

    parser.add_argument(
        "--cloud-only",
        default=False,
        action="store_true",
        help="Set MACHINE_SPECIAL_ID to 0 for condorpool"
    )

    return parser.parse_args(args)

def generate_edge_only_sc() -> SiteCatalog:
    WORK_DIR = Path(__file__).parent.resolve()
    LOCAL_STORAGE_DIR = str(WORK_DIR / "wf-output")
    LOCAL_SCRATCH_DIR = str(WORK_DIR / "wf-scratch")

    sc = SiteCatalog()

    local = Site("local")

    local_storage = Directory(Directory.LOCAL_STORAGE, LOCAL_STORAGE_DIR)
    local_storage.add_file_servers(FileServer("file://" + LOCAL_STORAGE_DIR, Operation.GET))
    local_storage.add_file_servers(FileServer("scp://panorama@10.100.101.107/" + LOCAL_STORAGE_DIR, Operation.PUT))
    local_scratch = Directory(Directory.SHARED_SCRATCH, LOCAL_SCRATCH_DIR)
    local_scratch.add_file_servers(FileServer("file://" + LOCAL_SCRATCH_DIR, Operation.ALL))
    local.add_directories(local_storage, local_scratch)
    local.add_pegasus_profile(SSH_PRIVATE_KEY="/home/panorama/.ssh/storage_key")

    condorpool = Site("condorpool")
    condorpool.add_pegasus_profile(style="condor")
    condorpool.add_condor_profile(universe="vanilla", requirements="MACHINE_SPECIAL_ID == 1")
    staging_dir = Directory(Directory.SHARED_SCRATCH, "/home/panorama/public_html")
    staging_dir.add_file_servers(FileServer("file:///home/panorama/public_html", Operation.ALL))
    condorpool.add_directories(staging_dir)

    sc.add_sites(local, condorpool)

    return sc

def generate_sc(cloud_only: bool) -> SiteCatalog():
    WORK_DIR = Path(__file__).parent.resolve()
    LOCAL_STORAGE_DIR = str(WORK_DIR / "wf-output")
    LOCAL_SCRATCH_DIR = str(WORK_DIR / "wf-scratch")

    sc = SiteCatalog()

    local = Site("local")
    local_storage = Directory(Directory.LOCAL_STORAGE, LOCAL_STORAGE_DIR)
    local_storage.add_file_servers(FileServer("file://" + LOCAL_STORAGE_DIR, Operation.ALL))
    local_scratch = Directory(Directory.SHARED_SCRATCH, LOCAL_SCRATCH_DIR)
    local_scratch.add_file_servers(FileServer("file://" + LOCAL_SCRATCH_DIR, Operation.ALL))
    local.add_directories(local_storage, local_scratch)
    local.add_pegasus_profile(SSH_PRIVATE_KEY="/home/panorama/.ssh/storage_key")

    condorpool = Site("condorpool")
    condorpool.add_pegasus_profile(style="condor")
    condorpool.add_condor_profile(universe="vanilla")
    if cloud_only:
        condorpool.add_condor_profile(requirements="MACHINE_SPECIAL_ID == 0")


    staging = Site("staging")
    staging_dir = Directory(Directory.SHARED_SCRATCH, "/home/panorama/public_html")
    staging_dir.add_file_servers(
                FileServer("http://10.100.101.107/~panorama/", Operation.GET),
                FileServer("scp://panorama@10.100.101.107/home/panorama/public_html/", Operation.PUT)
            )
    staging.add_directories(staging_dir)

    sc.add_sites(local, condorpool, staging)

    return sc

if __name__=="__main__":
    args = parse_args()
    sc = None
    if args.edge_only:
        sc = generate_edge_only_sc()
    else:
        sc = generate_sc(args.cloud_only)

    sc.write(sys.stdout)
    sc.write()

