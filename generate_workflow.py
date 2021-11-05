#!/usr/bin/env python3
import os
import sys
import logging
import tarfile
import requests
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta

#logging.basicConfig(level=logging.DEBUG)

# --- Import Pegasus API -----------------------------------------------------------
from Pegasus.api import *

class OrcasoundWorkflow():
    wf = None
    sc = None
    tc = None
    rc = None
    props = None

    dagfile = None
    wf_dir = None
    wf_name = "orcasound"
    
    s3_cache = None
    s3_files = None
    s3_bucket = "streaming-orcasound-net"
    s3_cache_location = ".s3_cache"
    s3_cache_file = ".s3_cache/streaming-orcasound-net.csv"
    s3_cache_xz = "streaming-orcasound-net.tar.xz"
    s3_cache_xz_url = "https://workflow.isi.edu/Panorama/Data/Orcasound/streaming-orcasound-net.tar.xz"
    
    # --- Init ---------------------------------------------------------------------
    def __init__(self, sensors, start_date, end_date, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.sensors = sensors
        self.start_date = int(start_date.timestamp())
        self.end_date = int(end_date.timestamp())

    
    # --- Write files in directory -------------------------------------------------
    def write(self):
        if not self.sc is None:
            self.sc.write()
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write()


    # --- Configuration (Pegasus Properties) ---------------------------------------
    def create_pegasus_properties(self):
        self.props = Properties()

        # props["pegasus.monitord.encoding"] = "json"                                                                    
        #self.properties["pegasus.integrity.checking"] = "none"
        return


    # --- Site Catalog -------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="condorpool"):
        self.sc = SiteCatalog()

        shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        local_storage_dir = os.path.join(self.wf_dir, "output")

        local = (Site("local")
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                            .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL)),
                        Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                            .add_file_servers(FileServer("file://" + local_storage_dir, Operation.ALL))
                    )
                )

        exec_site = (Site(exec_site_name)
                        .add_condor_profile(universe="vanilla")
                        .add_pegasus_profile(
                            style="condor",
                            data_configuration="nonsharedfs"
                        )
                    )

        self.sc.add_sites(local, exec_site)


    # --- Transformation Catalog (Executables and Containers) ----------------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()
        
        orcasound_container = Container("orcasound_container",
            container_type = Container.SINGULARITY,
            image="docker://papajim/orcasound-processing:latest",
            image_site="docker_hub"
        )

        # Add the orcasound processing
        orcasound_processing = Transformation("orcasound_processing", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/orcasound_processing.py"), is_stageable=True)
        
        self.tc.add_containers(orcasound_container)
        self.tc.add_transformations(orcasound_processing)

    
    # --- Fetch s3 catalog ---------------------------------------------------------
    def fetch_s3_catalog(self):
        print("Downloading S3 cache...")
        data = requests.get(self.s3_cache_xz_url)
        if data.status_code != 200:
            raise ConnectionError("Download for {} failed with error code: {}".format(self.s3_cache_xz_url, data.status_code))

        with open(self.s3_cache_xz, "wb") as f:
            f.write(data.content)

        print("Unpacking S3 cache...")
        with tarfile.open(self.s3_cache_xz) as f:
            f.extractall('.')

        os.remove(self.s3_cache_xz)
        print("S3 cache fetched successfully...")


    # --- Check s3 catalog for files -----------------------------------------------
    def check_s3_cache(self):
        s3_files = self.s3_cache[self.s3_cache["Sensor"].isin(self.sensors) & (self.s3_cache["Timestamp"] >= self.start_date) & (self.s3_cache["Timestamp"] <= self.end_date)]

        if s3_files.empty:
            print("No files found for sensors between {} and {}".format(self.start_date, self.end_date))
            exit()

        for sensor in self.sensors:
            if s3_files[s3_files["Sensor"] == sensor].empty:
                print("No files found for sensor {} between {} and {}".format(sensor, self.start_date, self.end_date))
        
        self.s3_files = s3_files


    # --- Read s3 catalog files ----------------------------------------------------
    def read_s3_cache(self):
        if not os.path.isfile(self.s3_cache_file):
            self.fetch_s3_catalog()
        
        print("Reading S3 cache...")
        self.s3_cache = pd.read_csv(self.s3_cache_file)
        self.check_s3_cache()

    
    # --- Replica Catalog ----------------------------------------------------------
    def create_replica_catalog(self):
        self.rc = ReplicaCatalog()

        self.read_s3_cache()
        if (self.s3_files is None):
            exit()
        
        # Drop m3u8 files
        # self.s3_files = self.s3_files[~self.s3_files["Filename"].str.endswith(".m3u8")]

        # Add s3 files as deep lfns
        for f in self.s3_files["Key"]:
            self.rc.add_replica("AmazonS3", f, "s3://george@amazon/{}/{}".format(self.s3_bucket, f))
    
    # --- Create Workflow ----------------------------------------------------------
    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)

        # Create a job for each Sensor and Timestamp
        for sensor in self.sensors:
            for ts in self.s3_files[self.s3_files["Sensor"] == sensor]["Timestamp"].unique():
                job_files = self.s3_files[(self.s3_files["Sensor"] == sensor) & (self.s3_files["Timestamp"] == ts)]
                input_files = job_files["Key"]
                output_files = []
                for f in job_files["Filename"]:
                    if f.endswith(".m3u8"):
                        continue
                    else:
                        output_files.append("png/{0}/{1}".format(sensor, f.replace(".ts", ".png")))
                
                processing_job = (Job("orcasound_processing")
                                    .add_args("{0}/hls/{1} -o png/{0}/{1}".format(sensor, ts))
                                    .add_inputs(*input_files, bypass_staging=True)
                                    .add_outputs(*output_files, stage_out=True, register_replica=True)
                                 )
                                
                # Share files to jobs
                self.wf.add_jobs(processing_job)


if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Diamond Workflow")

    parser.add_argument("-s", "--skip_sites_catalog", action="store_true", help="Skip site catalog creation")
    parser.add_argument("-e", "--execution_site_name", metavar="STR", type=str, default="condorpool", help="Execution site name (default: condorpool)")
    parser.add_argument("-o", "--output", metavar="STR", type=str, default="workflow.yml", help="Output file (default: workflow.yml)")
    parser.add_argument("-w", "--workers", metavar="STR", type=str, default=10, help="Number of workers (default: 10)")
    parser.add_argument("--sensors", metavar="STR", type=str, choices=["rpi_bush_point", "rpi_port_townsend", "rpi_orcasound_lab"], required=True, nargs="+", help="Sensor source [rpi_bush_point, rpi_port_townsend, rpi_orcasound_lab]")
    parser.add_argument("--start_date", metavar="STR", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), required=True, help="Start date (example: '2021-08-10')")
    parser.add_argument("--end_date", metavar="STR", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), default=None, help="End date (default: Start date + 1 day)")

    args = parser.parse_args()
    if not args.end_date:
        args.end_date = args.start_date + timedelta(days=1)
    
    workflow = OrcasoundWorkflow(sensors=args.sensors, start_date=args.start_date, end_date=args.end_date, dagfile=args.output)
    
    if not args.skip_sites_catalog:
        print("Creating execution sites...")
        workflow.create_sites_catalog(args.execution_site_name)

    print("Creating workflow properties...")
    workflow.create_pegasus_properties()
    
    print("Creating transformation catalog...")
    workflow.create_transformation_catalog(args.execution_site_name)

    print("Creating replica catalog...")
    workflow.create_replica_catalog()

    print("Creating orcasound workflow dag...")
    workflow.create_workflow()

    workflow.write()
