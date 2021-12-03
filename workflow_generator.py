#!/usr/bin/env python3
import os
import sys
import logging
import tarfile
import requests
import numpy as np
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
    def __init__(self, sensors, start_date, end_date, max_files, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.sensors = sensors
        self.max_files = max_files
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

        self.props["pegasus.transfer.threads"] = "16"
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
                        .add_directories(
                            Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                                .add_file_servers(FileServer("file://" + shared_scratch_dir, Operation.ALL))
                        )
                        .add_condor_profile(universe="vanilla")
                        .add_pegasus_profile(
                            style="condor",
                            data_configuration="nonsharedfs",
                            auxillary_local=True
                        )
                    )

        self.sc.add_sites(local, exec_site)


    # --- Transformation Catalog (Executables and Containers) ----------------------
    def create_transformation_catalog(self, exec_site_name="condorpool"):
        self.tc = TransformationCatalog()
        
        orcasound_container = Container("orcasound_container",
            container_type = Container.SINGULARITY,
            image="docker://papajim/orcasound-processing:latest",
            image_site="docker_hub",
            mounts=["{0}:{0}".format(os.path.join(self.wf_dir, "scratch"))]
        )
        
        orcasound_ml_container = Container("orcasound_ml_container",
            container_type = Container.SINGULARITY,
            image="docker://papajim/orcasound-ml-processing:latest",
            image_site="docker_hub",
            mounts=["{0}:{0}".format(os.path.join(self.wf_dir, "scratch"))]
        )

        # Add the orcasound processing
        convert2wav = Transformation("convert2wav", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/convert2wav.py"), is_stageable=True, container=orcasound_container)
        convert2spectrogram = Transformation("convert2spectrogram", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/convert2spectrogram.py"), is_stageable=True, container=orcasound_container)
        inference = Transformation("inference", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/inference.py"), is_stageable=True, container=orcasound_ml_container)
        merge = Transformation("merge", site=exec_site_name, pfn=os.path.join(self.wf_dir, "bin/merge.py"), is_stageable=True, container=orcasound_container)

        
        self.tc.add_containers(orcasound_container)
        self.tc.add_transformations(convert2wav, convert2spectrogram, inference)

    
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

        # Add inference dependencies
        self.rc.add_replica("local", "model.py", os.path.join(self.wf_dir, "bin/model.py"))
        self.rc.add_replica("local", "dataloader.py", os.path.join(self.wf_dir, "bin/dataloader.py"))
        self.rc.add_replica("local", "params.py", os.path.join(self.wf_dir, "bin/params.py"))
        self.rc.add_replica("local", "model.pkl", os.path.join(self.wf_dir, "input/model.pkl"))
     

    # --- Create Workflow ----------------------------------------------------------
    def create_workflow(self):
        self.wf = Workflow(self.wf_name, infer_dependencies=True)
        
        model_py = File("model.py")
        dataloader_py = File("dataloader.py")
        params_py = File("params.py")
        model_file = File("model.pkl")

        # Create a job for each Sensor and Timestamp
        predictions_files = []
        for sensor in self.sensors:
            predictions_sensor_files = []
            for ts in self.s3_files[self.s3_files["Sensor"] == sensor]["Timestamp"].unique():
                predictions_sensor_ts_files = []
                sensor_ts_files = self.s3_files[(self.s3_files["Sensor"] == sensor) & (self.s3_files["Timestamp"] == ts) & (self.s3_files["Filename"] != "live.m3u8")]
                sensor_ts_files_len = len(sensor_ts_files.index)
                # -2 if m3u8 in the list else -1
                sensor_ts_files = sensor_ts_files[sensor_ts_files["Filename"] != "live{}.ts".format(sensor_ts_files_len-1)]
                sensor_ts_files_len -= 1

                num_of_splits = -(-sensor_ts_files_len//self.max_files)
                
                counter = 1
                for job_files in np.array_split(sensor_ts_files, num_of_splits):
                    input_files = job_files["Key"]
                    wav_files = []
                    png_files = []
                    for f in job_files["Filename"]:
                        wav_files.append("wav/{0}/{1}/{2}".format(sensor, ts, f.replace(".ts", ".wav")))
                        png_files.append("png/{0}/{1}/{2}".format(sensor, ts, f.replace(".ts", ".png")))
                
                    convert2wav_job = (Job("convert2wav", _id="wav_{0}_{1}_{2}".format(sensor, ts, counter), node_label="wav_{0}_{1}_{2}".format(sensor, ts, counter))
                                        .add_args("-i {0}/hls/{1} -o wav/{0}/{1}".format(sensor, ts))
                                        .add_inputs(*input_files, bypass_staging=True)
                                        .add_outputs(*wav_files, stage_out=False, register_replica=False)
                                        .add_pegasus_profiles(label="{0}_{1}_{2}".format(sensor, ts, counter))
                                    )
                    
                    convert2spectrogram_job = (Job("convert2spectrogram", _id="png_{0}_{1}_{2}".format(sensor, ts, counter), node_label="spectrogram_{0}_{1}_{2}".format(sensor, ts, counter))
                                        .add_args("-i wav/{0}/{1} -o png/{0}/{1}".format(sensor, ts))
                                        .add_inputs(*wav_files)
                                        .add_outputs(*png_files, stage_out=True, register_replica=False)
                                        .add_pegasus_profiles(label="{0}_{1}_{2}".format(sensor, ts, counter))
                                    )

                    predictions = File("predictions_{0}_{1}_{2}.json".format(sensor, ts, counter))
                    predictions_sensor_ts_files.append(predictions)
                    inference_job = (Job("inference", _id="predict_{0}_{1}_{2}".format(sensor, ts, counter), node_label="inference_{0}_{1}_{2}".format(sensor, ts, counter))
                                        .add_args("-i wav/{0}/{1} -s {0} -t {1} -m {3} -o predictions_{0}_{1}_{2}.json".format(sensor, ts, counter, model_file.lfn))
                                        .add_inputs(model_file, model_py, dataloader_py, params_py, *wav_files)
                                        .add_outputs(predictions, stage_out=False, register_replica=False)
                                        .add_pegasus_profiles(label="{0}_{1}_{2}".format(sensor, ts, counter))
                                    )

                    # Increase counter
                    counter += 1

                    # Share files to jobs
                    self.wf.add_jobs(convert2wav_job, convert2spectrogram_job, inference_job)

                #merge predictions for sensor timestamps
                merged_predictions = File("predictions_{0}_{1}.json".format(sensor, ts))
                predictions_sensor_files.append(merged_predictions)
                merge_job_ts = (Job("merge", _id="merge_{0}_{1}".format(sensor, ts), node_label="merge_{0}_{1}".format(sensor, ts))
                                    .add_args("-i {0}".format(" ".join([x.lfn for x in predictions_sensor_ts_files])))
                                    .add_inputs(*predictions_sensor_ts_files)
                                    .add_outputs(merged_predictions, stage_out=True, register_replica=False)
                                    .add_pegasus_profiles(label="{0}_{1}".format(sensor, ts))
                                )

                self.wf.add_jobs(merge_job_ts)

            #merge predictions for sensor if more than 1 files
            if len(predictions_sensor_files) > 1:
                merged_predictions = File("predictions_{0}.json".format(sensor))
                predictions_files.append(merged_predictions)
                merge_job_sensor = (Job("merge", _id="merge_{0}".format(sensor, ts), node_label="merge_{0}".format(sensor, ts))
                                        .add_args("-i {0}".format(" ".join([x.lfn for x in predictions_sensor_files])))
                                        .add_inputs(*predictions_sensor_files)
                                        .add_outputs(merged_predictions, stage_out=True, register_replica=False)
                                        .add_pegasus_profiles(label="{0}".format(sensor))
                                    )

                self.wf.add_jobs(merge_job_sensor)

        #merge predictions for all sensors if more than 1 files
        if len(predictions_files) > 1:
            merged_predictions = File("predictions_all.json")
            merge_job_all = (Job("merge", _id="merge_all".format(sensor, ts), node_label="merge_all".format(sensor, ts))
                                    .add_args("-i {0}".format(" ".join([x.lfn for x in predictions_files])))
                                    .add_inputs(*predictions_files)
                                    .add_outputs(merged_predictions, stage_out=True, register_replica=False)
                            )

            self.wf.add_jobs(merge_job_all)


if __name__ == '__main__':
    parser = ArgumentParser(description="Pegasus Diamond Workflow")

    parser.add_argument("-s", "--skip-sites-catalog", action="store_true", help="Skip site catalog creation")
    parser.add_argument("-e", "--execution-site-name", metavar="STR", type=str, default="condorpool", help="Execution site name (default: condorpool)")
    parser.add_argument("-o", "--output", metavar="STR", type=str, default="workflow.yml", help="Output file (default: workflow.yml)")
    parser.add_argument("-m", "--max-files", metavar="INT", type=int, default=200, help="Max files per job (default: 200)")
    parser.add_argument("--sensors", metavar="STR", type=str, choices=["rpi_bush_point", "rpi_port_townsend", "rpi_orcasound_lab"], required=True, nargs="+", help="Sensor source [rpi_bush_point, rpi_port_townsend, rpi_orcasound_lab]")
    parser.add_argument("--start-date", metavar="STR", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), required=True, help="Start date (example: '2021-08-10')")
    parser.add_argument("--end-date", metavar="STR", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), default=None, help="End date (default: Start date + 1 day)")

    args = parser.parse_args()
    if not args.end_date:
        args.end_date = args.start_date + timedelta(days=1)
    
    workflow = OrcasoundWorkflow(sensors=args.sensors, start_date=args.start_date, end_date=args.end_date, max_files=args.max_files, dagfile=args.output)
    
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
