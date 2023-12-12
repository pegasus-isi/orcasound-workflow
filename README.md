# Pegasus Orcasound Workflow

This workflow is based on an open-source software and hardware project that trains itself using the audio files generated via the hydrophone sensors deployed in three locations in the state of Washington (SanJuan Island, Point Bush, and Port Townsend) in order to study Orca whales in the Pacific Northwest region.
This workflow uses code and ideas available in the Orcasound GitHub actions workflow
and the Orca Hello real-time notification system.

- Orcasound GitHub Actions Workflow: https://github.com/orcasound/orca-action-workflow
- Orca Hello: https://github.com/orcasound/aifororcas-livesystem


![Orcasound Workflow DAG](/images/orcasound-workflow.png)


## Dependencies:
- Pegasus v5.0+
- HTCondor v10.2+
- Python 3.6+, Numpy, Pandas, Requests
- Docker or Singularity
- Memory Requirement: 16 GB memory

## Accessing the Input Data:
The recording reside in an S3 bucket. To enable pegasus to access the S3 bucket and fetch these recordings during the workflow execution you need to specify your Amazon AWS credentials in the Pegasus credentials configuration file (~/.pegasus/credentials.conf). For more details follow the Pegasus manual section regarding AWS credentials (https://pegasus.isi.edu/documentation/manpages/pegasus-s3.html?highlight=credentials%20amazon).

## File Description:
- <b>workflow_generator.py:</b> - Creates the abstract workflow, the replica catalog, the transformation catalog, and the site catalog. The workflow uses two Dockerfiles - orcasound\_container (which runs on python), orcasound_ml_container (which runs on python and uses the ml libraries like pytorch, pandas, numpy, etc.)The audio files are stored in Amazon's S3 bucket.
- <b>Docker/Orca_Dockerfile:</b>
- <b>Docker/Orca_ML_Dockerfile:</b>
- <b>fetch_s3_catalog.py:</b>
- <b>fetch_s3_catalog_lossless.py:</b>


## Workflow Containers
- Orcasound Container: https://hub.docker.com/r/papajim/orcasound-processing
- Orcasound ML Processing Container: https://hub.docker.com/r/papajim/orcasound-ml-processing


## Available Workflow Generation Options
```
$ ./workflow_generator.py -h
usage: workflow_generator.py [-h] [-s] [-e STR] [-o STR] [-m INT] --sensors
                             STR [STR ...] --start-date STR [--end-date STR]

Pegasus Orcasound Workflow

optional arguments:
  -h, --help            show this help message and exit
  -s, --skip-sites-catalog
                        Skip site catalog creation
  -e STR, --execution-site-name STR
                        Execution site name (default: condorpool)
  -o STR, --output STR  Output file (default: workflow.yml)
  -m INT, --max-files INT
                        Max files per job (default: 200)
  --sensors STR [STR ...]
                        Sensor source [rpi_bush_point, rpi_port_townsend, rpi_orcasound_lab]
  --start-date STR      Start date (example: '2021-08-10')
  --end-date STR        End date (default: Start date + 1 day)
```


## How to Run the Workflow:
```
#Run the workflow generator to create an abstract workflow for a set of sensors and a set of dates for the input data
./workflow_generator.py --sensors rpi_bush_point --start-date 2021-08-10

#Plan and submit the generated workflow
pegasus-plan --submit -s condorpool -o local workflow.yml
```

