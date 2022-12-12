# Pegasus Orcasound Workflow

This workflow is based on an open-source software and hardware project that trains itself using the audio files generated via the hydrophone sensors deployed in three locations in the state of Washington (SanJuan Island, Point Bush, and Port Townsend) in order to study Orca whales in the Pacific Northwest region.
This workflow uses code and ideas available in the Orcasound GitHub actions workflow
and the Orca Hello real-time notification system.

- Orcasound GitHub Actions Workflow: https://github.com/orcasound/orca-action-workflow
- Orca Hello: https://github.com/orcasound/aifororcas-livesystem

# Dependencies:

Pegasus v5.0+ <br>
Docker & Singularity <br>
Python 3.0 - Pandas, Numpy, Pytorch <br>
Memory Requirement: 16 GB memory <br>
Amazon AWS Credentials: https://pegasus.isi.edu/documentation/manpages/pegasus-s3.html?highlight=credentials%20amazon <br>

# File Description and how to run?:


<b>workflow_generator.py</b> - Creates the replica catalog, transformation catalog, and site catalog. The workflow uses two Dockerfiles - orcasound_container (which runs on python), orcasound_ml_container (which runs on python and uses the ml libraries like pytorch, pandas, numpy, etc.)The audio files are stored in Amazon's S3 bucket.

```
<!-- Check if the condor is running and is free -->
condor_status 

<!-- Clone this repository -->
git clone https://github.com/pegasus-isi/orcasound-workflow.git - Clone this repository in your submit node

<!-- Run the python workflow using the following command -->
./workflow_generator.py --sensors rpi_bush_point --start-date 2021-08-10 -m 100

<!-- Run the generated workflow using Pegasus -->
pegasus-plan -s condorpool workflow.yml
pegasus-run  /home/poseidon/orcasound-workflow/poseidon/pegasus/orcasound/run0001
watch pegasus-status -l /home/poseidon/orcasound-workflow/poseidon/pegasus/orcasound/run0001

<!-- Generate the graphical representation of the workflow -->
pegasus-graphviz -s -f workflow.yml 
pegasus-graphviz -s -f -o workflow_graph.dot workflow.yml 
cat workflow_graph.dot 
dot -T png workflow_graph.dot
dot -T png workflow_graph.dot > workflow_graph.png

sftp poseidon@129.114.27.12
cd tests_workflow
get workflow_graph.png

.ssh % mv workflow_graph.png Desktop
```

# Available Generation Options

```
poseidon@submit:~/orcasound-workflow$ ./workflow_generator.py -h
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

# Example Usage With Options
```
./workflow_generator.py --sensors rpi_bush_point --start-date 2021-08-10 -m 100
```

## Workflow Containers

- Orcasound Container: https://hub.docker.com/r/papajim/orcasound-processing
- Orcasound ML Processing Container: https://hub.docker.com/r/papajim/orcasound-ml-processing

![Orcasound Workflow DAG](/images/orcasound-workflow.png)
