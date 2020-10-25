# Dr. PySpark or: How I Learned to Stop Worrying and Love the data-pipeline testing

_...or the title should be PySpark data-pipeline testing and CICD?!_


We're in the crusade of migrating our data from our various sources and repositories to the [cloud](https://xkcd.com/908/). One major challenge in 
the `pipeline-design` portion is of the tightly-coupled dependancy between the data and the codebase, which make it difficult for the developer to
test the code reliably and hindering the opportunity of a test-driven development and use continious integration with confidence. To mitigate the 
above scenario, it needs a flexible setup where the developer doesn’t end up spending a lot of effort in creating/maintaing a local test environment 
rather focusing on the application development.

So the focus should be about the second aspect, providing reusable self-contained data pipelines with CICD.

Let's talk about a structured framework where the heavy-lifting of spark environments, Schemas and tables configuration will be made available 
so that a dev can focus soley on application development with a little setup.

Alright, Talk is cheaper.Show me some code. Anyone?!

This blog is very detailed and meant to be followed [along with the code](https://github.com/soyelherein/pyspark-cicd-project)

For demo, let's have a minial setup of a spark job as below with five salient sections in the pipeline creation

	- spark session
	- static configuration variables  
	- Extract
	- Transform
	- Load

The pipeline structure is the constituents of five separate units _spark session, static configuration variables, Extract, Transform, Load._

![](/Users/thv/Documents/untitledxx.png)

<script src="https://gist.github.com/Thuruv/0c08d0eb52272f3686fb50407330d255.js"></script>




.. and the folder structure of the demo project (each part will be explained in the later section) as below
<script src="https://gist.github.com/soyelherein/4846e622ee40adcd2b2680aac5fbb986.js"></script>


### Exploring the Spark Environment

As it become tedious and later seems impractical to test and debug spark-jobs by sending them to a cluster (_spark-submit_) and becoming Shelock Holmes for investigating for clues in stack-traces on what could have went wrong.

	“How often have I said to you that when you have eliminated the impossible,
	 whatever remains, however improbable, must be the truth?”
		- Sherlock Holmes

To avoid the lifeless scenarios we might encounter, we can create a isolated environment (say thanks to _pipenv_) to initiate a _Pyspark_ session whereas 
- All development and production dependencies are described in the Pipfile
- the *_pipenv_* helps helps us managing project dependencies and Python environments (i.e. virtual environments)
- convenient with dependancies management on adhoc basis just with `pip install pipenv --dev`

### (1) dependencies.job_submitter
Since a data application can have numerous upstream and downstream pipelines, It makes sense to take out the spark environment management and other common tasks into a shared entry point. So, an application can focus only on the business logic. Looking closely, it parses the static configurations from JSON files and pass any dynamic argument to the job as a `dict` whereas the `run` method serves as the entry-point of the application in hand. With this submitter module, the command is changed like below ([Impletented demo](https://github.com/soyelherein/pyspark-cicd-project)),

	$SPARK_HOME/bin/spark-submit \
	 --py-files dependencies/job_submitter.py, jobs/pipeline_wo_modules.py \
	dependencies/job_submitter.py --job pipeline_wo_modules

### Exploring the Application


#### (2) Jobs
The IO operations are being handled by the designed ETL functions and since the business logics are being focussed by breaking down in to different `jobs.py` files.  Transform functions are designed that takes `DataFrame` as input returns `DataFrame` as output which makes it fitting to make use of it over the testcases with the local data to be a side effect free environment. 

	Extract — Reads the incremental/historical data from the table/flatfile a `DataFrame`
	Transform — Transform the data / Calculates the metrics and return a final `DataFrame`
	Load — Writes the data into the final designated output path
	Run — Does the integration job between ETL process. As exposed to the job submitter module,  it accepts the spark session, job configurations, and a logger object to execute the pipeline.


Let's dive deeper into the core part of our discussion :: Testing 

### (3) Testbed

#### (4) `conftest.py`

The static configurations and place them in a JSON file (configs/config.json) so that it can be overwritten as per the test config. We have used `pytest` style tests for our pipeline along with leveraging a few features (i.e. mock, patch) form `unittest`. This file does the heavy lifting of setting up the jobs for tests i.e providing test sparkSession and mocks creating the tables and DataFrames locally from the CSV files. The mapping is defined in the `testbed.json` file


<script src="https://gist.github.com/soyelherein/2ada1d422625dbed3f6da22330dccded.js"></script>



The dataframe and the underlying table details are tagged under `data` key and any dynamic parameters (a.k.a `job-args`) can be made available as job arguments which will override the `config` keys. A custom `setup_testbed` helper object, responsible for producing the DataFrame and tables once the test_bed.json file is configured and the file_formats is configurble as per the needs. Inorder to make the mocks more comprehensive, we encourage to to use the generic methods like `read.load` and `write`, instead of `read.csv` or `read.orc`

**test_pipeline** : We have created a session-level pytest fixture containing all the hard works done in the conftest in an object. As you see in the later section we will perform the entire testing using it’s member attributes.

<script src="https://gist.github.com/soyelherein/c03d865f8a16e9999e1526e70543aca7.js"></script>

<script src="https://gist.github.com/soyelherein/382b51acc81981bbfa91b7fde6872911.js"></script>



We have already tested individual methods we can make use of patching to do the integration test by patching the outcomes of different functions and avoiding side-effects of writing into the disk and the I/O operations are already been separated out we can introspect the calling behavior of extract and load using mocks. These mocks are setup in the conftest file.


<script src="https://gist.github.com/soyelherein/bdcb5721b5ea7d0274d2be9c903d2672.js"></script>


These tests can be run from IDE or by simply running `pytest` command.

![](https://miro.medium.com/max/1400/1*kv4Tt1RM3gz6pRwq1aCczg.png)


In a complex production scenario, related pipeline methods can be connected in terms of inputs and expected outputs which is immutable. A fair understanding of application and segregation of different subject area can provide a valuable regression like confidence for CICD integration.




### (5) CICD : Here comes the Sun


**Dockerfile**  — Contains the dockerized container with the virtual environment setup for the Jenkins agent.
**Makefile** — This Makefile utility zips all the code, dependencies, and config in the packages.zip file so that Jenkins can create the artifact, and CD process can upload it into a repository. The final code can be submitted as below

	$SPARK_HOME/bin/spark-submit \bu
	--py-files packages.zip \
	--files configs/config.json \
	dependencies/job_submitter.py --job pipeline --conf-file configs/config.json

**Jenkinsfile** — It defines the CICD process. where the Jenkins agent runs the docker container defined in the Dockerfile. in the prepare step followed by running the test. Once the test is successful in the prepare artifact step it uses the makefile to create a zipped artifact. The final step is to publish the artifact which is the deployment step.

All you need to have a Jenkins setup where you define a pipeline project and point to the Jenkins file.
