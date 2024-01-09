# eggd_conductor (DNAnexus Platform App)

DNAnexus app for automating end to end analysis of samples through apps and workflows.

---

## What are typical use cases for this app?
Automating analysis for given samples from a config file definition. This can either be as an app triggered at the end of [dx-streaming-upload](dx-streaming-upload-url) or run as a stand-alone app with the required inputs.

---

## App Inputs - Defaults & Behaviour

The following describe default app input behaviour:

**Required**
- `-iEGGD_CONDUCTOR_CONFIG` (`file`): config file for app containing required variables

and either:

- `-iupload_sentinel_record` (`record`): sentinel file created by dx-streaming-upload to use for specifying run data for analysis (*n.b. this is the only input that is lowercase as this is a fixed requirement from dx-streaming-upload*)

OR

- `-iFASTQS` (`array:file`): array of fastq files, to use if not providing a sentinel file


**Optional**
**Files**
- `-iSAMPLESHEET`: samplesheet used to parse sample names from, if not given this will be attempted to be located from the sentinel file properties first, then sentinel file run directory then the first upload tar file.
- `-iASSAY_CONFIG`: assay specific config file, if not given will search in `-iASSAY_CONFIG_PATH` from `-iEGGD_CONDUCTOR_CONFIG` for appropriate file
- `-iRUN_INFO_XML`: *Only required if starting from `-iFASTQS` input and not providing `-iRUN_ID`*. RunInfo.xml file for the run, used to parse RunID from for naming of DNAnexus project if `-iCREATE_PROJECT=true` and for adding to Slack notifications.



**Strings**
- `-iDEMULTIPLEX_JOB_ID`:  use output fastqs of a previous demultiplexing job instead of performing demultiplexing
- `-iDEMULTIPLEX_OUT`: path to store demultiplexing output, if not given will default parent of sentinel file. Should be in the format `project:path`
- `-iDX_PROJECT`: project ID in which to run and store output
- `-iRUN_ID`: ID of sequencing run used to name project, parsed from RunInfo.xml if not specified
- `-iSAMPLE_NAMES`: comma separated list of sample names, to use if not providing a samplesheet
- `-iJOB_REUSE`: JSON formatted string mapping analysis step -> job ID to reuse outputs from instead of running analysis (i.e. `'{"analysis_1": "job-xxx"}'`). This is currently only implemented for per-run analysis steps.
- `-iEXCLUDE_SAMPLES`: comma separated string of sample names to exclude from per sample analysis steps (*n.b. these must be as they are in the samplesheet*)


**Integers**
- `-iTESTING_SAMPLE_LIMIT`: no. of samples to launch per sample jobs for, useful when testing to not wait on launching all per sample jobs
- `-iMISMATCH_ALLOWANCE` (default: `1`): no. of samples allowed to be missing assay code and continue analysis using the assay code of the other samples on the run (i.e. allows for a control sample on the run not named specifically for the assay)

**Booleans**
- `-iCREATE_PROJECT` (default: `false`): controls if to create a downstream analysis project to launch analysis jobs in, default behaviour is to use same project as eggd_conductor is running in. If true, the app will create a new project (or use if already exists) named as `002_<RUNID>_<ASSAY_CODE>`.
- `-iTESTING`: terminates all jobs and clears output files after launching - for testing use only

---

## What data are required for this app to run?

The app may be run in 2 ways:

- from a sentinel file uploaded by [dx-streaming-upload](dx-streaming-upload-url)
- from a set of fastqs, if starting from fastqs other inputs are required:
  - a samplesheet (`-iSAMPLESHEET`) or list of sample names (`-SAMPLE_NAMES`)
  - a `RunInfo.xml` file (`-iRUN_INFO_XML`) to parse the run ID from **or** the run ID as a string (`-iRUN_ID`)

In addition, both require passing a config file for the app (`-iEGGD_CONDUCTOR_CONFIG`). This is the config file containing app ID / name for the demultiplexing app ([bcl2fastq][bcl2fastq-url] | [bclconvert][bclconvert-url]), slack API token and DNAnexus auth token, as well as the path to the assay json configs in DNAnexus.

---

## Config file design

The app is built to rely on 2 config files:

- an app config file to store auth tokens and app variables
- an assay specific config file that specifies all aspects of calling the required workflows and apps for a given assay

---

### eggd_conductor app config

This currently must contain the following:

- `ASSAY_CONFIG_PATH`: DNAnexus path to directory containing assay level json config files (format: `project:/path/to/configs`)
- `DEMULTIPLEX_APP_ID`: app ID of bcl2fastq or bclconvert for demultiplexing from dx-streaming-upload runs
- `AUTH_TOKEN`: DNAnexus API token
- `SLACK_TOKEN`: Slack API token, used for sending slack notifications
- `SLACK_LOG_CHANNEL`: Slack channel to send general start and success notifications to
- `SLACK_ALERT_CHANNEL`: Slack channel to send any alerts of fails to

n.b. The default behaviour of running the app with minimum inputs specified is to search the given `ASSAY_CONFIG_PATH` above for the highest available version of config files for each assay code, as defined under `version` and `assay_code` fields in the assay config (described below). For each assay code, the highest version will be used for analysing any samples with a matching assay code in the sample name, which may be overridden with the input `-iASSAY_CONFIG`.

---

### Assay config file

The assay config file for the conductor app is designed to be written as a JSON file, with each workflow or apps defined as an executable. For each workflow/app, there are several required and optional keys to add, with a required structure. An example empty template and populated config file may be found [here](example/config_template.json) and [here](example/example_populated_config.json) respectively.

In addition, a GitHub repository containing full production config files may be found [here](https://github.com/eastgenomics/eggd_conductor_configs).

Config files are expected to be stored in a given directory in DNAnexus (`ASSAY_CONFIG_PATH` from  `-iEGGD_CONDUCTOR_CONFIG`), and the default behaviour if a config file is not specified at run time is the search this directory and use the highest version config file for each `assay_code` (detailed below).

As the config file is a JSON, several fields may be added to enhance readability that will not be parsed when running, such as the name, details and GitHub URL for each executable.


**Required keys in the top level of the config include**:

- `assay` (str): identifier of the assay the config is for (i.e. MYE, TSO500), will be used to name output directory, must be the same between versions for the same `assay_code`
- `assay_code` (str): code(s) used to match against sample name to determine if the config is for the given sample. At runtime, all configs are pulled from `ASSAY_CONFIG_PATH` in DNAnexus, and the highest version for each code present is determined and kept to use for analysis. These assay codes are then used to match against sample names to determine which config file to use for analysis based on the presence of the code in the sample name. Multiple codes may be defined for one assay by seperating with a `|` (i.e. `'assay_code': 'LAB123|LAB456'`). This allows where a single config should be used for different codes, and handles having samples on a run with mixed codes but requiring the sample analysis.
- `version` (str): version of config file using semantic versioning (i.e. `1.0.0`). At run time all configs for a given `assay_code` are checked, and the highest version for each used.
- `demultiplex` (boolean): if true, run demultiplexing to generate fastqs
- `users` (dict): DNAnexus users to add to output project and access level to be granted, valid permission levels may be found [here]( project-permissions).
- `executables` (dict): each key should be the workflow or app id, with it's value being a dictionary (see below for example)

**Optional keys in top level of assay config include**:

- `changelog` (dict): optional recording of changes for each version of config file, useful for quickly identifying what has changed between versions
- `demultiplex_config` (dict): a set of config values for the demultiplexing job. This may contain the following keys:
  - `app_id` : app- ID of demultiplexing app to use, this will override the one in the app config if specified.
  - `app_name`: app name of demultiplexing app to use, this will override both the ID in the app config and `app_id` above if specified.
  - `additional_args` : additional command line arguments to pass into the demultiplexing app, this will ONLY work if either the [eggd_bcl2fastq][bcl2fastq-url] or [eggd_bclconvert][bclconvert-url] apps are being used as `additional_args` is a valid input for those apps that is then passed directly to bcl2fastq or bclconvert respectively.
  - `instance_type` : instance type to use, will override the default for the app if specified

Example top level of config:
```{
    "name": "Config for myeloid assay",
    "assay": "MYE",
    "assay_code": "EGG2",
    "version": "v1.0.0",
    "details": "Includes main Uranus workflow, multi-fastqc and uranus annotation workflow",
    "users": {
        "org-emee_1": "CONTRIBUTE"
    },
    "changelog" {
        "v1.0.0": "Initial working version"
    }
    "demultiplex": true,
    "demultiplex_config": {
        "app_name": "app-eggd_bclconvert",
        "additional_args": "--strict-mode true",
        "instance_type": "mem1_ssd1_v2_x36"
    }
```

n.b. the `instance_type` in the `demultiplex_config` may either be defined as a string or a mapping of flowcell IDs to instance type strings to allow for setting the instance type based off the flowcell ID.
```
# single instance
"demultiplex_config": {
    "instance_type": "mem1_ssd1_v2_x36"
}

# flowcell dependent instances
"demultiplex_config": {
    "instance_type": {
        "S1": "mem2_ssd1_v2_x16",
        "S2": "mem2_ssd1_v2_x48",
        "S4": "mem2_ssd1_v2_x96"
    }
}
```
See the below section **Dynamic instance types** for full explanation.


**Required keys per executable dictionary**:

- `name`: will be used to name output directory if using output variable naming (see below)
- `analysis`: the value must be written as `analysis_1`, where the number is the executable stage in the config (i.e for the first workflow / app this would be `analysis_1`, for the second `analysis_2`...). This determines the order of executables being launched and is used to link the outputs of one workflow / app to subsequent workflows / apps.
- `per_sample` (boolean): if to run the executable on each sample individually, or as one multi-sample job
- `process_fastqs` (boolean): if the executable requires fastqs passing
- `inputs` (dict): this forms the input dictionary passed to the call to dx api to trigger the running of the executable, more details may be found [here][dx-run-url]. See below for structure and available inputs.
- `output_dirs` (dict): maps the app / workflow stages to directories in which to store output data. See below for structure and available inputs.


**Optional keys per executable dictionary**:

- `details` (str): not parsed by the app but useful for adding verbosity to the config when reading by humans
- `url` (str): same as above for `details`, just acts as a reference to GitHub provenance of what is being used for analysis
- `depends_on` (list): Where an executables input(s) are dependent on the output of a previous job(s), these should be defined as a list of strings. This relies on using the `analysis_X` key, where `X` is the number of the dependent executable to collect the output from
    - (e.g. `"output_dirs": ["analysis_1"]`, where the job is dependent on the first executable completing successfully before starting)
- `sample_name_delimeter` (str): string to split sample name on and pass to where `INPUT-SAMPLE-NAME` is used. Useful for passing as input where full sample name is not wanted (i.e. for displaying in a report)
- `extra_args` (dict): mapping of [additional paramaters][dx-run-parameters] to pass to underlying API call for running dx analysis (i.e priority, cost_limit, instance_type) - see below for example formatting
- `hold` (boolean): controls whether to hold conductor until all jobs for the given executable complete before attempting to launch the next analysis steps. This may be used when downstream analysis may need to split out an array of output files from an upstream job, instead of taking the full array as input.
- `instance_types` (dict): mapping of flowcell identifiers to instance types to use for jobs, this allows for dynamically setting instances types based upon the flowcell used for sequencing. See the **Dynamic instance types** selection below for details.
- `inputs_filter` (dict): mapping of stage / app input field and list of pattern(s) to filter input by. This is used when providing the output of one app as input to another, but not all files want to be provided as input (i.e. taking all output bam files of analysis_X jobs, but only wanting to use the one from a control). This should be structured as such:
```
"inputs_filter": {
    "stage-G9Z2B8841bQY907z1ygq7K9x.bam": [
        "NA12878.*"
    ]
}
```


Example of per executable config:
```
    "executables": {
        "workflow-G4VpkG8433GZKf90KkXB4XZx": {
            "name": "uranus_main_workflow_GRCh38_v1.5.0_novaseq",
            "details": "Main Uranus workflow for alignment and variant calling",
            "url": "https://github.com/eastgenomics/eggd_uranus_main_workflow",
            "analysis": "analysis_1",
            "per_sample": true,
            "sample_name_delimeter": "_",
            "process_fastqs": true,
            "inputs": {
                "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs": "INPUT-R1",
                "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs": "INPUT-R2"
            },
            "output_dirs": {
                "stage-G0qpXy0433Gv75XbPJ3xj8jV": "/output/OUT-FOLDER/APP-NAME",
                "stage-G0qpY1Q433GpzBp958KJYfBK": "/output/OUT-FOLDER/APP-NAME",
                "stage-G21GYKj4q5J37F0B5ky018QG": "/output/OUT-FOLDER/APP-NAME",
                "stage-G02ZFz0433GpQB4j9Gvg0b81": "/output/OUT-FOLDER/APP-NAME",
                "stage-G0Y87ZQ433Gy6y7vBB74p30j": "/output/OUT-FOLDER/APP-NAME",
                "stage-G0KbB6Q433GyV6vbJZKVYV96": "/output/OUT-FOLDER/APP-NAME",
                "stage-Fy0549Q41zg02Kjg05x69yvK": "/output/OUT-FOLDER/APP-NAME",
                "stage-Fv6jY9Q4KB7yKfKx8Fq8b7zG": "/output/OUT-FOLDER/APP-NAME",
                "stage-Fy4j4K041zgF5Z8y0x9KjV55": "/output/OUT-FOLDER/APP-NAME",
                "stage-G02ZG6Q433GV76v29b6Gggjp": "/output/OUT-FOLDER/APP-NAME"
            }
        },
        "workflow-Jh6253Gg172u6253Kk82hFx": {
            ...
```

Example use of `extra_args` key to set priority to high and override default instance type for an **app**:
```
"extra_args": {
    "systemRequirements": {
        "*": {"instanceType": "mem1_ssd1_v2_x8"}
    },
    "priority": "high"
}
```

Use of `extra_args` for overriding default instance types for a **workflow** requires specifying the `stageSystemRequirements` key and stage ID key, then a run specification mapping as done for apps:
```
"extra_args": {
    "stageSystemRequirements": {
        "stage-G0qpXy0433Gv75XbPJ3xj8jV": {
            "*": {"instanceType": "mem1_ssd1_v2_x2"}
        }
    }
}
```

---

### Dynamic instance types
Different instance types for different types of flowcells may be defined for each executable, allowing for instances to be dynamically selected based upon the flowcell used for sequencing of the run being processed. This allows for setting optimal instances for all apps where the assay may be run on S1, S2, S4 flowcells etc and have differing compute requirements (i.e needing more storage for larger flowcells).
This should be defined in the assay config file for each executable, with the required instance types given for each flowcell.

Flowcell identifiers may be given either as patterns such as `xxxxxDRxx` (documented [here](https://knowledge.illumina.com/instrumentation/general/instrumentation-general-reference_material-list/000005589)), or as NovaSeq S1, S2 or S4 IDs. In addition, default instances may be given using a `"*"` as the key, which will be used if none of the given identifiers match the flowcell ID. Matching of these identifiers is done against the last field of the run ID, which for Illumina sequencers is the flowcell ID.

Examples of instance type setting for a single **app** using 'S' identifiers:
```
"instance_types": {
    "S1": "mem1_ssd1_v2_x2"
    "S2": "mem1_ssd2_v2_x4"
    "S4": "mem1_ssd2_v2_x8
}
```

Examples of instance type setting for a **workflow** using Illumina flowcell ID patterns:
```
"instance_types: {
    "xxxxxDRxx": {
        "stage-xxx": "mem1_ssd1_v2_x2"
        "stage-yyy": "mem2_ssd1_v2_x4"
        "stage-zzz": "mem1_ssd1_v2_x8"
    },
    "xxxxxDMxx": {
        "stage-xxx": "mem1_ssd1_v2_x4"
        "stage-yyy": "mem2_ssd1_v2_x8"
        "stage-zzz": "mem1_ssd1_v2_x16"
    },
    "xxxxxDSxx": {
        "stage-xxx": "mem2_ssd1_v2_x4"
        "stage-yyy": "mem2_ssd1_v2_x8"
        "stage-zzz": "mem2_ssd2_v2_x32"
    }
}
```

---

### Structuring the inputs dictionary

The inputs dict may be given several inputs that act as placeholders to be parsed by the script at runtime. Each key : value pair should be given as the app/stage input as the key, and the placeholder as the value. The key MUST match the input given in the specified workflow /apps available inputs (i.e. in `dxapp.json` for apps, `stage-id.input` for workflows). These are all prefixed with `INPUT-` to be identifiable.

Currently, the available placeholder inputs include the following:

- `INPUT-R1`: indicates to pass 1 or more R1 fastq files as input
- `INPUT-R2`: indicates to pass 1 or more R2 fastq files as input
- `INPUT-R1-R2`: indicates to pass all R1 AND R2 fastq files as input
- `INPUT-UPLOAD_TARS`: provide upload tars associated to sentinel record as an input
- `INPUT-SAMPLE-PREFIX`: if to pass string input of sample name prefix split on `'sample_name_delimeter'` if specified in config
- `INPUT-SAMPLE-NAME`: if to pass string input of sample name
- `INPUT-SAMPLESHEET`: passes the samplesheet associated to the sentinel file (or provided with `-iSAMPLESHEET`) as an input, must be structured in the `inputs` section of the config as: `{"$dnanexus_link": "INPUT-SAMPLESHEET"}`
- `INPUT-dx_project_id`: pass the project id used for analysis
- `INPUT-dx_project_name`: pass the project name used for analysis
- `INPUT-analysis_X-out_dir`: pass the output directory of analysis `X` as input, where `X` is the number of the analysis defined as above (used when an app takes a path to a directory as input)

Inputs dependent on the output of a previous job should be defined as shown below. This relies on using the `analysis_X` key, where `X` is the number of the executable to collect the output from.

n.b. where any inputs are linked to previous job outputs, the `depends_on` key should be given in the executable keys, with the value as a list of `analysis_X` to hold for the completion of that job.

- For workflows:

    ```
    "stage-G0QQ8jj433Gxyx2K8xfPyV7B.input_vcf": {
                        "$dnanexus_link": {
                            "analysis": "analysis_1",
                            "stage": "stage-G0Y87ZQ433Gy6y7vBB74p30j",
                            "field": "out"
                        }
                    },
     ```

- For apps / applets:

    ```
    "applet-G48VfX8433GypYP8418pPfq7.panel_bed": {
                        "$dnanexus_link": {
                            "analysis": "analysis_2",
                            "stage": "applet-G49452Q433GzX0jy3Gg60vz6",
                            "field": "bed_file"
                        }
                    },
    ```

For hardcoded file inputs to be provided to apps / workflows, these should be formatted as `$dnanexus_link` dictionaries:

```
"multiqc_docker": {
    "$dnanexus_link": "file-GF3PxgQ433Gqv1Q029Gjzjfv"
},
"multiqc_config_file": {
    "$dnanexus_link": "file-GF3Py30433GvZGb99kBVjZk1"
}
```

---

### Structuring the output_dirs dictionary

This defines the output directory structure for the executables outputs. For workflows, each stage should have the `stage-id: /path_to_output/` defined, otherwise the output will all go to the root of the project. These may either be hardcoded strings, or optionally use either or both of the following 4 placefolders to subsitute:

- `OUT-FOLDER`: will be in the format of `/output/{assay_name}_{timestamp}/`
- `APP-NAME`: will use the name for the given app id from a `dx describe` call
- `WORKFLOW-NAME`: will use the name for the given workflow id from a `dx describe` call
- `STAGE-NAME`: will use the name for the stage of the given workflow id from a `dx describe` call

- For workflows:

    ```
    "output_dirs": {
        "stage-G0QQ8jj433Gxyx2K8xfPyV7B": "/OUT-FOLDER/STAGE-NAME",
        "stage-G0QQ8q8433Gb8YFG4q7847Px": "/OUT-FOLDER/STAGE-NAME"
    }
    ```

- For apps / applets:

    ```
    "output_dirs": {
        "applet-Fz93FfQ433Gvf6pKFZYbXZQf": "/OUT-FOLDER/APP-NAME"
    }

    ```

---

## Demultiplexing

Demultiplexing may optionally be run if uploading non-demultiplexed data via [dx-streaming-upload][dx-streaming-upload-url], this is controlled by setting `demultiplex: true` in the top level of the assay config. As described above, this will either use the demultiplexing app specified in the eggd_conductor app config, or one specified in the assay config with `demultiplex_config`. This will trigger the demultiplexing app to launch in the same project as the current eggd_conudctor job unless `-iDEMULTIPLEX_OUT` is specified in a different project, which will trigger the job to run in the same project.

Once demultiplexing has completed, certain QC files will be copied that may be used by multiQC for including in the QC report. These will be copied into a directory in the root of the analysis project named `/demultiplex_multiqc_files`, this includes the following files:
- **bcl2fastq**
  - Stats.json
- **bclconvert**
  - RunInfo.xml
  - Demultiplex_Stats.csv
  - Quality_Metrics.csv
  - Adapter_Metrics.csv
  - Top_Unknown_Barcodes.csv

---

## Jira Integration

At run time, a Jira service desk may be queried for sequencing run tickets used to track progress of runs, and links to the analysis jobs automatically added as an internal comment. This requires the following variables adding to `eggd_conductor.config` file:

- `JIRA_EMAIL` : Jira email address of user account to use for connecting to Jira
- `JIRA_TOKEN` : Jira auth token of above user account
- `JIRA_QUEUE_URL` : Jira API endpoint to query for tickets, this will be in the format: `https://{domain}.atlassian.net/rest/servicedeskapi/servicedesk/{desk_number}/queue/{queue_number}` (e.g. https://cuhbioinformatics.atlassian.net/rest/servicedeskapi/servicedesk/1/queue/12)
- `JIRA_ISSUE_URL` : Jira API endpoint for posting comments to, this will be in the format: `https://{domain}.atlassian.net/rest/api/3/issue` (e.g. https://cuhbioinformatics.atlassian.net/rest/api/3/issue)

If none are given in the config it is assumed that Jira is not to be queried and the app will continue without.
If one or more are missing / invalid, or any error occurs connecting and / or querying Jira, a Slack alert will be sent with the error. This alert is non-blocking and analysis will still continue without continuing attempting to connect to Jira.

Example comment added at begining of processing to link to eggd_conductor job:
```
This run was processed automatically by eggd_conductor: http://platform.dnanexus.com/projects/GB3jx784Bv40j3Zx4P5vvbzQ/monitor/job/GJ3ykQ84Bv42ZP8J5zjz6qBb
```

Example comment added at end of successfully launching all jobs with link to analysis project:
```
All jobs sucessfully launched by eggd_conductor.
Analysis project: http://platform.dnanexus.com/projects/GB3jx784Bv40j3Zx4P5vvbzQ/monitor/
```

---

## Slack Integration

Notifications are sent to both the `SLACK_LOG_CHANNEL` and `SLACK_ALERT_CHANNEL` as set in the eggd_conductor app config. On starting, a notification is sent to the log channel to notify of automated analysis beginning, with a link to the job:

![image](https://user-images.githubusercontent.com/45037268/205304799-d1969b95-69f9-4d92-8a5a-e73d27b05b48.png)

Once all jobs have completed launching, another notification will be sent to the log channel with a link to the downstream analysis:

![image](https://user-images.githubusercontent.com/45037268/205304962-e94b1f74-555d-499e-8231-46d98fb850a9.png)

Any errors will be sent to the alerts channel, this may both be where an error is caught and handled, and also if an unexpected error occurs and the traceback is parsed into the notification:

![image](https://github.com/eastgenomics/eggd_conductor/assets/45037268/75baae97-2e50-4076-b8f2-9457e0e88541)
> Example of missing Jira ticket alert

![image](https://user-images.githubusercontent.com/45037268/205305399-8d21e471-f691-4f80-8389-9d144e423794.png)
> Example of error occuring from checks during launching jobs

![image](https://user-images.githubusercontent.com/45037268/205305431-8d89e519-1794-4046-9381-9d80eee90411.png)
> Example of unhandled error occurring and traceback being parsed in notification

---

## Supressing Automated Analysis

If the app ID is set in the playbook YAML config for dx-streaming-upload, it will automatically be run on completion of a sequencing run upload to DNAnexus. This behaviour may be suppressed for a given run by tagging of the sentinel record file for the run with `suppress-automation` during the time taken to upload the run. This may be desired when a run is known to need processing in a different manner (i.e. for validation of changes) or if a different assay config file to those present at `ASSAY_CONFIG_PATH` from the app config is to be used.

On starting, the app will check the provided sentinel record for the presence of this tag, if present an alert will be sent to the `SLACK_ALERT_CHANNEL` defined in the app config and the app will exit with a 0 status code. To then run analysis, the tag should be removed and the job relaunched, with any changes to inputs or arguments configured as needed.

- Example tagged sentinel record:

![image](https://user-images.githubusercontent.com/45037268/223139496-9749c7e1-6a35-4560-a919-22f3a3c1f630.png)

- Example Slack notification:

![image](https://user-images.githubusercontent.com/45037268/223140050-afb80a91-d76c-409a-8ef8-6003cb9a6d05.png)

---

## Monitoring

A [separate package][eggd_conductor_monitor] is available for monitoring and notifying of the state of analysis jobs launched via eggd_conductor. This parses the job IDs launched by each eggd_conductor job, and checks if all are complete, or if any have failed, and send an appropriate notification to given Slack channel(s).

---

[dx-streaming-upload-url]: https://github.com/dnanexus-rnd/dx-streaming-upload
[dx-run-url]: http://autodoc.dnanexus.com/bindings/python/current/dxpy_apps.html?highlight=run#dxpy.bindings.dxapplet.DXExecutable.run
[hermes-url]: https://github.com/eastgenomics/hermes
[bcl2fastq-url]: https://github.com/eastgenomics/eggd_bcl2fastq
[bclconvert-url]: https://github.com/eastgenomics/eggd_bclconvert

[project-permissions]: https://documentation.dnanexus.com/developer/api/data-containers/project-permissions-and-sharing
[dx-run-parameters]: http://autodoc.dnanexus.com/bindings/python/current/dxpy_apps.html?highlight=run#dxpy.bindings.dxapplet.DXExecutable.run
[eggd_conductor_monitor]: https://github.com/eastgenomics/eggd_conductor_monitor
