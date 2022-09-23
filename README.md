# conductor (DNAnexus Platform App)

DNAnexus app for automating end to end analysis of samples through apps and workflows.


## What are typical use cases for this app?
Automating analysis for given samples from a config file definition. This can either be as an app triggered at the end of [dx-streaming-upload](dx-streaming-upload-url) or run as a stand-alone app with the required inputs.


## What data are required for this app to run?

The app may be run in 2 ways:

- from a sentinel file uploaded by dx-streamiung-upload
- from a set of fastqs, if starting from fastqs other inputs are required:
  - a samplesheet (`-SAMPLESHEET`) or list of sample names (`-SAMPLE_NAMES`)
  - a `RunInfo.xml` file (`-RUN_INFO_XML`) to parse the run ID from or the run ID as a string (`-RUN_ID`)

In addition, both requires passing eggd_conductor config file (`.cfg`), this is the config file containing app ID for bcl2fastq, slack API token and DNAnexus auth token, aswell as the path to the assay json configs in DNAnexus.


## Config file design

The app is built to rely on 2 config files:

- a config file to store auth tokens and app variables
- an assay specific config file that specifies all aspects of calling the required workflows and apps for a given assay

### eggd_conductor config

This currently must contain the following:

- `ASSAY_CONFIG_PATH`: DNAnexus path to directory containing assay level json config files (format: `project:/path/to/configs`)
- `BCL2FASTQ_APP_ID=`: app ID of bcl2fastq for demultiplexing from dx-streaming-upload runs
- `AUTH_TOKEN`: DNAnexus API token
- `SLACK_TOKEN`: Slack API token, used for sending slack notifications
- `SLACK_LOG_CHANNEL`: Slack channel to send general start and success notifications to
- `SLACK_ALERT_CHANNEL`: Slack channel to send any alerts of fails to


### Low level / assay config file

The assay config file for the conductor app is designed to be written as a JSON file, with each workflow or apps defined as an executable. For each workflow/app, there are several required and optional keys to add, with a required structure. An example empty template and populated config file may be found [here](example/config_template.json) and [here](example/example_populated_config.json) respectively.

Config files are expected to be stored in a given directory in DNAnexus (`ASSAY_CONFIG_PATH` from  `conductor.cfg`), and the default behaviour if a config file is not specified at run time is the search this directory and use the highest version config file for each `assay_code` (detailed below).

As the config file is a JSON, several fields may be added to enhance readability that will not be parsed when running, such as the name, details and GitHub URL for each executable.

**Required keys in the top level of the config include**:

- `assay` (str): identifier of the assay the config is for (i.e. MYE, TSO500), will be used to name output directory, must be the same between versions for the same `assay_code`
- `assay_code` (str): code used to parse from samplename to determine if the config is for the given sample. At EGLH, the prefix EGG[0-9] is used in the samplename to indicate its assay (i.e. EGG2 -> myeloid sample). The assay codes from all found config files are used to determine which configs to use for which samples.
- `version` (str): version of config file using semantic versioning (i.e. `1.0.0`). At run time all configs for a given `assay_code` are checked, and the highest version for each used.
- `demultiplex` (boolean): if true, run bcl2fastq to generate fastqs
- `users` (dict): DNAnexus users to add to output project and access level to be granted, valid permission levels may be found [here]( project-permissions).
- `executables` (dict): each key should be the workflow or app id, with it's value being a dictionary (see below for example)

**Optional keys in top level of config include**:

- `sample_name_regex` (list): list of regex patterns to use for performing samplesheet validation on sample names with

Example top level of config:
```{
    "name": "Config for myeloid assay",
    "assay": "MYE",
    "assay_code": "EGG2",
    "version": "v1.0.0",
    "details": "Includes main Uranus workflow, multi-fastqc and uranus annotation workflow",
    "demultiplex": true,
    "users": {
        "org-emee_1": "CONTRIBUTE"
    },
    "sample_name_regex": [
        "[0-9]{7}-[A-Z0-9]*-[A-Z]{2,3}-[A-Za-z]*-MYE-[FMNU]-EGG2",
        "Oncospan-[A-za-z0-9-]*"
    ]
```

**Required keys per executable dictionary**:

- `name`: will be used to name output directory if using output variable naming (see below)
- `analysis`: the value should be written as `analysis_1`, where the number is the executable stage in the config (i.e for the first workflow app this would be `analysis_1`, for the second `analysis_2`...). This is used to link the outputs of one workflow / app to subsequent workflows / apps.
- `per_sample` (boolean): if to run the executable on each sample individually, or as one multi-sample job
- `process_fastqs` (boolean): if the executable requires fastqs passing
- `inputs` (dict): this forms the input dictionary passed to the call to dx api to trigger the running of the executable, more details may be found [here](dx-run-url). See below for structure and available inputs.
- `output_dirs` (dict): maps the app / workflow stages to directories in which to store output data. See below for structure and available inputs.

**Optional keys per executable dictionary**:

- `depends_on` (list): Where an executables input(s) are dependent on the output of a previous job(s), these should be defined as a list of strings. This relies on using the `analysis_X` key, where `X` is the number of the dependent executable to collect the output from
    - (e.g. `"output_dirs": ["analysis_1"]`, where the job is dependent on the first executable completing successfully before starting)
- `sample_name_delimeter` (str): string to split sample name on and pass to where `INPUT-SAMPLE-NAME` is used. Useful for passing as input where full sample name is not wanted (i.e. for displaying in a report)
- `details` (str): not parsed by the app but useful for adding verbosity to the config when reading by humans
- `url` (str): same as above for `details`, just acts as a reference to GitHub provenance of what is being used for analysis


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


### Structuring the inputs dictionary

The inputs dict may be given several inputs that act as placeholders to be parsed by the script at runtime. Each key : value pair should be given as the app/stage input as the key, and the placeholder as the value. The key MUST match the input given in the specified workflow /apps available inputs (i.e. in `dxapp.json` for apps, `stage-id.input` for workflows). These are all prefixed with `INPUT-` to be identifiable.

Currently, the available placeholder inputs include the following:

- `INPUT-R1`: indicates to pass 1 or more R1 fastq files as input
- `INPUT-R2`: indicates to pass 1 or more R2 fastq files as input
- `INPUT-R1-R2`: indicates to pass all R1 AND R2 fastq files as input
- `INPUT-SAMPLE-PREFIX`: if to pass string input of sample name prefix split on `'sample_name_delimeter'` if specified in config
- `INPUT-SAMPLE-NAME`: if to pass string input of sample name
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


## App Inputs - Defaults & Behaviour

The following describe default app input behaviour:


- `EGGD_CONDUCTOR_CONFIG`: config file for app containing required variables
- `ASSAY_CONFIG` (optional): assay specific config file, if not given will search in `ASSAY_CONFIG_PATH` from `EGGD_CONDUCTOR_CONFIG` for appropriate file
- `SENTINEL_FILE` (optional): sentinel file created by dx-streaming-upload to use for specifying run data for analysis
- `SAMPLESHEET` (optional): samplesheet used to parse sample names from, if not given this will be attempted to be located from the sentinel file properties first, then sentinel file run directory then the first upload tar file.
- `FASTQS` (optional): array of fastq files, to use if not providing a sentinel file
- `SAMPLE_NAMES` (optional): comma separated list of sample names, to use if not providing a samplesheet
- `DX_PROJECT` (optional):  Project in which to run and store output, if not specified will create a new project named as `002_<RUNID>_<ASSAY_CODE>` or `003_YYMMDD_<RUNID>_<ASSAY_CODE>` if `development=true`
- `RUN_ID` ( optional): ID of sequencing run used to name project, parsed from samplesheet if not specified
- `VALIDATE_SAMPLESHEET` (optional): Perform samplesheet validation and exit on invalid sheet
- `DEVELOPMENT` (optional): Name output project with 003 prefix and date instead of 002_{RUN_ID}_{ASSAY} format
- `TESTING` (optional): Terminates all jobs and clears output files after launching - for testing use only
- `BCL2FASTQ_JOB_ID` (optional):  use output fastqs of a previous bcl2fastq job instead of performing demultiplexing
-  `BCL2FASTQ_OUT` (optional): Path to store bcl2fastq output, if not given will default parent of sentinel file. Should be in the format project:path



## Dependencies

The following release `.tar.gz` are required to be included in `/resources/home/dnanexus/`:

- [samplesheet validator](samplesheet-validator-url): used for validating samplesheets before running with bcl2fastq


[dx-streaming-upload-url]: https://github.com/dnanexus-rnd/dx-streaming-upload
[dx-run-url]: http://autodoc.dnanexus.com/bindings/python/current/dxpy_apps.html?highlight=run#dxpy.bindings.dxapplet.DXExecutable.run
[hermes-url]: https://github.com/eastgenomics/hermes
[samplesheet-validator-url]: https://github.com/eastgenomics/validate_sample_sheet
[bcl2fastq-url]: https://github.com/eastgenomics/eggd_bcl2fastq

[project-permissions]: https://documentation.dnanexus.com/developer/api/data-containers/project-permissions-and-sharing