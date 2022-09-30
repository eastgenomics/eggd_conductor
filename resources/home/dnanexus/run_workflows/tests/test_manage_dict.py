from argparse import Namespace
from copy import deepcopy
import json
import os
import pytest
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.manage_dict import PPRINT, ManageDict
from tests import TEST_DATA_DIR


class TestSearchDict():
    """
    Tests for ManageDict.search() that searches a given dictionary for a
    given pattern against either keys or values, and can return the
    keys or values
    """
    with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
        full_config = json.load(fh)

    def test_search_key_return_key_level1(self) -> None:
        """
        Test ManageDict().search() against first level, checking
        and returning keys
        """
        output = ManageDict().search(
            identifier='level1',
            input_dict=self.full_config,
            check_key=True,
            return_key=True
        )

        correct_output = ['A_level1', 'B_level1', 'C_level1']

        assert sorted(output) == correct_output, (
            'Wrong keys returned checking keys with identifier "level1"'
        )

    def test_search_key_return_value_level1(self) -> None:
        """
        Test ManageDict().search() against first level, checking
        keys and returning values
        """
        output = ManageDict().search(
            identifier='level1',
            input_dict=self.full_config,
            check_key=True,
            return_key=False
        )

        correct_output = [
            'A_level3_array_value1', 'A_level3_array_value2',
            'A_level3_array_value3', 'A_level3_array_value4',
            'A_level3_value1', 'B_level3_value1', 'B_level3_value2',
            'B_level3_value3', 'C_array1_value1', 'C_array1_value2',
            'C_array1_value3'
        ]

        assert sorted(output) == correct_output, (
            'Wrong values returned checking keys with identifier "level1"'
        )

    def test_search_key_return_array_values(self) -> None:
        """
        Test ManageDict().search() against for key where values are an array
        """
        output = ManageDict().search(
            identifier='A_level3_key2',
            input_dict=self.full_config,
            check_key=True,
            return_key=False
        )

        correct_output = [
            'A_level3_array_value1', 'A_level3_array_value2',
            'A_level3_array_value3', 'A_level3_array_value4'
        ]

        assert sorted(output) == correct_output, (
            'Wrong values returned checking array of values'
        )

    def test_search_dict_array(self) -> None:
        """
        Test ManageDict().search() against for key where values are an array
        of dicts and return each value
        """
        output = ManageDict().search(
            identifier='C_array1',
            input_dict=self.full_config,
            check_key=True,
            return_key=False
        )

        correct_output = [
            'C_array1_value1', 'C_array1_value2', 'C_array1_value3'
        ]

        assert sorted(output) == correct_output, (
            'Wrong values returned checking array of dict values'
        )


class TestReplaceDict():
    """
    Tests for ManageDict.replace() that searches a dictionaries keys or values
    for a pattern, and replaces it with another given pattern
    """
    with open(os.path.join(TEST_DATA_DIR, 'test_low_level_config.json')) as fh:
        full_config = json.load(fh)

    def test_replace_level1_keys(self):
        """
        Test replacing the first level of keys in the dictionary
        """
        output = ManageDict().replace(
            input_dict=self.full_config,
            to_replace='level1',
            replacement='test',
            search_key=True,
            replace_key=True
        )

        # replacing all level1 keys with same so should only be one key
        assert list(output.keys()) == ['test'], (
            "Replacing level1 keys not correct"
        )

    def test_replace_all_value1(self):
        """
        Test searching replacing all values containing 'value1'
        """
        output = ManageDict().replace(
            input_dict=self.full_config,
            to_replace='value1',
            replacement='test',
            search_key=False,
            replace_key=False
        )

        correct_output = [
            {
                'A_level2': {
                    'A_level3_key1': 'test',
                    'A_level3_key2': [
                        'test',
                        'A_level3_array_value2',
                        'A_level3_array_value3',
                        'A_level3_array_value4'
                    ]
                }
            }, {
                'B_level2': {
                'B_level3_key1': 'test',
                'B_level3_key2': 'B_level3_value2',
                'B_level3_key3': 'B_level3_value3'
            }}, {'C_level2': [
                {'C_array1': 'test'},
                {'C_array1': 'C_array1_value2'},
                {'C_array1': 'C_array1_value3'}
            ]}
        ]

        assert list(output.values()) == correct_output, (
            "Searching and replacing 'value1' output incorrect"
        )

    def test_replace_value_from_key(self):
        """
        Test replacing all values from keys matching 'B_level3'
        """
        output = ManageDict().replace(
            input_dict=self.full_config,
            to_replace='B_level3',
            replacement='test',
            search_key=True,
            replace_key=False
        )

        correct_output = {
            'A_level1': {
                'A_level2': {
                    'A_level3_key1': 'A_level3_value1',
                    'A_level3_key2': [
                        'A_level3_array_value1',
                        'A_level3_array_value2',
                        'A_level3_array_value3',
                        'A_level3_array_value4'
                    ]
                }
            },
            'B_level1': {
                'B_level2': {
                    'B_level3_key1': 'test',
                    'B_level3_key2': 'test',
                    'B_level3_key3': 'test'}},
            'C_level1': {
                'C_level2': [
                    {'C_array1': 'C_array1_value1'},
                    {'C_array1': 'C_array1_value2'},
                    {'C_array1': 'C_array1_value3'}
                ]
            }
        }

        assert output == correct_output, (
            'Searching keys and replacing values returned wrong output'
        )


class TestAddFastqs():
    """
    Tests for adding fastq file IDs to input dict
    """
    fastq_details = [
        ('file-GGJY9604p3zBzjz5Fp66KF0Y',
        '2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2_S30_L002_R1_001.fastq.gz'),
        ('file-GGJY9684p3zG6fvf1vqvbqzx',
        '2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2_S30_L002_R2_001.fastq.gz'),
        ('file-GGJY96Q4p3z3233Q8v39Fzg2',
        '2207713-22222Z0074-1-BM-MPD-MYE-M-EGG2_S31_L002_R1_001.fastq.gz'),
        ('file-GGJY9704p3z250PB1yvZj7Y9',
        '2207713-22222Z0074-1-BM-MPD-MYE-M-EGG2_S31_L002_R2_001.fastq.gz'),
        ('file-GGJY9704p3z9P41f80bfQ623',
        '2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L002_R1_001.fastq.gz'),
        ('file-GGJY9784p3z78j8F1qkp4GZ4',
        '2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L002_R2_001.fastq.gz'),
        ('file-GGJY97j4p3z250PB1yvZj7YF',
        'Oncospan-158-2-AA1-BBB-MYE-U-EGG2_S33_L002_R1_001.fastq.gz'),
        ('file-GGJY9804p3z1X9YZJ4xf5v13',
        'Oncospan-158-2-AA1-BBB-MYE-U-EGG2_S33_L002_R2_001.fastq.gz')
    ]

    # minimal test section of config with executables requiring fastqs
    test_input_dict = {
        "workflow-GB6J7qQ433Gkf0ZYGbKfF0x6": {
            "analysis": "analysis_1",
            "process_fastqs": True,
            "inputs": {
                "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs": "INPUT-R1",
                "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs": "INPUT-R2"
            }
        },
        "applet-FvyXygj433GbKPPY0QY8ZKQG": {
            "analysis": "analysis_2",
            "process_fastqs": True,
            "inputs": {
                "fastqs": "INPUT-R1-R2"
            },
            "output_dirs": {
                "applet-FvyXygj433GbKPPY0QY8ZKQG": "/OUT-FOLDER/APP-NAME"
            }
        }
    }

    def test_adding_all_r1(self):
        """
        Test adding R1 fastqs from all samples as input where INPUT-R1 given
        """
        output = ManageDict().add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict['workflow-GB6J7qQ433Gkf0ZYGbKfF0x6']["inputs"]
            ),
            fastq_details=self.fastq_details
        )
        output_R1_fastqs = output['stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs']

        correct_R1_fastqs = [
            {'$dnanexus_link': 'file-GGJY9604p3zBzjz5Fp66KF0Y'},
            {'$dnanexus_link': 'file-GGJY96Q4p3z3233Q8v39Fzg2'},
            {'$dnanexus_link': 'file-GGJY9704p3z9P41f80bfQ623'},
            {'$dnanexus_link': 'file-GGJY97j4p3z250PB1yvZj7YF'}
        ]

        assert output_R1_fastqs == correct_R1_fastqs, (
            "R1 fastqs not correctly added"
        )

    def test_adding_all_r2(self):
        """
        Test adding R2 fastqs from all samples as input where INPUT-R1 given
        """
        output = ManageDict().add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict['workflow-GB6J7qQ433Gkf0ZYGbKfF0x6']["inputs"]
            ),
            fastq_details=self.fastq_details
        )
        output_R1_fastqs = output['stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs']

        correct_R2_fastqs = [
            {'$dnanexus_link': 'file-GGJY9684p3zG6fvf1vqvbqzx'},
            {'$dnanexus_link': 'file-GGJY9704p3z250PB1yvZj7Y9'},
            {'$dnanexus_link': 'file-GGJY9784p3z78j8F1qkp4GZ4'},
            {'$dnanexus_link': 'file-GGJY9804p3z1X9YZJ4xf5v13'}
        ]

        assert output_R1_fastqs == correct_R2_fastqs, (
            "R2 fastqs not correctly added"
        )

    def test_adding_all_r1_and_r2(self):
        """
        Test adding R1 and R2 fastqs from all samples as input
        where INPUT-R1-R2 given
        """
        output = ManageDict().add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict['applet-FvyXygj433GbKPPY0QY8ZKQG']["inputs"]
            ),
            fastq_details=self.fastq_details
        )
        output_fastqs = output['fastqs']

        correct_fastqs = [
            {'$dnanexus_link': 'file-GGJY9604p3zBzjz5Fp66KF0Y'},
            {'$dnanexus_link': 'file-GGJY96Q4p3z3233Q8v39Fzg2'},
            {'$dnanexus_link': 'file-GGJY9704p3z9P41f80bfQ623'},
            {'$dnanexus_link': 'file-GGJY97j4p3z250PB1yvZj7YF'},
            {'$dnanexus_link': 'file-GGJY9684p3zG6fvf1vqvbqzx'},
            {'$dnanexus_link': 'file-GGJY9704p3z250PB1yvZj7Y9'},
            {'$dnanexus_link': 'file-GGJY9784p3z78j8F1qkp4GZ4'},
            {'$dnanexus_link': 'file-GGJY9804p3z1X9YZJ4xf5v13'}
        ]

        assert output_fastqs == correct_fastqs, (
            "R1-R2 fastqs not correctly added"
        )

    def test_adding_per_sample_r1_fastqs(self):
        """
        Test adding fastqs when a sample defined => fastqs should be for just
        that sample
        """
        output = ManageDict().add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict['workflow-GB6J7qQ433Gkf0ZYGbKfF0x6']["inputs"]
            ),
            fastq_details=self.fastq_details,
            sample='2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2'
        )
        output_R1_fastqs = output['stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs']

        correct_R1_fastqs = [
            {'$dnanexus_link': 'file-GGJY9704p3z9P41f80bfQ623'}
        ]
        PPRINT(output)
        # print(output_R1_fastqs)
        # print(correct_R1_fastqs)

        assert output_R1_fastqs == correct_R1_fastqs, (
            "R1 fastqs not correctly added for given sample"
        )


    def test_adding_per_sample_r2_fastqs(self):
        """
        Test adding fastqs when a sample defined => fastqs should be for just
        that sample
        """
        output = ManageDict().add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict['workflow-GB6J7qQ433Gkf0ZYGbKfF0x6']["inputs"]
            ),
            fastq_details=self.fastq_details,
            sample='2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2'
        )
        PPRINT(output)
        output_R2_fastqs = output['stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs']

        correct_R2_fastqs = [
            {'$dnanexus_link': 'file-GGJY9784p3z78j8F1qkp4GZ4'}
        ]

        assert output_R2_fastqs == correct_R2_fastqs, (
            "R2 fastqs not correctly added for given sample"
        )

    def test_adding_all_r1_and_r2(self):
        """
        Test adding R1 and R2 fastqs for given sample as input
        where INPUT-R1-R2 given
        """
        output = ManageDict().add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict['applet-FvyXygj433GbKPPY0QY8ZKQG']["inputs"]
            ),
            fastq_details=self.fastq_details,
            sample='2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2'
        )
        output_fastqs = output['fastqs']

        correct_fastqs = [
            {'$dnanexus_link': 'file-GGJY9704p3z9P41f80bfQ623'},
            {'$dnanexus_link': 'file-GGJY9784p3z78j8F1qkp4GZ4'}
        ]

        assert output_fastqs == correct_fastqs, (
            "R1-R2 fastqs not correctly added for given sample"
        )

    def test_assert_equal_number_fastqs(self):
        """
        Test for assertion being raised where an unequal no. R1 and R2
        fastqs found
        """
        # copy list and remove one fastq to be unequal
        fastq_details_copy = self.fastq_details.copy()
        fastq_details_copy.remove(
            ('file-GGJY9604p3zBzjz5Fp66KF0Y',
            '2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2_S30_L002_R1_001.fastq.gz')
        )

        with pytest.raises(AssertionError):
            ManageDict().add_fastqs(
                input_dict=deepcopy(
                    self.test_input_dict['applet-FvyXygj433GbKPPY0QY8ZKQG']["inputs"]
                ),
                fastq_details=fastq_details_copy
            )

    def test_assert_found_fastqs(self):
        """
        Test when giving a sample to filter fastqs for if none are found
        then an AssertionError is raised
        """
        with pytest.raises(AssertionError):
            ManageDict().add_fastqs(
                input_dict=deepcopy(
                    self.test_input_dict['applet-FvyXygj433GbKPPY0QY8ZKQG']["inputs"]
                ),
                fastq_details=self.fastq_details,
                sample='test-sample'
            )


class TestAddOtherInputs():
    """
    Tests for add_other_inputs() used to gather up all random INPUT-
    keys and replace as required
    """
    # test input dict with all keys handled by add_other_inputs()
    test_input_dict = {
        "my_project_name": "INPUT-dx_project_name",
        "my_project_id": "INPUT-dx_project_id",
        "all_analysis_output": "INPUT-parent_out_dir",
        "custom_coverage": True,
        "eggd_multiqc_config_file": {
            "$dnanexus_link": "file-G0K191j433Gv6JG63b43z8Gy"
        },
        "sample_name_prefix": "INPUT-SAMPLE-PREFIX",
        "sample_name": "INPUT-SAMPLE-NAME",
        "output_path": "INPUT-analysis_1-out_dir"
    }

    # set up argparse namespace with required variables
    args = Namespace()
    args.dx_project_id = 'project-12345'
    args.dx_project_name = 'some_analysis_project'
    args.parent_out_dir = '/output/some_assay-220930-1200'

    analysis_output_directories = {
        "analysis_1": "/output/some_assay-220930-1200/my_first_app"
    }

    # call add_other_inputs() to replace all INPUT-s
    output = ManageDict().add_other_inputs(
        input_dict=test_input_dict,
        args = args,
        executable_out_dirs=analysis_output_directories,
        sample='my_sample_with_a_long_name',
        sample_prefix='my_sample'
    )

    def test_adding_sample_name(self):
        """
        Test for finding INPUT-SAMPLE-NAME and replacing with sample name
        """
        assert self.output['sample_name'] == 'my_sample_with_a_long_name', (
            'INPUT-SAMPLE-NAME not correctly replaced'
        )

    def test_adding_sample_prefix(self):
        """
        Test for finding INPUT-SAMPLE-PREFIX and replacing with sample prefix
        """
        assert self.output['sample_name_prefix'] == 'my_sample', (
            'INPUT-SAMPLE-PREFIX not correctly replaced'
        )

    def test_adding_project_id(self):
        """
        Test for finding INPUT-dx_project_id and replacing with project_id
        from args Namespace object
        """
        assert self.output['my_project_id'] == 'project-12345', (
            'INPUT-dx_project_id not correctly replaced'
        )

    def test_adding_project_name(self):
        """
        Test for finding INPUT-dx_project_name and replacing with project_name
        from args Namespace object
        """
        assert self.output['my_project_name'] == 'some_analysis_project', (
            'INPUT-dx_project_name not correctly replaced'
        )

    def test_adding_parent_out_dir(self):
        """
        Test for finding INPUT-parent_out_dir and replacing with parent output
        directory from args Namespace object
        """
        assert self.output['all_analysis_output'] == 'some_assay-220930-1200', (
            'INPUT-parent_out_dir not correctly replaced'
        )

    def test_adding_analysis_1_out_dir(self):
        """
        Test for finding INPUT-analysis_1_out_dir and replacing with the
        output path stored in the analysis output directories dictionary
        """
        correct_path = '/output/some_assay-220930-1200/my_first_app'
        assert self.output['output_path'] == correct_path, (
            'INPUT-analysis_1-out_dir not correctly replaced'
        )



class TestGetDependentJobs():
    """
    Test for get_dependent_jobs() that gathers up all jobs a downstream
    job requires to complete before launching.
    """
    # example structure of dict that tracks all jobs launched with
    # analysis_X keys, here there are 2 samples with per_sample jobs
    # (analysis_1  & analysis_3) and one per run job (analysis_2)
    job_outputs_dict = {
        '2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2': {
            'analysis_1': 'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2',
            'analysis_3': 'job-GGjgyX04Bv44Vz151GGzFKgP'
        },
        'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
            'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb',
            'analysis_3': 'job-GGp69xQ4Bv45bk0y4kyVqvJ1'
        },
        'analysis_2': 'job-GGjgz1j4Bv48yF89GpZ6zkGz'
    }

    def test_per_sample_w_per_run_dependent_job(self):
        """
        Test when calling get_dependent_jobs() for a per sample job that
        if it depends on a previous per run job, the job ID is correctly
        returned in the list of dependent jobs
        """
        params = {"depends_on": ["analysis_1", "analysis_2"]}

        jobs = ManageDict().get_dependent_jobs(
            params=params,
            job_outputs_dict=self.job_outputs_dict,
            sample="2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2"
        )

        sample_jobs = [
            'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2',
            'job-GGjgz1j4Bv48yF89GpZ6zkGz'
        ]

        assert sorted(jobs) == sample_jobs, (
            'Failed to get correct dependent jobs per sample'
        )

    def test_per_run_get_analysis_1_jobs(self):
        """
        Test for a per run job that depends on all analysis_1 jobs and
        therefore should return all job IDs for just analysis_1
        """
        params = {"depends_on": ["analysis_1"]}

        jobs = ManageDict().get_dependent_jobs(
            params=params,
            job_outputs_dict=self.job_outputs_dict
        )

        analysis_1_jobs = [
            'analysis-GGjgz004Bv4P8yqJGp9pyyqb',
            'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2'
        ]

        assert sorted(jobs) == analysis_1_jobs, (
            'Failed to get analysis_1 dependent jobs'
        )


    def test_per_run_get_all_jobs(self):
        """
        Test for a per run job that depends on all upstream jobs and
        therefore should return all job and analysis IDs
        """
        params = {"depends_on": ["analysis_1", "analysis_2", "analysis_3"]}

        jobs = ManageDict().get_dependent_jobs(
            params=params,
            job_outputs_dict=self.job_outputs_dict
        )

        all_jobs = [
            'analysis-GGjgz004Bv4P8yqJGp9pyyqb',
            'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2',
            'job-GGjgyX04Bv44Vz151GGzFKgP',
            'job-GGjgz1j4Bv48yF89GpZ6zkGz',
            'job-GGp69xQ4Bv45bk0y4kyVqvJ1'
        ]

        assert sorted(jobs) == all_jobs, (
            'Failed to get all dependent jobs'
        )


class TestLinkInputsToOutputs():
    """
    TODO
    """


class TestPopulateOutputDirConfig():
    """
    Tests for populate_output_dir_config() that takes a dict of output paths
    for a workflow or app and configures them with human readable names etc.
    """
    # output directories as would be defined in config
    app_output_config = {
        "applet-FvyXygj433GbKPPY0QY8ZKQG": "/OUT-FOLDER/APP-NAME"
    }

    workflow_output_config = {
        "stage-G9Z2B8841bQY907z1ygq7K9x": "/OUT-FOLDER/STAGE-NAME",
        "stage-G9Z2B7Q41bQg2Jy40zVqqGg4": "/OUT-FOLDER/STAGE-NAME"
    }

    # parent dir set at runtime based off assay name and date time
    parent_out_folder = '/output/myAssay_timestamp/'

    # dict as generated at run time of human names for each executable
    executable_names = {
        'applet-FvyXygj433GbKPPY0QY8ZKQG': {
            'name': 'multi_fastqc_v1.1.0'
        },
        'workflow-GB12vxQ433GygFZK6pPF75q8': {
            'name': 'somalier_workflow_v1.0.0',
            'stages': {
                'stage-G9Z2B7Q41bQg2Jy40zVqqGg4': 'eggd_somalier_relate2multiqc_v1.0.1',
                'stage-G9Z2B8841bQY907z1ygq7K9x': 'eggd_somalier_relate_v1.0.3'
            }
        }
    }

    def test_populate_app_output_dirs(self):
        """
        Test populating output path for an app
        """
        output_dict = ManageDict().populate_output_dir_config(
            executable='applet-FvyXygj433GbKPPY0QY8ZKQG',
            exe_names=self.executable_names,
            output_dict=self.app_output_config,
            out_folder=self.parent_out_folder
        )

        correct_output = {
            'applet-FvyXygj433GbKPPY0QY8ZKQG': '/output/myAssay_timestamp/multi_fastqc_v1.1.0'
        }

        assert output_dict == correct_output, (
            'Error in populating output path dict for app'
        )

    def test_populate_workflow_output_dirs(self):
        """
        Test populating output paths for each stage of a workflow
        """
        output_dict = ManageDict().populate_output_dir_config(
            executable='workflow-GB12vxQ433GygFZK6pPF75q8',
            exe_names=self.executable_names,
            output_dict=self.workflow_output_config,
            out_folder=self.parent_out_folder
        )

        correct_output = {
            'stage-G9Z2B7Q41bQg2Jy40zVqqGg4': '/output/myAssay_timestamp/eggd_somalier_relate2multiqc_v1.0.1',
            'stage-G9Z2B8841bQY907z1ygq7K9x': '/output/myAssay_timestamp/eggd_somalier_relate_v1.0.3'
        }

        assert output_dict == correct_output, (
            'Error in populating output path dict for workflow'
        )

    def test_not_replacing_hard_coded_paths(self):
        """
        Test when paths aren't using keys and are hard coded that they
        remain unmodified
        """
        output_config = {
            "applet-FvyXygj433GbKPPY0QY8ZKQG": "/some/hardcoded/path"
        }

        output_dict = ManageDict().populate_output_dir_config(
            executable='applet-FvyXygj433GbKPPY0QY8ZKQG',
            exe_names=self.executable_names,
            output_dict=output_config,
            out_folder=self.parent_out_folder
        )

        correct_output = {
            'applet-FvyXygj433GbKPPY0QY8ZKQG': '/some/hardcoded/path'
        }

        assert output_dict == correct_output, (
            'Output path dict with hardcoded paths wrongly modified'
        )


class TestFilterJobOutputsDict():
    """
    Test for filter_job_outputs_dict() that can filter down the all the
    jobs for a given analysis_X to keep those only for a sample(s) matching
    a set of given pattern(s)
    """
    # dict of per sample jobs launched as built in app
    job_outputs_dict = {
        '2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2': {
            'analysis_1': 'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2'
        },
        'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
            'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb'
        },
        'analysis_2': 'job-GGjgz1j4Bv48yF89GpZ6zkGz'
    }

    def test_filter_job_outputs_dict_one_pattern(self):
        """
        Test filtering job outputs -> inputs dict by given pattern(s)
        """
        # dict matching section as would be in config defining the stage
        # input and patterns to filter by
        inputs_filter = {
            "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": [
                "Oncospan.*"
            ]
        }

        # get the jobs for Oncospan sample
        filtered_output = ManageDict().filter_job_outputs_dict(
            stage='stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file',
            outputs_dict=self.job_outputs_dict,
            filter_dict=inputs_filter
        )

        correct_output = {
        'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
            'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb'
            }
        }

        assert filtered_output == correct_output, (
            "Filtering outputs dict with filter_job_outputs_dict() incorrect"
        )

    def test_filter_multiple_patterns(self):
        """
        Test filtering job inputs by multiple patterns returns correct IDs
        """
        # dict matching section as would be in config defining the stage
        # input and patterns to filter by
        inputs_filter = {
            "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": [
                "Oncospan.*",
                "2207155-22207Z0091.*"
            ]
        }

        # get the jobs for both samples
        filtered_output = ManageDict().filter_job_outputs_dict(
            stage='stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file',
            outputs_dict=self.job_outputs_dict,
            filter_dict=inputs_filter
        )

        correct_output = {
        '2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2': {
            'analysis_1': 'analysis-GGjgz0j4Bv4P8yqJGp9pyyv2'
        },
        'Oncospan-158-1-AA1-BBB-MYE-U-EGG2': {
            'analysis_1': 'analysis-GGjgz004Bv4P8yqJGp9pyyqb'
            }
        }

        assert filtered_output == correct_output, (
            "Filtering outputs dict with filter_job_outputs_dict() incorrect"
        )


class TestCheckAllInputs():
    """
    Tests for final check of populated input dict to check for remaining
    INPUT- or analysis_ that have not been parsed
    """
    input_dict_with_unparsed_input = {
        'stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs': [
            {'$dnanexus_link': 'file-GGJY8Q04p3z2K7qp1qf5bpkf'},
            {'$dnanexus_link': 'file-GGJY78j4p3zF10gz1xqv8v95'}
        ],
        'stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs': [
            {'$dnanexus_link': 'file-GGJY8Q04p3z5b57zJ4g5kQx7'},
            {'$dnanexus_link': 'file-GGJY78Q4p3z5pqB93Yb04gf1'},
            {'$dnanexus_link': 'INPUT-test'}
        ]
    }

    input_dict_with_unparsed_analysis = {
        'stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file': {
            '$dnanexus_link': {
                'analysis': 'analysis_500',
                'field': 'somalier_output',
                'stage': 'stage-G9x7x0Q41bQkpZXgBGzqGqX5'
            }
        }
    }

    def test_find_unparsed_input(self):
        """
        Test that AssertionError is raised from remaining INPUT-
        left in input dictionary
        """
        with pytest.raises(AssertionError):
            ManageDict().check_all_inputs(
                input_dict=self.input_dict_with_unparsed_input
            )

    def test_find_unparsed_analysis(self):
        """
        Test that AssertionError is raised from remaining analysis_
        left in input dictionary
        """
        with pytest.raises(AssertionError):
            ManageDict().check_all_inputs(
                input_dict=self.input_dict_with_unparsed_analysis
            )


if __name__ == '__main__':

    TestAddOtherInputs().test_adding_sample_name()
