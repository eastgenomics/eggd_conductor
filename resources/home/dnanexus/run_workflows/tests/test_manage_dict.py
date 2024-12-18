from copy import deepcopy
import json
import os
import unittest

import pytest

from utils.manage_dict import (
    search,
    replace,
    add_fastqs,
    add_upload_tars,
    add_other_inputs,
    get_dependent_jobs,
    link_inputs_to_outputs,
    filter_job_outputs_dict,
    fix_invalid_inputs,
    check_all_inputs,
    populate_tso500_reports_workflow,
)
from .settings import TEST_DATA_DIR


class TestSearchDict:
    """
    Tests for search() that searches a given dictionary for a
    given pattern against either keys or values, and can return the
    keys or values
    """

    with open(os.path.join(TEST_DATA_DIR, "test_low_level_config.json")) as fh:
        full_config = json.load(fh)

    def test_search_key_return_key_level1(self) -> None:
        """
        Test search() against first level, checking
        and returning keys
        """
        output = search(
            identifier="level1",
            input_dict=self.full_config,
            check_key=True,
            return_key=True,
        )

        correct_output = ["A_level1", "B_level1", "C_level1"]

        assert (
            sorted(output) == correct_output
        ), 'Wrong keys returned checking keys with identifier "level1"'

    def test_search_key_return_value_level1(self) -> None:
        """
        Test search() against first level, checking
        keys and returning values
        """
        output = search(
            identifier="level1",
            input_dict=self.full_config,
            check_key=True,
            return_key=False,
        )

        correct_output = [
            "A_level3_array_value1",
            "A_level3_array_value2",
            "A_level3_array_value3",
            "A_level3_array_value4",
            "A_level3_value1",
            "B_level3_value1",
            "B_level3_value2",
            "B_level3_value3",
            "C_array1_value1",
            "C_array1_value2",
            "C_array1_value3",
        ]

        assert (
            sorted(output) == correct_output
        ), 'Wrong values returned checking keys with identifier "level1"'

    def test_search_key_return_array_values(self) -> None:
        """
        Test search() against for key where values are an array
        """
        output = search(
            identifier="A_level3_key2",
            input_dict=self.full_config,
            check_key=True,
            return_key=False,
        )

        correct_output = [
            "A_level3_array_value1",
            "A_level3_array_value2",
            "A_level3_array_value3",
            "A_level3_array_value4",
        ]

        assert (
            sorted(output) == correct_output
        ), "Wrong values returned checking array of values"

    def test_search_dict_array(self) -> None:
        """
        Test search() against for key where values are an array
        of dicts and return each value
        """
        output = search(
            identifier="C_array1",
            input_dict=self.full_config,
            check_key=True,
            return_key=False,
        )

        correct_output = [
            "C_array1_value1",
            "C_array1_value2",
            "C_array1_value3",
        ]

        assert (
            sorted(output) == correct_output
        ), "Wrong values returned checking array of dict values"


class TestReplaceDict:
    """
    Tests for ManageDict.replace() that searches a dictionaries keys or values
    for a pattern, and replaces it with another given pattern
    """

    with open(os.path.join(TEST_DATA_DIR, "test_low_level_config.json")) as fh:
        full_config = json.load(fh)

    def test_replace_level1_keys(self):
        """
        Test replacing the first level of keys in the dictionary
        """
        output = replace(
            input_dict=self.full_config,
            to_replace="level1",
            replacement="test",
            search_key=True,
            replace_key=True,
        )

        # replacing all level1 keys with same so should only be one key
        assert list(output.keys()) == [
            "test"
        ], "Replacing level1 keys not correct"

    def test_replace_all_value1(self):
        """
        Test searching replacing all values containing 'value1'
        """
        output = replace(
            input_dict=self.full_config,
            to_replace="value1",
            replacement="test",
            search_key=False,
            replace_key=False,
        )

        correct_output = [
            {
                "A_level2": {
                    "A_level3_key1": "test",
                    "A_level3_key2": [
                        "test",
                        "A_level3_array_value2",
                        "A_level3_array_value3",
                        "A_level3_array_value4",
                    ],
                }
            },
            {
                "B_level2": {
                    "B_level3_key1": "test",
                    "B_level3_key2": "B_level3_value2",
                    "B_level3_key3": "B_level3_value3",
                }
            },
            {
                "C_level2": [
                    {"C_array1": "test"},
                    {"C_array1": "C_array1_value2"},
                    {"C_array1": "C_array1_value3"},
                ]
            },
        ]

        assert (
            list(output.values()) == correct_output
        ), "Searching and replacing 'value1' output incorrect"

    def test_replace_value_from_key(self):
        """
        Test replacing all values from keys matching 'B_level3'
        """
        output = replace(
            input_dict=self.full_config,
            to_replace="B_level3",
            replacement="test",
            search_key=True,
            replace_key=False,
        )

        correct_output = {
            "A_level1": {
                "A_level2": {
                    "A_level3_key1": "A_level3_value1",
                    "A_level3_key2": [
                        "A_level3_array_value1",
                        "A_level3_array_value2",
                        "A_level3_array_value3",
                        "A_level3_array_value4",
                    ],
                }
            },
            "B_level1": {
                "B_level2": {
                    "B_level3_key1": "test",
                    "B_level3_key2": "test",
                    "B_level3_key3": "test",
                }
            },
            "C_level1": {
                "C_level2": [
                    {"C_array1": "C_array1_value1"},
                    {"C_array1": "C_array1_value2"},
                    {"C_array1": "C_array1_value3"},
                ]
            },
        }

        assert (
            output == correct_output
        ), "Searching keys and replacing values returned wrong output"


class TestAddFastqs(unittest.TestCase):
    """
    Tests for adding fastq file IDs to input dict
    """

    fastq_details = [
        (
            "file-GGJY9604p3zBzjz5Fp66KF0Y",
            "2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2_S30_L002_R1_001.fastq.gz",
        ),
        (
            "file-GGJY9684p3zG6fvf1vqvbqzx",
            "2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2_S30_L002_R2_001.fastq.gz",
        ),
        (
            "file-GGJY96Q4p3z3233Q8v39Fzg2",
            "2207713-22222Z0074-1-BM-MPD-MYE-M-EGG2_S31_L002_R1_001.fastq.gz",
        ),
        (
            "file-GGJY9704p3z250PB1yvZj7Y9",
            "2207713-22222Z0074-1-BM-MPD-MYE-M-EGG2_S31_L002_R2_001.fastq.gz",
        ),
        (
            "file-GGJY9704p3z9P41f80bfQ623",
            "2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L002_R1_001.fastq.gz",
        ),
        (
            "file-GGJY9784p3z78j8F1qkp4GZ4",
            "2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L002_R2_001.fastq.gz",
        ),
        (
            "file-GGJY97j4p3z250PB1yvZj7YF",
            "Oncospan-158-2-AA1-BBB-MYE-U-EGG2_S33_L002_R1_001.fastq.gz",
        ),
        (
            "file-GGJY9804p3z1X9YZJ4xf5v13",
            "Oncospan-158-2-AA1-BBB-MYE-U-EGG2_S33_L002_R2_001.fastq.gz",
        ),
    ]

    # minimal test section of config with executables requiring fastqs
    test_input_dict = {
        "workflow-GB6J7qQ433Gkf0ZYGbKfF0x6": {
            "analysis": "analysis_1",
            "process_fastqs": True,
            "inputs": {
                "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs": "INPUT-R1",
                "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs": "INPUT-R2",
            },
        },
        "applet-FvyXygj433GbKPPY0QY8ZKQG": {
            "analysis": "analysis_2",
            "process_fastqs": True,
            "inputs": {"fastqs": "INPUT-R1-R2"},
            "output_dirs": {
                "applet-FvyXygj433GbKPPY0QY8ZKQG": "/OUT-FOLDER/APP-NAME"
            },
        },
    }

    def test_adding_all_r1(self):
        """
        Test adding R1 fastqs from all samples as input where INPUT-R1 given
        """
        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["workflow-GB6J7qQ433Gkf0ZYGbKfF0x6"][
                    "inputs"
                ]
            ),
            fastq_details=self.fastq_details,
        )
        output_R1_fastqs = output[
            "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs"
        ]

        correct_R1_fastqs = [
            {"$dnanexus_link": "file-GGJY9604p3zBzjz5Fp66KF0Y"},
            {"$dnanexus_link": "file-GGJY96Q4p3z3233Q8v39Fzg2"},
            {"$dnanexus_link": "file-GGJY9704p3z9P41f80bfQ623"},
            {"$dnanexus_link": "file-GGJY97j4p3z250PB1yvZj7YF"},
        ]

        assert (
            output_R1_fastqs == correct_R1_fastqs
        ), "R1 fastqs not correctly added"

    def test_adding_all_r2(self):
        """
        Test adding R2 fastqs from all samples as input where INPUT-R2 given
        """
        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["workflow-GB6J7qQ433Gkf0ZYGbKfF0x6"][
                    "inputs"
                ]
            ),
            fastq_details=self.fastq_details,
        )
        output_R2_fastqs = output[
            "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs"
        ]

        correct_R2_fastqs = [
            {"$dnanexus_link": "file-GGJY9684p3zG6fvf1vqvbqzx"},
            {"$dnanexus_link": "file-GGJY9704p3z250PB1yvZj7Y9"},
            {"$dnanexus_link": "file-GGJY9784p3z78j8F1qkp4GZ4"},
            {"$dnanexus_link": "file-GGJY9804p3z1X9YZJ4xf5v13"},
        ]

        assert (
            output_R2_fastqs == correct_R2_fastqs
        ), "R2 fastqs not correctly added"

    def test_adding_all_r1_and_r2(self):
        """
        Test adding R1 and R2 fastqs from all samples as input
        where INPUT-R1-R2 given
        """
        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["applet-FvyXygj433GbKPPY0QY8ZKQG"][
                    "inputs"
                ]
            ),
            fastq_details=self.fastq_details,
        )
        output_fastqs = output["fastqs"]

        correct_fastqs = [
            {"$dnanexus_link": "file-GGJY9604p3zBzjz5Fp66KF0Y"},
            {"$dnanexus_link": "file-GGJY96Q4p3z3233Q8v39Fzg2"},
            {"$dnanexus_link": "file-GGJY9704p3z9P41f80bfQ623"},
            {"$dnanexus_link": "file-GGJY97j4p3z250PB1yvZj7YF"},
            {"$dnanexus_link": "file-GGJY9684p3zG6fvf1vqvbqzx"},
            {"$dnanexus_link": "file-GGJY9704p3z250PB1yvZj7Y9"},
            {"$dnanexus_link": "file-GGJY9784p3z78j8F1qkp4GZ4"},
            {"$dnanexus_link": "file-GGJY9804p3z1X9YZJ4xf5v13"},
        ]

        assert (
            output_fastqs == correct_fastqs
        ), "R1-R2 fastqs not correctly added"

    def test_adding_per_sample_r1_fastqs(self):
        """
        Test adding fastqs when a sample defined => fastqs should be for just
        that sample
        """
        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["workflow-GB6J7qQ433Gkf0ZYGbKfF0x6"][
                    "inputs"
                ]
            ),
            fastq_details=self.fastq_details,
            sample="2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2",
        )
        output_R1_fastqs = output[
            "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs"
        ]

        correct_R1_fastqs = [
            {"$dnanexus_link": "file-GGJY9704p3z9P41f80bfQ623"}
        ]

        assert (
            output_R1_fastqs == correct_R1_fastqs
        ), "R1 fastqs not correctly added for given sample"

    def test_adding_per_sample_r2_fastqs(self):
        """
        Test adding fastqs when a sample defined => fastqs should be for just
        that sample
        """
        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["workflow-GB6J7qQ433Gkf0ZYGbKfF0x6"][
                    "inputs"
                ]
            ),
            fastq_details=self.fastq_details,
            sample="2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2",
        )
        output_R2_fastqs = output[
            "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs"
        ]

        correct_R2_fastqs = [
            {"$dnanexus_link": "file-GGJY9784p3z78j8F1qkp4GZ4"}
        ]

        assert (
            output_R2_fastqs == correct_R2_fastqs
        ), "R2 fastqs not correctly added for given sample"

    def test_adding_all_r1_and_r2_for_one_sample(self):
        """
        Test adding R1 and R2 fastqs for given sample as input
        where INPUT-R1-R2 given
        """
        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["applet-FvyXygj433GbKPPY0QY8ZKQG"][
                    "inputs"
                ]
            ),
            fastq_details=self.fastq_details,
            sample="2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2",
        )
        output_fastqs = output["fastqs"]

        correct_fastqs = [
            {"$dnanexus_link": "file-GGJY9704p3z9P41f80bfQ623"},
            {"$dnanexus_link": "file-GGJY9784p3z78j8F1qkp4GZ4"},
        ]

        assert (
            output_fastqs == correct_fastqs
        ), "R1-R2 fastqs not correctly added for given sample"

    def test_assert_equal_number_fastqs(self):
        """
        Test for assertion being raised where an unequal no. R1 and R2
        fastqs found
        """
        # copy list and remove one fastq to be unequal
        fastq_details_copy = self.fastq_details.copy()
        fastq_details_copy.remove(
            (
                "file-GGJY9604p3zBzjz5Fp66KF0Y",
                "2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2_S30_L002_R1_001.fastq.gz",
            )
        )

        with pytest.raises(AssertionError):
            add_fastqs(
                input_dict=deepcopy(
                    self.test_input_dict["applet-FvyXygj433GbKPPY0QY8ZKQG"][
                        "inputs"
                    ]
                ),
                fastq_details=fastq_details_copy,
            )

    def test_assert_found_fastqs(self):
        """
        Test when giving a sample to filter fastqs for if none are found
        then an AssertionError is raised
        """
        with pytest.raises(AssertionError):
            add_fastqs(
                input_dict=deepcopy(
                    self.test_input_dict["applet-FvyXygj433GbKPPY0QY8ZKQG"][
                        "inputs"
                    ]
                ),
                fastq_details=self.fastq_details,
                sample="test-sample",
            )

    def test_sorting_per_lane_correct(self):
        """
        Test that fastqs are sorted and added in the correct order
        by lane number, ensure we are actually sorting on the filename
        and not the file ID
        """
        fastq_details = [
            (
                "file-xxx2",
                "2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L001_R1_001.fastq.gz",
            ),
            (
                "file-xxx1",
                "2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L002_R1_001.fastq.gz",
            ),
            (
                "file-yyy2",
                "2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L001_R2_001.fastq.gz",
            ),
            (
                "file-yyy1",
                "2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2_S32_L002_R2_001.fastq.gz",
            ),
        ]

        output = add_fastqs(
            input_dict=deepcopy(
                self.test_input_dict["workflow-GB6J7qQ433Gkf0ZYGbKfF0x6"][
                    "inputs"
                ]
            ),
            fastq_details=fastq_details,
            sample="2207714-22222Z0110-1-BM-MPD-MYE-M-EGG2",
        )

        # if we were to be sorting on the file ID, we would expect the
        # respective xxx1 and yyy1 IDs to be returned first, sorting on
        # file name should give xxx2 and yyy2 first
        with self.subTest():
            self.assertEqual(
                output["stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs"],
                [
                    {"$dnanexus_link": "file-xxx2"},
                    {"$dnanexus_link": "file-xxx1"},
                ],
            )

        with self.subTest():
            self.assertEqual(
                output["stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs"],
                [
                    {"$dnanexus_link": "file-yyy2"},
                    {"$dnanexus_link": "file-yyy1"},
                ],
            )


class TestAddUploadTars:
    """
    Tests for adding upload tars to input dict
    """

    input_dict = {
        "TSO500_ruo": {"$dnanexus_link": "file-Fz4X61Q44Bv44FyfJX1jJPj6"},
        "samplesheet": {"$dnanexus_link": "INPUT-SAMPLESHEET"},
        "input_files": "INPUT-UPLOAD_TARS",
    }

    # formatted as returned from DXManage.get_upload_tars
    upload_tars = [
        {"$dnanexus_link": "file-GGyq26j4X7kXxJ0fG5PF08jy"},
        {"$dnanexus_link": "file-GGyq6y84X7kYPFVBG89bBBF7"},
        {"$dnanexus_link": "file-GGyq94j4X7kp6xB0GGx3q864"},
        {"$dnanexus_link": "file-GGyqFPQ4X7kqFx1bG6596qqQ"},
        {"$dnanexus_link": "file-GGyqK2j4X7kzzFfbGKBk8Xgp"},
        {"$dnanexus_link": "file-GGyqXp04X7kb7zV99ZYV3k9b"},
        {"$dnanexus_link": "file-GGyqgF84X7kY0fjZBk6jb68P"},
    ]

    parsed_dict = add_upload_tars(
        input_dict=input_dict, upload_tars=upload_tars
    )

    def test_adding_upload_tars(self):
        """
        Test that INPUT-UPLOAD_TARS is replaced by the list of file IDs
        """
        assert (
            self.parsed_dict.get("input_files") == self.upload_tars
        ), "Upload tars not correctly added to input dict"


class TestAddOtherInputs:
    """
    Tests for add_other_inputs() used to gather up all random INPUT-
    keys and replace as required
    """

    @pytest.fixture
    def other_inputs(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            "utils.manage_dict.get_job_out_folder",
            lambda _: "/out_dir/dir1",
        )

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
            "output_path": "INPUT-analysis_1-out_dir",
            "run_sample_sheet": {"$dnanexus_link": "INPUT-SAMPLESHEET"},
        }

        dx_project_id = "project-12345"
        dx_project_name = "some_analysis_project"
        parent_out_dir = "/output/some_assay-220930-1200"

        # set samplesheet file ID as env variable as set in eggd_conductor.sh
        os.environ["SAMPLESHEET_ID"] = (
            "{'$dnanexus_link': 'file-GGxPVxQ4X7kbkFBx7b913b0G'}"
        )

        job_outputs = {"analysis_1": "job-id"}

        # call add_other_inputs() to replace all INPUT-s
        return add_other_inputs(
            input_dict=test_input_dict,
            parent_out_dir=parent_out_dir,
            project_id=dx_project_id,
            project_name=dx_project_name,
            sample="my_sample_with_a_long_name",
            sample_prefix="my_sample",
            job_outputs_dict=job_outputs,
        )

    def test_adding_sample_name(self, other_inputs):
        """
        Test for finding INPUT-SAMPLE-NAME and replacing with sample name
        """
        assert (
            other_inputs["sample_name"] == "my_sample_with_a_long_name"
        ), "INPUT-SAMPLE-NAME not correctly replaced"

    def test_adding_sample_prefix(self, other_inputs):
        """
        Test for finding INPUT-SAMPLE-PREFIX and replacing with sample prefix
        """
        assert (
            other_inputs["sample_name_prefix"] == "my_sample"
        ), "INPUT-SAMPLE-PREFIX not correctly replaced"

    def test_adding_project_id(self, other_inputs):
        """
        Test for finding INPUT-dx_project_id and replacing with project_id
        from args Namespace object
        """
        assert (
            other_inputs["my_project_id"] == "project-12345"
        ), "INPUT-dx_project_id not correctly replaced"

    def test_adding_project_name(self, other_inputs):
        """
        Test for finding INPUT-dx_project_name and replacing with project_name
        from args Namespace object
        """
        assert (
            other_inputs["my_project_name"] == "some_analysis_project"
        ), "INPUT-dx_project_name not correctly replaced"

    def test_adding_parent_out_dir(self, other_inputs):
        """
        Test for finding INPUT-parent_out_dir and replacing with parent output
        directory from args Namespace object
        """
        assert (
            other_inputs["all_analysis_output"] == "some_assay-220930-1200"
        ), "INPUT-parent_out_dir not correctly replaced"

    def test_adding_samplesheet(self, other_inputs):
        """
        Test for finding and replacing INPUT-SAMPLESHEET from SAMPLESHEET
        environment variable which will be the dnanexus file ID
        """
        correct_samplesheet = {
            "$dnanexus_link": "file-GGxPVxQ4X7kbkFBx7b913b0G"
        }

        assert (
            other_inputs["run_sample_sheet"] == correct_samplesheet
        ), "Samplesheet not correctly parsed to input dict"

    def test_adding_analysis_1_out_dir(self, other_inputs):
        """
        Test for finding INPUT-analysis_1_out_dir and replacing with the
        output path stored in the analysis output directories dictionary
        """
        correct_path = "/out_dir/dir1"
        assert (
            other_inputs["output_path"] == correct_path
        ), "INPUT-analysis_1-out_dir not correctly replaced"


class TestGetDependentJobs:
    """
    Test for get_dependent_jobs() that gathers up all jobs a downstream
    job requires to complete before launching.
    """

    # example structure of dict that tracks all jobs launched with
    # analysis_X keys, here there are 2 samples with per_sample jobs
    # (analysis_1  & analysis_3) and one per run job (analysis_2)
    job_outputs_dict = {
        "2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2": {
            "analysis_1": "analysis-GGjgz0j4Bv4P8yqJGp9pyyv2",
            "analysis_3": "job-GGjgyX04Bv44Vz151GGzFKgP",
        },
        "Oncospan-158-1-AA1-BBB-MYE-U-EGG2": {
            "analysis_1": "analysis-GGjgz004Bv4P8yqJGp9pyyqb",
            "analysis_3": "job-GGp69xQ4Bv45bk0y4kyVqvJ1",
        },
        "analysis_2": "job-GGjgz1j4Bv48yF89GpZ6zkGz",
    }

    def test_per_sample_w_per_run_dependent_job(self):
        """
        Test when calling get_dependent_jobs() for a per sample job that
        if it depends on a previous per run job, the job ID is correctly
        returned in the list of dependent jobs
        """
        params = {"depends_on": ["analysis_1", "analysis_2"]}

        jobs = get_dependent_jobs(
            params=params,
            job_outputs_dict=self.job_outputs_dict,
            sample="2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2",
        )

        sample_jobs = [
            "analysis-GGjgz0j4Bv4P8yqJGp9pyyv2",
            "job-GGjgz1j4Bv48yF89GpZ6zkGz",
        ]

        assert (
            sorted(jobs) == sample_jobs
        ), "Failed to get correct dependent jobs per sample"

    def test_per_run_get_analysis_1_jobs(self):
        """
        Test for a per run job that depends on all analysis_1 jobs and
        therefore should return all job IDs for just analysis_1
        """
        params = {"depends_on": ["analysis_1"]}

        jobs = get_dependent_jobs(
            params=params, job_outputs_dict=self.job_outputs_dict
        )

        analysis_1_jobs = [
            "analysis-GGjgz004Bv4P8yqJGp9pyyqb",
            "analysis-GGjgz0j4Bv4P8yqJGp9pyyv2",
        ]

        assert (
            sorted(jobs) == analysis_1_jobs
        ), "Failed to get analysis_1 dependent jobs"

    def test_per_run_get_all_jobs(self):
        """
        Test for a per run job that depends on all upstream jobs and
        therefore should return all job and analysis IDs
        """
        params = {"depends_on": ["analysis_1", "analysis_2", "analysis_3"]}

        jobs = get_dependent_jobs(
            params=params, job_outputs_dict=self.job_outputs_dict
        )

        all_jobs = [
            "analysis-GGjgz004Bv4P8yqJGp9pyyqb",
            "analysis-GGjgz0j4Bv4P8yqJGp9pyyv2",
            "job-GGjgyX04Bv44Vz151GGzFKgP",
            "job-GGjgz1j4Bv48yF89GpZ6zkGz",
            "job-GGp69xQ4Bv45bk0y4kyVqvJ1",
        ]

        assert sorted(jobs) == all_jobs, "Failed to get all dependent jobs"

    def test_absent_analysis_does_not_raise_error(self):
        """
        Test that when an analysis_ value is given that is not present
        in the job outputs dict, it does not raise an error and just
        returns an empty list
        """
        params = {"depends_on": ["analysis_5"]}

        jobs = get_dependent_jobs(
            params=params, job_outputs_dict=self.job_outputs_dict
        )

        assert (
            jobs == []
        ), "Getting dependent jobs for absent analyis_ did not return an empty list"

    def test_absent_analysis_does_not_raise_error_per_sample(self):
        """
        Test that when an analysis_ value is given that is not present
        in the job outputs dict when searching for a given sample, it
        does not raise an error and just returns an empty list
        """
        params = {"depends_on": ["analysis_5"]}

        jobs = get_dependent_jobs(
            params=params,
            job_outputs_dict=self.job_outputs_dict,
            sample="2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2",
        )

        assert (
            jobs == []
        ), "Getting dependent jobs for absent analyis_ did not return an empty list"


class TestLinkInputsToOutputs:
    """
    Tests for linking the output of a job(s) to the input of another
    """

    job_outputs = {
        "2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2": {
            "analysis_1": "analysis-GGp34p84Bv40x7Kj4bjB55JG"
        },
        "2207674-22220Z0059-1-BM-MPD-MYE-M-EGG2": {
            "analysis_1": "analysis-GGp34q04Bv4688gq4bYxBJb7"
        },
        "2207859-22227Z0029-1-PB-MPD-MYE-F-EGG2": {
            "analysis_1": "analysis-GGp34v04Bv44xKp04f5ygGb4"
        },
        "2207862-22227Z0035-1-PB-MPD-MYE-M-EGG2": {
            "analysis_1": "analysis-GGp34vj4Bv40x7Kj4bjB55Jk"
        },
        "Oncospan-158-1-AA1-BBB-MYE-U-EGG2": {
            "analysis_1": "analysis-GGp34kQ4Bv4KkyxF4f91V26q"
        },
        "analysis_2": "job-GGp34xQ4Bv4KkyxF4f91V278",
    }

    input_dict_analysis_1 = {
        "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": {
            "$dnanexus_link": {
                "analysis": "analysis_1",
                "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                "field": "somalier",
            }
        }
    }
    input_dict_analysis_2 = {
        "stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input": {
            "$dnanexus_link": {
                "analysis": "analysis_2",
                "stage": "stage-G0KbB6Q433GyV6vbJZKVYV96",
                "field": "output_vcf",
            }
        }
    }

    def test_adding_all_analysis_1(self):
        """
        Tests for building array for all of analysis_1 jobs and adding
        as input
        """
        output = link_inputs_to_outputs(
            job_outputs_dict=self.job_outputs,
            input_dict=deepcopy(self.input_dict_analysis_1),
            analysis="analysis_2",
            per_sample=False,
        )

        output_input_dict = sorted(
            output["stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file"],
            key=lambda x: x["$dnanexus_link"]["analysis"],
        )

        # input dict we expect where analysis_1 outputs for all jobs
        # is being provided as input and is turned into an array
        # of $dnanexus_link onjects
        correct_input = [
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGp34kQ4Bv4KkyxF4f91V26q",
                    "field": "somalier",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGp34p84Bv40x7Kj4bjB55JG",
                    "field": "somalier",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGp34q04Bv4688gq4bYxBJb7",
                    "field": "somalier",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGp34v04Bv44xKp04f5ygGb4",
                    "field": "somalier",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGp34vj4Bv40x7Kj4bjB55Jk",
                    "field": "somalier",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
        ]

        assert (
            output_input_dict == correct_input
        ), "job IDs for all analysis_1 jobs not correctly parsed"

    def test_adding_one_sample_analysis_1(self):
        """
        Test for adding analysis_1 job ID for given sample
        """
        output = link_inputs_to_outputs(
            job_outputs_dict=self.job_outputs,
            input_dict=deepcopy(self.input_dict_analysis_1),
            analysis="analysis_2",
            per_sample=True,
            sample="Oncospan-158-1-AA1-BBB-MYE-U-EGG2",
        )

        # input dict we expect where analysis_1 outputs for given sample
        correct_input = {
            "$dnanexus_link": {
                "analysis": "analysis-GGp34kQ4Bv4KkyxF4f91V26q",
                "field": "somalier",
                "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
            }
        }

        assert (
            output["stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file"]
            == correct_input
        ), "job IDs for single sample analysis_1 jobs not correctly parsed"

    def test_parse_output_of_per_run_job(self):
        """
        Test for parsing out job ID of analysis_2 from job_outputs dict
        which is a per_run job and analysis_2 is a root key of the dict
        """
        output = link_inputs_to_outputs(
            job_outputs_dict=self.job_outputs,
            input_dict=deepcopy(self.input_dict_analysis_2),
            analysis="analysis_2",
            per_sample=False,
        )

        correct_output = [
            {
                "$dnanexus_link": {
                    "analysis": "job-GGp34xQ4Bv4KkyxF4f91V278",
                    "field": "output_vcf",
                    "stage": "stage-G0KbB6Q433GyV6vbJZKVYV96",
                }
            }
        ]

        assert (
            output["stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input"]
            == correct_output
        ), "job ID for analysis 2 wrongly parsed as input"


class TestFilterJobOutputsDict:
    """
    Test for filter_job_outputs_dict() that can filter down the all the
    jobs for a given analysis_X to keep those only for a sample(s) matching
    a set of given pattern(s)
    """

    # dict of per sample jobs launched as built in app
    job_outputs_dict = {
        "2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2": {
            "analysis_1": "analysis-GGjgz0j4Bv4P8yqJGp9pyyv2"
        },
        "Oncospan-158-1-AA1-BBB-MYE-U-EGG2": {
            "analysis_1": "analysis-GGjgz004Bv4P8yqJGp9pyyqb"
        },
        "analysis_2": "job-GGjgz1j4Bv48yF89GpZ6zkGz",
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
        filtered_output = filter_job_outputs_dict(
            stage="stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file",
            outputs_dict=self.job_outputs_dict,
            filter_dict=inputs_filter,
        )

        correct_output = {
            "Oncospan-158-1-AA1-BBB-MYE-U-EGG2": {
                "analysis_1": "analysis-GGjgz004Bv4P8yqJGp9pyyqb"
            }
        }

        assert (
            filtered_output == correct_output
        ), "Filtering outputs dict with filter_job_outputs_dict() incorrect"

    def test_filter_multiple_patterns(self):
        """
        Test filtering job inputs by multiple patterns returns correct IDs
        """
        # dict matching section as would be in config defining the stage
        # input and patterns to filter by
        inputs_filter = {
            "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": [
                "Oncospan.*",
                "2207155-22207Z0091.*",
            ]
        }

        # get the jobs for both samples
        filtered_output = filter_job_outputs_dict(
            stage="stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file",
            outputs_dict=self.job_outputs_dict,
            filter_dict=inputs_filter,
        )

        correct_output = {
            "2207155-22207Z0091-1-BM-MPD-MYE-M-EGG2": {
                "analysis_1": "analysis-GGjgz0j4Bv4P8yqJGp9pyyv2"
            },
            "Oncospan-158-1-AA1-BBB-MYE-U-EGG2": {
                "analysis_1": "analysis-GGjgz004Bv4P8yqJGp9pyyqb"
            },
        }

        assert (
            filtered_output == correct_output
        ), "Filtering outputs dict with filter_job_outputs_dict() incorrect"


class TestFixInvalidInputs:
    """
    Tests for checking classes of input in input dict to ensure they are
    correct against what the app / workflow expects
    """

    # mapping of inputs -> class from get_input_classes()
    input_classes = {
        "applet-Fz93FfQ433Gvf6pKFZYbXZQf": {
            "custom_coverage": {"class": "boolean", "optional": False},
            "eggd_multiqc_config_file": {"class": "file", "optional": False},
            "ms_for_multiqc": {"class": "string", "optional": True},
            "project_for_multiqc": {"class": "string", "optional": False},
            "single_folder": {"class": "boolean", "optional": False},
            "ss_for_multiqc": {"class": "string", "optional": False},
        },
        "workflow-GB12vxQ433GygFZK6pPF75q8": {
            "stage-G9Z2B7Q41bQg2Jy40zVqqGg4.female_threshold": {
                "class": "int",
                "optional": False,
            },
            "stage-G9Z2B7Q41bQg2Jy40zVqqGg4.male_threshold": {
                "class": "int",
                "optional": False,
            },
            "stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input": {
                "class": "file",
                "optional": False,
            },
            "stage-G9Z2B8841bQY907z1ygq7K9x.file_prefix": {
                "class": "string",
                "optional": False,
            },
            "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": {
                "class": "array:file",
                "optional": False,
            },
        },
    }

    # dict with input that expected to be an array but is a single dict
    test_input_dict1 = {
        "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": {
            "$dnanexus_link": {
                "analysis": "analysis-GGqJBYQ4Bv44xxK04b4k7G12",
                "field": "somalier_output",
                "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
            }
        }
    }

    # dict that expects to be a single file but is a list with one item
    test_input_dict2 = {
        "stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input": [
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGqJBYQ4Bv44xxK04b4k7G12",
                    "field": "somalier_output",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            }
        ]
    }

    # dict that expects to be a single file but is a list with one item
    test_input_dict3 = {
        "stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input": [
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGqJBYQ4Bv44xxK04b4k7G12",
                    "field": "somalier_output",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
            {
                "$dnanexus_link": {
                    "analysis": "analysis-GGqJBYQ4Bv44xxK04b4k7G12",
                    "field": "somalier_output",
                    "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
                }
            },
        ]
    }

    def test_array_input_with_single_dict_given(self):
        """
        Test when an input expects to be an array and a single dict is given
        if this is correctly changed to a list
        """
        output = fix_invalid_inputs(
            input_dict=self.test_input_dict1,
            input_classes=self.input_classes[
                "workflow-GB12vxQ433GygFZK6pPF75q8"
            ],
        )

        input_type = type(
            output["stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file"]
        )

        assert (
            input_type == list
        ), "array:file input not converted to a list as expected"

    def test_file_input_with_array_length_one_given(self):
        """
        Test when a list with one input given to an input that expects to be
        a single file that it is correctly set to a dict
        """
        output = fix_invalid_inputs(
            input_dict=self.test_input_dict2,
            input_classes=self.input_classes[
                "workflow-GB12vxQ433GygFZK6pPF75q8"
            ],
        )

        input_type = type(
            output["stage-G9Z2B7Q41bQg2Jy40zVqqGg4.somalier_input"]
        )

        assert input_type == dict, "Input type not correctly set to dict"

    def test_file_input_with_array_length_over_one(self):
        """
        Test that when an input that expects a file type is given
        an array with more than one item, as RuntimeError is raised
        """
        with pytest.raises(RuntimeError):
            fix_invalid_inputs(
                input_dict=self.test_input_dict3,
                input_classes=self.input_classes[
                    "workflow-GB12vxQ433GygFZK6pPF75q8"
                ],
            )

    def test_unknown_input_field(self):
        with pytest.raises(
            AssertionError,
            match="'not_existing_field' doesn't exist in the input_dict",
        ):
            fix_invalid_inputs(
                input_dict={"not_existing_field": None},
                input_classes={"existing_field": None},
            )


class TestCheckAllInputs:
    """
    Tests for final check of populated input dict to check for remaining
    INPUT- or analysis_ that have not been parsed
    """

    input_dict_with_unparsed_input = {
        "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads2_fastqgzs": [
            {"$dnanexus_link": "file-GGJY8Q04p3z2K7qp1qf5bpkf"},
            {"$dnanexus_link": "file-GGJY78j4p3zF10gz1xqv8v95"},
        ],
        "stage-G0qpXy0433Gv75XbPJ3xj8jV.reads_fastqgzs": [
            {"$dnanexus_link": "file-GGJY8Q04p3z5b57zJ4g5kQx7"},
            {"$dnanexus_link": "file-GGJY78Q4p3z5pqB93Yb04gf1"},
            {"$dnanexus_link": "INPUT-test"},
        ],
    }

    input_dict_with_unparsed_analysis = {
        "stage-G9Z2B8841bQY907z1ygq7K9x.somalier_extract_file": {
            "$dnanexus_link": {
                "analysis": "analysis_500",
                "field": "somalier_output",
                "stage": "stage-G9x7x0Q41bQkpZXgBGzqGqX5",
            }
        }
    }

    def test_find_unparsed_input(self):
        """
        Test that AssertionError is raised from remaining INPUT-
        left in input dictionary
        """
        with pytest.raises(AssertionError):
            check_all_inputs(input_dict=self.input_dict_with_unparsed_input)

    def test_find_unparsed_analysis(self):
        """
        Test that AssertionError is raised from remaining analysis_
        left in input dictionary
        """
        with pytest.raises(AssertionError):
            check_all_inputs(input_dict=self.input_dict_with_unparsed_analysis)


class TestPopulateTso500ReportsWorkflow(unittest.TestCase):
    """
    Tests for populate_tso500_reports_workflow

    Function handles the specific output -> input parsing of eggd_tso500
    and the downstream per sample eggd_tso500_reports_workflow. Tests
    here to ensure that the input dict is correctly populated and
    missing files will correctly be caught.
    """

    def setUp(self):
        """
        Define minimal sets of data structures expected for parsing
        """
        # input dict as would be parsed from assay config file, only
        # including the inputs here to link to eggd_tso500 outputs
        self.reports_workflow_input_dict = {
            "stage-multi_fastqc.fastqs": "eggd_tso500.fastqs",
            "stage-mosdepth.bam": "eggd_tso500.bam",
            "stage-mosdepth.index": "eggd_tso500.idx",
            "stage-vcf_rescue.gvcf": "eggd_tso500.vcf",
            "stage-generate_variant_workbook.additional_files": "eggd_tso500.cvo",
        }

        # minimal example files as would be returned from finding files in
        # eggd_tso500 directory with get_job_output_details()
        self.all_output_files = [
            {"id": "file-a1", "describe": {"name": "sample1_R1.fastq"}},
            {"id": "file-a2", "describe": {"name": "sample1_R2.fastq"}},
            {"id": "file-a3", "describe": {"name": "sample3_R1.fastq"}},
            {"id": "file-a4", "describe": {"name": "sample3_R2.fastq"}},
            {"id": "file-b1", "describe": {"name": "sample1.bam"}},
            {"id": "file-b2", "describe": {"name": "sample2.bam"}},
            {"id": "file-c1", "describe": {"name": "sample3.rna.bam"}},
            {"id": "file-c2", "describe": {"name": "sample4.rna.bam"}},
            {"id": "file-d1", "describe": {"name": "sample1.bam.bai"}},
            {"id": "file-d2", "describe": {"name": "sample2.bam.bai"}},
            {"id": "file-e1", "describe": {"name": "sample3.ran.bam.bai"}},
            {"id": "file-e2", "describe": {"name": "sample4.ran.bam.bai"}},
            {"id": "file-f1", "describe": {"name": "sample1.genome.vcf"}},
            {"id": "file-f2", "describe": {"name": "sample2.genome.vcf"}},
            {
                "id": "file-g1",
                "describe": {"name": "sample3.splice_variants.vcf"},
            },
            {
                "id": "file-g2",
                "describe": {"name": "sample4.splice_variants.vcf"},
            },
            {
                "id": "file-h1",
                "describe": {"name": "sample1_CombinedVariantOutput.tsv"},
            },
            {
                "id": "file-h2",
                "describe": {"name": "sample2_CombinedVariantOutput.tsv"},
            },
            {
                "id": "file-h3",
                "describe": {"name": "sample3_CombinedVariantOutput.tsv"},
            },
            {"id": "file-i1", "describe": {"name": "metricsOutput.tsv"}},
        ]

        # example mapping of eggd_tso500 output fields -> $dnanexus_links
        self.job_output_ids = {
            "fastqs": [
                {"$dnanexus_link": "file-a1"},
                {"$dnanexus_link": "file-a2"},
                {"$dnanexus_link": "file-a3"},
                {"$dnanexus_link": "file-a4"},
            ],
            "dna_bams": [
                {"$dnanexus_link": "file-b1"},
                {"$dnanexus_link": "file-b2"},
            ],
            "rna_bams": [
                {"$dnanexus_link": "file-c1"},
                {"$dnanexus_link": "file-c2"},
            ],
            "dna_bam_index": [
                {"$dnanexus_link": "file-d1"},
                {"$dnanexus_link": "file-d2"},
            ],
            "rna_bam_index": [
                {"$dnanexus_link": "file-e1"},
                {"$dnanexus_link": "file-e2"},
            ],
            "gvcfs": [
                {"$dnanexus_link": "file-f1"},
                {"$dnanexus_link": "file-f2"},
            ],
            "splice_variants_vcfs": [
                {"$dnanexus_link": "file-g1"},
                {"$dnanexus_link": "file-g2"},
            ],
            "cvo": [
                {"$dnanexus_link": "file-h1"},
                {"$dnanexus_link": "file-h2"},
                {"$dnanexus_link": "file-h3"},
            ],
            "metricsOutput": {"$dnanexus_link": "file-i1"},
        }

    def test_inputs_for_dna_sample_correct(self):
        """
        Test that for sample1 that is DNA (has dna_bam output) that the
        input dict is populated correctly
        """
        populated_input_dict = populate_tso500_reports_workflow(
            input_dict=self.reports_workflow_input_dict,
            sample="sample1",
            all_output_files=self.all_output_files,
            job_output_ids=self.job_output_ids,
        )

        expected_output = (
            {
                "stage-multi_fastqc.fastqs": [
                    {"$dnanexus_link": "file-a1"},
                    {"$dnanexus_link": "file-a2"},
                ],
                "stage-mosdepth.bam": {"$dnanexus_link": "file-b1"},
                "stage-mosdepth.index": {"$dnanexus_link": "file-d1"},
                "stage-vcf_rescue.gvcf": {"$dnanexus_link": "file-f1"},
                "stage-generate_variant_workbook.additional_files": [
                    {"$dnanexus_link": "file-h1"},
                    {"$dnanexus_link": "file-i1"},
                ],
            },
            None,
        )

        self.assertEqual(populated_input_dict, expected_output)

    def test_inputs_for_rna_sample_correct(self):
        """
        Test that for sample3 that is RNA (has rna_bam output and
        splice_variant vcf) that the input dict is populated correctly
        """
        populated_input_dict = populate_tso500_reports_workflow(
            input_dict=self.reports_workflow_input_dict,
            sample="sample3",
            all_output_files=self.all_output_files,
            job_output_ids=self.job_output_ids,
        )

        expected_output = (
            {
                "stage-multi_fastqc.fastqs": [
                    {"$dnanexus_link": "file-a3"},
                    {"$dnanexus_link": "file-a4"},
                ],
                "stage-mosdepth.bam": {"$dnanexus_link": "file-c1"},
                "stage-mosdepth.index": {"$dnanexus_link": "file-e1"},
                "stage-vcf_rescue.gvcf": {"$dnanexus_link": "file-g1"},
                "stage-generate_variant_workbook.additional_files": [
                    {"$dnanexus_link": "file-h3"},
                    {"$dnanexus_link": "file-i1"},
                ],
            },
            None,
        )

        self.assertEqual(populated_input_dict, expected_output)

    def test_missing_tso_output_file(self):
        """Test to ensure that the input dict is untouched if a sample is
        detected to be absent after processing by the eggd_tso app.
        """

        unmodified_input_dict = populate_tso500_reports_workflow(
            input_dict=self.reports_workflow_input_dict,
            sample="missing_sample",
            all_output_files=self.all_output_files,
            job_output_ids=self.job_output_ids,
        )

        expected_output = (
            self.reports_workflow_input_dict,
            "missing_sample",
        )

        self.assertEqual(unmodified_input_dict, expected_output)

    def test_missing_metrics_output_raises_assertion_error(self):
        """
        Test when run level metricsOutput is missing that an AssertionError
        is raised
        """
        missing_files = deepcopy(self.job_output_ids)
        missing_files.pop("metricsOutput")

        expected_error = "No metrics output file found from tso500 job"

        with pytest.raises(AssertionError, match=expected_error):
            populate_tso500_reports_workflow(
                input_dict=self.reports_workflow_input_dict,
                sample="sample1",
                all_output_files=self.all_output_files,
                job_output_ids=missing_files,
            )
