from datetime import datetime
import json
import os
import pathlib
import re
from unittest import mock
from unittest.mock import patch

import dxpy as dx
import pytest

from utils.AssayHandler import AssayHandler
from .settings import TEST_DATA_DIR

test_data_folder = pathlib.Path(f"{TEST_DATA_DIR}/build_job_inputs")


@pytest.fixture()
def empty_assay_handler():
    assay_handler = AssayHandler({})
    yield assay_handler
    del assay_handler


@pytest.fixture()
def normal_assay_handler():
    config = {"assay_code": "code1", "assay": "assay1", "version": "v1"}
    assay_handler = AssayHandler(config)
    # parent dir set at runtime based off assay name and date time
    assay_handler.parent_out_dir = "/output/myAssay_timestamp/"
    assay_handler.project = mock.MagicMock(
        id="MagicMock-id", name="MagicMock-name"
    )
    yield assay_handler
    del assay_handler


@pytest.fixture()
def executable_assay_handler():
    config = {
        "assay_code": "code1",
        "assay": "assay1",
        "version": "v1",
        "executables": {"workflow-id": "value", "app-id": "value"},
    }
    assay_handler = AssayHandler(config)
    yield assay_handler
    del assay_handler


@pytest.fixture()
def output_dirs_assay_handler(normal_assay_handler):
    normal_assay_handler.config["executables"] = {
        "applet-FvyXygj433GbKPPY0QY8ZKQG": {
            "output_dirs": {
                "applet-FvyXygj433GbKPPY0QY8ZKQG": "/OUT-FOLDER/APP-NAME",
            }
        },
        "workflow-GB12vxQ433GygFZK6pPF75q8": {
            "output_dirs": {
                "stage-G9Z2B8841bQY907z1ygq7K9x": "/OUT-FOLDER/STAGE-NAME",
                "stage-G9Z2B7Q41bQg2Jy40zVqqGg4": "/OUT-FOLDER/STAGE-NAME",
            }
        },
    }

    # dict as generated at run time of human names for each executable
    normal_assay_handler.execution_mapping = {
        "applet-FvyXygj433GbKPPY0QY8ZKQG": {"name": "multi_fastqc_v1.1.0"},
        "workflow-GB12vxQ433GygFZK6pPF75q8": {
            "name": "somalier_workflow_v1.0.0",
            "stages": {
                "stage-G9Z2B7Q41bQg2Jy40zVqqGg4": "eggd_somalier_relate2multiqc_v1.0.1",
                "stage-G9Z2B8841bQY907z1ygq7K9x": "eggd_somalier_relate_v1.0.3",
            },
        },
    }

    # fake the creation of the job_info_per_run dict as it is always present
    # before calling the populate_output_dir_config method
    normal_assay_handler.job_info_per_run = {
        "applet-FvyXygj433GbKPPY0QY8ZKQG": {"output_dirs": ""},
        "workflow-GB12vxQ433GygFZK6pPF75q8": {"output_dirs": ""},
    }

    yield normal_assay_handler
    del normal_assay_handler


@pytest.fixture()
def job_inputs_assay_handler(normal_assay_handler):
    with open(test_data_folder / "tso500_config.json") as f:
        normal_assay_handler.config = json.loads(f.read())

    # dict as generated at run time of human names for each executable
    normal_assay_handler.execution_mapping = {
        "app-GgBKy7j4QjX77XK69fG7Pj0y": {"name": "eggd_tso500-v2.0.1"},
        "applet-FvyXygj433GbKPPY0QY8ZKQG": {
            "name": "multi_fastqc_v1.1.0",
        },
        "app-GjF8gFQ4yyVBg4Y62BV10Vp2": {
            "name": "eggd_metricsoutput_editor-v1.1.0"
        },
        "app-Gpzb0Y04FXvff0vJPxj0p1x1": {
            "name": "eggd_MetricsOutput_MultiQC_parser-1.0.0"
        },
        "app-GF3K4Qj4bJyvpzx055V3G8q7": {
            "name": "eggd_MultiQC-v2.0.1",
        },
        "workflow-Gjk42k84yfKPv0x151ZvYBpK": {
            "name": "TSO500_reports_workflow_v2.0.0",
            "stages": {
                "stage-mosdepth": "eggd_mosdepth-1.1.0",
                "stage-athena": "eggd_athena-1.6.0",
                "stage-vcf_rescue": "eggd_vcf_rescue-1.1.0",
                "stage-vep": "eggd_vep-1.3.0",
                "stage-eggd_add_MANE_annotation": "eggd_add_MANE_annotation-1.1.0",
                "stage-generate_variant_workbook": "eggd_generate_variant_workbook-2.8.2",
            },
        },
    }

    normal_assay_handler.fastq_details = [
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

    normal_assay_handler.upload_tars = None
    normal_assay_handler.input_class_mapping = {
        "applet-FvyXygj433GbKPPY0QY8ZKQG": None,
        "app-GjF8gFQ4yyVBg4Y62BV10Vp2": None,
        "workflow-Gjk42k84yfKPv0x151ZvYBpK": None,
    }

    yield normal_assay_handler
    del normal_assay_handler


class TestAssayHandler:
    def test_correct_setup(self, normal_assay_handler):
        assert (
            normal_assay_handler.assay_code == "code1"
            and normal_assay_handler.assay == "assay1"
            and normal_assay_handler.version == "v1"
        ), "Configuration of Assay Handler incorrect"

    @pytest.mark.parametrize(
        "test_input, expected", [(1, 1), (4, 4), (5, 5), (0, 10)]
    )
    def test_limiting_number_samples(
        self, test_input, expected, empty_assay_handler
    ):
        empty_assay_handler.samples = [
            "sample1",
            "sample2",
            "sample3",
            "sample4",
            "sample5",
            "sample6",
            "sample7",
            "sample8",
            "sample9",
            "sample10",
        ]

        empty_assay_handler.limit_samples(limit_nb=test_input)

        assert expected == len(
            empty_assay_handler.samples
        ), "Unexpected number samples kept"

    @pytest.mark.parametrize(
        "test_input, expected",
        [
            (
                ["sample1"],
                [
                    "sample2",
                    "sample3",
                    "sample4",
                    "sample5",
                    "sample6",
                    "sample7",
                    "sample8",
                    "sample9",
                    "sample10",
                ],
            ),
            (
                ["sample1", "sample2"],
                [
                    "sample3",
                    "sample4",
                    "sample5",
                    "sample6",
                    "sample7",
                    "sample8",
                    "sample9",
                    "sample10",
                ],
            ),
            (
                [""],
                [
                    "sample1",
                    "sample2",
                    "sample3",
                    "sample4",
                    "sample5",
                    "sample6",
                    "sample7",
                    "sample8",
                    "sample9",
                    "sample10",
                ],
            ),
        ],
    )
    def test_limiting_using_sample_names(
        self, test_input, expected, empty_assay_handler
    ):
        empty_assay_handler.samples = [
            "sample1",
            "sample2",
            "sample3",
            "sample4",
            "sample5",
            "sample6",
            "sample7",
            "sample8",
            "sample9",
            "sample10",
        ]

        empty_assay_handler.limit_samples(samples_to_exclude=test_input)

        assert expected == empty_assay_handler.samples and len(
            expected
        ) == len(empty_assay_handler.samples), "Unexpected samples kept"

    def test_subset_with_missing_subset_samplesheet(
        self, normal_assay_handler
    ):
        normal_assay_handler.samples = ["sample1", "sample2"]

        normal_assay_handler.subset_samples()

        assert normal_assay_handler.samples == [
            "sample1",
            "sample2",
        ], "Samples have changed even without a subset samplesheet"

    def test_subset_with_incorrect_pattern(self):
        config = {"subset_samplesheet": "++"}
        assay_handler = AssayHandler(config)

        with pytest.raises(re.error, match="Invalid subset pattern provided"):
            assay_handler.subset_samples()

    def test_subset_no_samples_left(self):
        config = {"subset_samplesheet": "not_sample"}
        assay_handler = AssayHandler(config)
        assay_handler.samples = ["sample1", "sample2"]

        with pytest.raises(
            AssertionError,
            match=("No samples left after filtering using pattern not_sample"),
        ):
            assay_handler.subset_samples()

    def test_subset_some_samples(self):
        config = {"subset_samplesheet": "1"}
        assay_handler = AssayHandler(config)
        assay_handler.samples = ["sample1", "sample2", "sample10"]

        assay_handler.subset_samples()

        assert assay_handler.samples == [
            "sample1",
            "sample10",
        ], "Did not remove sample2 as expected"

    @patch("utils.AssayHandler.dx.bindings.dxproject.DXProject")
    @patch("utils.AssayHandler.find_dx_project")
    def test_get_dx_project(
        self, mock_project_id, mock_project_obj, normal_assay_handler
    ):
        mock_project_id.return_value = "project-id1"

        test_inputs = {"project_name": "002_run1_assay1", "run_id": "run1"}

        normal_assay_handler.get_or_create_dx_project(**test_inputs)

        assert mock_project_obj.call_count == 1, (
            "Expected one call to DXProject when getting the DXProject, got "
            f"'{mock_project_obj.call_count}'"
        )

    @patch("utils.AssayHandler.dx.bindings.dxproject.DXProject")
    @patch("utils.AssayHandler.find_dx_project")
    def test_create_dx_project(
        self, mock_project_id, mock_project_obj, normal_assay_handler
    ):
        mock_project_id.return_value = None
        mock_project_obj.return_value.new.return_value = "project_id1"

        test_inputs = {"project_name": "002_run1_assay1", "run_id": "run1"}

        normal_assay_handler.get_or_create_dx_project(**test_inputs)

        assert mock_project_obj.return_value.new.call_count == 1, (
            "Expected one call to DXProject.new when creating the DXProject, "
            f"got '{mock_project_obj.new.call_count}'"
        )

    def test_create_analysis_project_logs(self, normal_assay_handler):
        normal_assay_handler.project = dx.bindings.dxproject.DXProject()
        normal_assay_handler.project.id = "Fake ID"

        normal_assay_handler.create_analysis_project_logs()
        log_file = pathlib.Path("analysis_project.log")

        assert log_file.read_text() == (
            (
                f"{normal_assay_handler.project.id} "
                f"{normal_assay_handler.config.get('assay_code')} "
                f"{normal_assay_handler.config.get('version')}\n"
            )
        ), "Content of log file different than expected"

        log_file.unlink()

    def test_get_upload_tars_no_sentinel_file(self, empty_assay_handler):
        empty_assay_handler.get_upload_tars(None)

        assert (
            empty_assay_handler.upload_tars is None
        ), "Upload_tars is not None"

    @patch("utils.AssayHandler.dx.bindings.dxrecord.DXRecord")
    def test_get_upload_tars_correct_sentinel_file(
        self, mock_record, empty_assay_handler
    ):
        mock_record.return_value = dx.bindings.dxrecord.DXRecord
        mock_record.return_value.describe.return_value = {
            "details": {
                "tar_file_ids": [
                    "file_id1",
                    "file_id2",
                    "file_id3",
                    "file_id4",
                    "file_id5",
                ]
            }
        }

        empty_assay_handler.get_upload_tars("thing_that_returns_true")

        assert empty_assay_handler.upload_tars == [
            {"$dnanexus_link": "file_id1"},
            {"$dnanexus_link": "file_id2"},
            {"$dnanexus_link": "file_id3"},
            {"$dnanexus_link": "file_id4"},
            {"$dnanexus_link": "file_id5"},
        ]

    def test_set_parent_out_dir(self, normal_assay_handler):
        run_time = datetime.now().strftime("%y%m%d_%H%M")
        with mock.patch.dict(os.environ, {"DESTINATION": "PROJECT-ID"}):
            normal_assay_handler.set_parent_out_dir(run_time)

        expected_output = (
            f"PROJECT-ID/output/{normal_assay_handler.assay}-{run_time}"
        )
        assert normal_assay_handler.parent_out_dir == expected_output

    @patch("utils.AssayHandler.Slack.send")
    def test_get_executable_names_per_config_invalid_dx_executable(
        self, mock_slack_send, normal_assay_handler
    ):
        normal_assay_handler.config["executables"] = {
            "app-id": "value",
            "applet-id": "value",
            "invalid_workflow-id": "value",
        }

        with pytest.raises(AssertionError):
            normal_assay_handler.get_executable_names_per_config()
            assert mock_slack_send.call_args == (
                f"Executable(s) from the config not valid: "
                f'{normal_assay_handler.config.get("executables").keys()}'
            )

    @patch("utils.AssayHandler.dx.api.workflow_describe")
    def test_get_executable_names_per_config(
        self, mock_describe, executable_assay_handler
    ):
        mock_describe.side_effect = [
            {
                "name": "workflow-name",
                "stages": [
                    {"id": "stage-id1", "executable": "applet-id"},
                    {"id": "stage-id2", "executable": "app-id"},
                ],
            },
            {"name": "applet-name"},
            {"name": "app-name"},
        ]

        executable_assay_handler.get_executable_names_per_config()

        assert executable_assay_handler.execution_mapping == {
            "workflow-id": {
                "name": "workflow-name",
                "stages": {"stage-id1": "applet-name", "stage-id2": "id"},
            },
            "app-id": {"name": "name"},
        }

        assert mock_describe.call_count == 3

    @patch("utils.AssayHandler.dx.describe")
    def test_get_input_classes_per_config(
        self, mock_describe, executable_assay_handler
    ):
        mock_describe.side_effect = [
            {
                "inputSpec": [
                    {
                        "name": "input_name1",
                        "class": "input_class1",
                        "optional": True,
                    }
                ]
            },
            {
                "inputSpec": [
                    {
                        "name": "input_name2",
                        "class": "input_class2",
                    }
                ]
            },
        ]

        executable_assay_handler.get_input_classes_per_config()

        assert executable_assay_handler.input_class_mapping == {
            "workflow-id": {
                "input_name1": {"class": "input_class1", "optional": True}
            },
            "app-id": {
                "input_name2": {"class": "input_class2", "optional": False}
            },
        }

    @patch("utils.AssayHandler.dx_run")
    def test_job_calling_per_sample(self, mock_job_id, normal_assay_handler):
        mock_job_id.return_value = "job-id"
        normal_assay_handler.job_info_per_sample["sample1"] = {
            "executable1": {
                "job_name": "job_name1",
                "inputs": "inputs1",
                "output_dirs": "output_dirs1",
                "dependent_jobs": "dependent_jobs1",
                "extra_args": "extra_args1",
            }
        }
        normal_assay_handler.jobs = []
        normal_assay_handler.job_outputs = {}

        normal_assay_handler.call_job(
            "executable1", "analysis1", "instance1", "sample1"
        )

        expected_output = {"sample1": {"analysis1": "job-id"}}

        assert expected_output == normal_assay_handler.job_outputs


class TestPopulateOutputDirConfig:
    """
    Tests for populate_output_dir_config() that takes a dict of output paths
    for a workflow or app and configures them with human readable names etc.
    """

    def test_populate_app_output_dirs(self, output_dirs_assay_handler):
        """
        Test populating output path for an app
        """
        output_dirs_assay_handler.populate_output_dir_config(
            executable="applet-FvyXygj433GbKPPY0QY8ZKQG",
        )

        correct_output = {
            "applet-FvyXygj433GbKPPY0QY8ZKQG": "/output/myAssay_timestamp/multi_fastqc_v1.1.0"
        }

        output_dict = output_dirs_assay_handler.job_info_per_run[
            "applet-FvyXygj433GbKPPY0QY8ZKQG"
        ]["output_dirs"]

        assert (
            output_dict == correct_output
        ), "Error in populating output path dict for app"

    def test_populate_workflow_output_dirs(self, output_dirs_assay_handler):
        """
        Test populating output paths for each stage of a workflow
        """
        output_dirs_assay_handler.populate_output_dir_config(
            executable="workflow-GB12vxQ433GygFZK6pPF75q8",
        )

        correct_output = {
            "stage-G9Z2B7Q41bQg2Jy40zVqqGg4": "/output/myAssay_timestamp/eggd_somalier_relate2multiqc_v1.0.1",
            "stage-G9Z2B8841bQY907z1ygq7K9x": "/output/myAssay_timestamp/eggd_somalier_relate_v1.0.3",
        }

        output_dict = output_dirs_assay_handler.job_info_per_run[
            "workflow-GB12vxQ433GygFZK6pPF75q8"
        ]["output_dirs"]

        assert (
            output_dict == correct_output
        ), "Error in populating output path dict for workflow"

    def test_not_replacing_hard_coded_paths(self, output_dirs_assay_handler):
        """
        Test when paths aren't using keys and are hard coded that they
        remain unmodified
        """

        output_dirs_assay_handler.config["executables"][
            "applet-FvyXygj433GbKPPY0QY8ZKQG"
        ]["output_dirs"] = {
            "applet-FvyXygj433GbKPPY0QY8ZKQG": "/some/hardcoded/path"
        }

        output_dirs_assay_handler.populate_output_dir_config(
            executable="applet-FvyXygj433GbKPPY0QY8ZKQG",
        )

        correct_output = {
            "applet-FvyXygj433GbKPPY0QY8ZKQG": "/some/hardcoded/path"
        }

        output_dict = output_dirs_assay_handler.config["executables"][
            "applet-FvyXygj433GbKPPY0QY8ZKQG"
        ]["output_dirs"]

        assert (
            output_dict == correct_output
        ), "Output path dict with hardcoded paths wrongly modified"


class TestBuildJobInputs:
    with open(test_data_folder / "mocked_fixed_inputs.json") as f:
        mocked_fixed_inputs_json = json.loads(f.read())

    with open(test_data_folder / "mocked_handle_TSO500.json") as f:
        mocked_handle_TSO500 = json.loads(f.read())

    with open(test_data_folder / "expected_output.json") as f:
        expected_output = json.loads(f.read())

    @patch("utils.AssayHandler.manage_dict.fix_invalid_inputs")
    def test_build_job_inputs_per_sample(
        self, mock_fixed_inputs, job_inputs_assay_handler
    ):
        mock_fixed_inputs.return_value = {
            "fastqs": [
                {"$dnanexus_link": "file-GGJY9604p3zBzjz5Fp66KF0Y"},
                {"$dnanexus_link": "file-GGJY9684p3zG6fvf1vqvbqzx"},
            ]
        }
        params = job_inputs_assay_handler.config["executables"][
            "applet-FvyXygj433GbKPPY0QY8ZKQG"
        ]

        job_inputs_assay_handler.build_job_inputs(
            "applet-FvyXygj433GbKPPY0QY8ZKQG",
            params,
            "2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2",
        )

        expected_output = {
            "2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2": {
                "applet-FvyXygj433GbKPPY0QY8ZKQG": {
                    "dependent_jobs": [],
                    "job_name": "multi_fastqc_v1.1.0-2207712-22222Z0005-1-BM-MPD-MYE-M-EGG2",
                    "extra_args": {},
                    "inputs": {
                        "fastqs": [
                            {
                                "$dnanexus_link": "file-GGJY9604p3zBzjz5Fp66KF0Y"
                            },
                            {
                                "$dnanexus_link": "file-GGJY9684p3zG6fvf1vqvbqzx"
                            },
                        ]
                    },
                }
            }
        }

        assert expected_output == job_inputs_assay_handler.job_info_per_sample

    @patch("utils.AssayHandler.manage_dict.fix_invalid_inputs")
    def test_build_job_inputs_per_run(
        self, mock_fixed_inputs, job_inputs_assay_handler
    ):
        mock_fixed_inputs.return_value = {
            "tsv_input": [
                {"$dnanexus_link": {"field": "metricsOutput", "job": "job_id"}}
            ]
        }
        params = job_inputs_assay_handler.config["executables"][
            "app-GjF8gFQ4yyVBg4Y62BV10Vp2"
        ]
        job_inputs_assay_handler.job_outputs = {"analysis_1": "job_id"}

        job_inputs_assay_handler.build_job_inputs(
            "app-GjF8gFQ4yyVBg4Y62BV10Vp2", params
        )

        expected_output = {
            "app-GjF8gFQ4yyVBg4Y62BV10Vp2": {
                "dependent_jobs": ["job_id"],
                "job_name": "eggd_metricsoutput_editor-v1.1.0",
                "extra_args": {},
                "inputs": {
                    "tsv_input": [
                        {
                            "$dnanexus_link": {
                                "field": "metricsOutput",
                                "job": "job_id",
                            }
                        }
                    ]
                },
            }
        }

        assert expected_output == job_inputs_assay_handler.job_info_per_run

    @patch("utils.AssayHandler.AssayHandler.handle_TSO500_inputs")
    @patch("utils.AssayHandler.manage_dict.fix_invalid_inputs")
    def test_build_job_inputs_TSO500(
        self, mock_fixed_inputs, mock_handle_TSO500, job_inputs_assay_handler
    ):
        mock_fixed_inputs.return_value = self.mocked_fixed_inputs_json
        # mock the handling of TSO500 method so that test passes using Github
        # action
        mock_handle_TSO500.return_value = self.mocked_handle_TSO500
        params = job_inputs_assay_handler.config["executables"][
            "workflow-Gjk42k84yfKPv0x151ZvYBpK"
        ]
        # actual eggd_tso500 job id
        job_inputs_assay_handler.job_outputs = {
            "analysis_1": "job-Gqz41pQ4ZvYz723Py0X8jvgK"
        }
        # sample id used in the job id
        job_inputs_assay_handler.build_job_inputs(
            "workflow-Gjk42k84yfKPv0x151ZvYBpK", params, "132516078-24261S0023"
        )

        expected_output = self.expected_output

        assert expected_output == job_inputs_assay_handler.job_info_per_sample
