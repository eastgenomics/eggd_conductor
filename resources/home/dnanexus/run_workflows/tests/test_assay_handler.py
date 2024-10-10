from datetime import datetime
import os
import pathlib
import re
from unittest import mock
from unittest.mock import patch

import dxpy as dx
import pytest

from utils.AssayHandler import AssayHandler


@pytest.fixture()
def empty_assay_handler():
    assay_handler = AssayHandler({})
    yield assay_handler
    del assay_handler


@pytest.fixture()
def normal_assay_handler():
    config = {"assay_code": "code1", "assay": "assay1", "version": "v1"}
    assay_handler = AssayHandler(config)
    yield assay_handler
    del assay_handler


class TestAssayHandler:
    def test_correct_setup(self, normal_assay_handler):
        assert (
            normal_assay_handler.assay_code == "code1" and
            normal_assay_handler.assay == "assay1" and
            normal_assay_handler.version == "v1"
        ), "Configuration of Assay Handler incorrect"

    @pytest.mark.parametrize(
        "test_input, expected", [(1, 1), (4, 4), (5, 5), (0, 10)]
    )
    def test_limiting_number_samples(
        self, test_input, expected, empty_assay_handler
    ):
        empty_assay_handler.samples = [
            "sample1", "sample2", "sample3", "sample4", "sample5",
            "sample6", "sample7", "sample8", "sample9", "sample10"
        ]

        empty_assay_handler.limit_samples(limit_nb=test_input)

        assert expected == len(empty_assay_handler.samples), (
            "Unexpected number samples kept"
        )

    @pytest.mark.parametrize("test_input, expected", [
            (
                ["sample1"],
                [
                    "sample2", "sample3", "sample4", "sample5", "sample6",
                    "sample7", "sample8", "sample9", "sample10"
                ]
            ),
            (
                ["sample1", "sample2"],
                [
                    "sample3", "sample4", "sample5", "sample6",
                    "sample7", "sample8", "sample9", "sample10"
                ]
            ),
            (
                [""],
                [
                    "sample1", "sample2", "sample3", "sample4", "sample5",
                    "sample6", "sample7", "sample8", "sample9", "sample10"
                ]
            )
        ]
    )
    def test_limiting_using_sample_names(
        self, test_input, expected, empty_assay_handler
    ):
        empty_assay_handler.samples = [
            "sample1", "sample2", "sample3", "sample4", "sample5",
            "sample6", "sample7", "sample8", "sample9", "sample10"
        ]

        empty_assay_handler.limit_samples(samples_to_exclude=test_input)

        assert (
            expected == empty_assay_handler.samples and
            len(expected) == len(empty_assay_handler.samples)
        ), "Unexpected samples kept"

    def test_subset_with_missing_subset_samplesheet(
        self, normal_assay_handler
    ):
        normal_assay_handler.samples = ["sample1", "sample2"]

        normal_assay_handler.subset_samples()

        assert normal_assay_handler.samples == ["sample1", "sample2"], (
            "Samples have changed even without a subset samplesheet"
        )

    def test_subset_with_incorrect_pattern(self):
        config = {"subset_samplesheet": "++"}
        assay_handler = AssayHandler(config)

        with pytest.raises(
            re.error, match="Invalid subset pattern provided"
        ):
            assay_handler.subset_samples()

    def test_subset_no_samples_left(self):
        config = {"subset_samplesheet": "not_sample"}
        assay_handler = AssayHandler(config)
        assay_handler.samples = ["sample1", "sample2"]

        with pytest.raises(
            AssertionError, match=(
                "No samples left after filtering using pattern not_sample"
            )
        ):
            assay_handler.subset_samples()

    def test_subset_some_samples(self):
        config = {"subset_samplesheet": "1"}
        assay_handler = AssayHandler(config)
        assay_handler.samples = ["sample1", "sample2", "sample10"]

        assay_handler.subset_samples()

        assert assay_handler.samples == ["sample1", "sample10"], (
            "Did not remove sample2 as expected"
        )

    @patch('utils.AssayHandler.dx.bindings.dxproject.DXProject')
    @patch('utils.AssayHandler.find_dx_project')
    def test_get_dx_project(
        self, mock_project_id, mock_project_obj, normal_assay_handler
    ):
        mock_project_id.return_value = "project-id1"

        test_inputs = {
            "project_name": "002_run1_assay1", "run_id": "run1"
        }

        normal_assay_handler.get_or_create_dx_project(**test_inputs)

        assert mock_project_obj.call_count == 1, (
            "Expected one call to DXProject when getting the DXProject, got "
            f"'{mock_project_obj.call_count}'"
        )

    @patch('utils.AssayHandler.dx.bindings.dxproject.DXProject')
    @patch('utils.AssayHandler.find_dx_project')
    def test_create_dx_project(
        self, mock_project_id, mock_project_obj, normal_assay_handler
    ):
        mock_project_id.return_value = None
        mock_project_obj.return_value.new.return_value = "project_id1"

        test_inputs = {
            "project_name": "002_run1_assay1", "run_id": "run1"
        }

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

        assert empty_assay_handler.upload_tars is None, (
            "Upload_tars is not None"
        )

    @patch("utils.AssayHandler.dx.bindings.dxrecord.DXRecord")
    def test_get_upload_tars_correct_sentinel_file(
        self, mock_record, empty_assay_handler
    ):
        mock_record.return_value = dx.bindings.dxrecord.DXRecord
        mock_record.return_value.describe.return_value = {
            "details": {"tar_file_ids": [
                "file_id1",
                "file_id2",
                "file_id3",
                "file_id4",
                "file_id5",
            ]}
        }

        empty_assay_handler.get_upload_tars("thing_that_returns_true")

        assert empty_assay_handler.upload_tars == [
            {"$dnanexus_link": "file_id1"},
            {"$dnanexus_link": "file_id2"},
            {"$dnanexus_link": "file_id3"},
            {"$dnanexus_link": "file_id4"},
            {"$dnanexus_link": "file_id5"}
        ]

    def test_set_parent_out_dir(self, normal_assay_handler):
        run_time = datetime.now().strftime("%y%m%d_%H%M")
        mock_os_environment = mock.patch.dict(os.environ, {"DESTINATION": "PROJECT-ID"})
        mock_os_environment.start()

        normal_assay_handler.set_parent_out_dir(run_time)

        mock_os_environment.stop()
        expected_output = f"PROJECT-ID/output/{normal_assay_handler.assay}-{run_time}"
        assert normal_assay_handler.parent_out_dir == expected_output

    @patch("utils.AssayHandler.Slack.send")
    def test_get_executable_names_per_config_invalid_dx_executable(
        self, mock_slack_send, normal_assay_handler
    ):
        normal_assay_handler.config["executables"] = {
            "app-id": "value",
            "applet-id": "value",
            "invalid_workflow-id": "value"
        }

        with pytest.raises(AssertionError):
            normal_assay_handler.get_executable_names_per_config()
            assert mock_slack_send.call_args == (
                f'Executable(s) from the config not valid: '
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
                    {
                        "id": "stage-id1",
                        "executable": "applet-id"
                    },
                    {
                        "id": "stage-id2",
                        "executable": "app-id"
                    },
                ],
            },
            {
                "name": "applet-name"
            },
            {
                "name": "app-name"
            }
        ]

        executable_assay_handler.get_executable_names_per_config()

        assert executable_assay_handler.execution_mapping == {
            "workflow-id": {
                "name": "workflow-name",
                "stages": {
                    "stage-id1": "applet-name",
                    "stage-id2": "id"
                }
            },
            "app-id": {
                "name": "name"
            }
        }

        assert mock_describe.call_count == 3

    @patch("utils.AssayHandler.dx.describe")
    def test_get_input_classes_per_config(
        self, mock_describe, executable_assay_handler
    ):
        mock_describe.side_effect = [
            {
                "inputSpec": [{
                    "name": "input_name1",
                    "class": "input_class1",
                    "optional": True
                }]
            },
            {
                "inputSpec": [{
                    "name": "input_name2",
                    "class": "input_class2",
                }]
            }
        ]

        executable_assay_handler.get_input_classes_per_config()

        assert executable_assay_handler.input_class_mapping == {
            "workflow-id": {
                "input_name1": {
                    "class": "input_class1",
                    "optional": True
                }
            },
            "app-id": {
                "input_name2": {
                    "class": "input_class2",
                    "optional": False
                }
            }
        }
