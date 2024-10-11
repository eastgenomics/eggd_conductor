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
    with open(
        "resources/home/dnanexus/run_workflows/tests/data/tso500_config.json"
    ) as f:
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
        mock_os_environment = mock.patch.dict(
            os.environ, {"DESTINATION": "PROJECT-ID"}
        )
        mock_os_environment.start()

        normal_assay_handler.set_parent_out_dir(run_time)

        mock_os_environment.stop()
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

    @patch("utils.AssayHandler.manage_dict.fix_invalid_inputs")
    def test_build_job_inputs_TSO500(
        self, mock_fixed_inputs, job_inputs_assay_handler
    ):
        mock_fixed_inputs.return_value = {
            "stage-athena.cutoff_threshold": 100,
            "stage-athena.exons_file": {
                "$dnanexus_link": {
                    "id": "file-Fq18Yp0433GjB7172630p9Yv",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-athena.name": "132516078-24261S0023",
            "stage-athena.panel_bed": {
                "$dnanexus_link": {
                    "id": "file-G4F6jX04ZFVV3JZJG62ZQ5yJ",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-athena.panel_filters": "Glioma_PanCan:ALK,ATRX,BCOR,BRAF,CDKN2A,CDKN2B,CTNNB1,DDX3X,DICER1,EZH2,FGFR1,FGFR4,H3-3A,H3-3B,H3C14,H3C2,H3C3,HIST1H3B,IDH1,IDH2,KIT,MSH6,MYCN,NF1,NRAS,PHOX2B,PIK3CA,PMS2,PTCH1,PTCH2,PTEN,RAF1,RB1,SMARCA4,SMARCB1,SMO,SUFU,TERT,TP53,TSC1,TSC2,VHL,YAP1,EGFR Prostate:BRCA1,BRCA2,ATM,CDK12,AR,PTEN,RAD51B,KRAS,GNAS,PIK3CA,TP53,PTEN,ATM,CCND1,FANCA,FANCC,FANCG,RAD50,STK11,CHEK1,CHEK2,ERBB2,PALB2,CDKN2A Melanoma:NRAS,BRAF,KIT,MYB,RREB1,CCND1,MYC,CDKN2A,ARID2,ATM,CDK12,CDKN2A,FGFR1,FGFR2,FGFR3,IDH1,KRAS,MAP2K1,MTOR,NOTCH2,NOTCH4,PDGFRA,PTEN,RB1,SF3B1,SMARCB1,TERT,BAP1,HRAS,MET,GNA11,GNAQ,GNAS,NF1,CCND1,CDK4,FGFR1,FGFR3,KRAS,MDM2,NOTCH2,NOTCH4,PDGFRA,SMARCB1,MET Colon:KRAS,NRAS,BRAF,MLH1,MSH2,MSH6,PMS2,POLD1,POLE,ATM,CHEK1,FGFR2,FGFR3,PALB2,APC,SMAD4,FBXW7,ARID1A,RNF43,PTEN,B2N,PIK3R1,GNAS,ARID1B,BRCA2,AMER1,CREBBP,HRAS,PIK3CA,TP53,RET,ROS1,ERBB2,ERBB3,UGT1 Ovarian:BRCA1,BRCA2,SMARCA4,AKT1,ATM,ATR,CDK12,CHEK1,CHEK2,BARD1,BRIP1,ARID1A,CTNNB1,FANCL,NF1,TP53,MEK,EMSY,RB1,PALB2,PPP2R2A,PTEN,RAD54L,RAD51B,RAD51D,RAD51C,KRAS,NRAS,HRAS,BRAF Endometrial:MLH1,MSH2,MSH6,PMS2,POLE,POLD1,PIK3CA,FGFR2,FGFR3,TP53,ERBB2,ERBB2 Sarcoma:IDH1,IDH2,APC,CTNNB1,GNAS,H3-3A,H3-3B GIST:KIT,PDGFRA,NF1,SDHA,SDHB,SDHC,SDHD,SDHAF2 AFX_PDM:TP53,CDKN2A,TERT,NOTCH1,TMB,MSI,ASXL1,HRAS,KNSTRN,PIK3CA Phaeo_Para:BRAF,EPAS1,FH,HRAS,IDH1,KRAS,MAX,NF1,PTEN,RET,SDHA,SDHAF2,SDHB,SDHC,SDHD,TMEM127,TP53,VHL,SLC25A11,DNMT3A,DAXX,ATRX,MDH2,GOT2,DLST Histiocytosis:BRAF,MAP2K1,NRAS,KRAS,HRAS,ERBB3,ARAF,MAP3K1,PIK3CA,PIK3CD RENAL:CHR_8,CHR_7,CHR_17,FH,SDHA,SDHB,SDHC,SDHD,VHL,ELOC,TSC1,MET,BRAF,TSC2,TCEB1,MET,RET,MTOR,FLCN",
            "stage-athena.per_chromosome_coverage": True,
            "stage-athena.summary": True,
            "stage-athena.thresholds": "50,100,150,250",
            "stage-eggd_add_MANE_annotation.transcript_file": {
                "$dnanexus_link": {
                    "id": "file-Gg5p2J04qJpk3GyJXjFyby66",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-generate_variant_workbook.add_classification_column": True,
            "stage-generate_variant_workbook.add_comment_column": True,
            "stage-generate_variant_workbook.add_raw_change": True,
            "stage-generate_variant_workbook.additional_columns": "oncoKB PeCan cBioPortal",
            "stage-generate_variant_workbook.additional_files": [
                {"$dnanexus_link": "file-Gqz5k0j4K7y0KKKV5kzGvz09"},
                {"$dnanexus_link": "file-Gqz5k884K7y67JpQQ7bK79bP"},
            ],
            "stage-generate_variant_workbook.colour_cells": "VF:>=0.9:#2ab600 VF:<0.9&>=0.8:#51bc00 VF:<0.8&>=0.7:#7bc100 VF:<0.7&>=0.6:#a7c700 VF:<0.6&>=0.5:#ccc300 VF:<0.5&>=0.4:#d29d00 VF:<0.4&>=0.3:#d77600 VF:<0.3&>=0.2:#dd4c00 VF:<0.2&>=0.1:#e22000 VF:<0.1:#e8000f",
            "stage-generate_variant_workbook.exclude_columns": "ID CSQ_Allele CSQ_Mastermind_MMID3 DP_FMT",
            "stage-generate_variant_workbook.filter": 'bcftools filter -e \'INFO/DP==0 | CSQ_gnomADe_AF > 0.01 | CSQ_gnomADg_AF > 0.01 | VF < 0.05 | CSQ_Consequence=="intron_variant&non_coding_transcript_variant" | CSQ_Consequence=="non_coding_transcript_exon_variant" | CSQ_Consequence=="3_prime_UTR_variant" | CSQ_Consequence=="5_prime_UTR_variant" | CSQ_Consequence=="downstream_gene_variant" | CSQ_Consequence=="intron_variant" | CSQ_Consequence=="splice_region_variant&intron_variant" | CSQ_Consequence=="splice_region_variant&synonymous_variant" | CSQ_Consequence=="synonymous_variant" | (CSQ_SYMBOL != "TERT" & CSQ_Consequence=="upstream_gene_variant")\'',
            "stage-generate_variant_workbook.freeze_column": "H2",
            "stage-generate_variant_workbook.keep_tmp": True,
            "stage-generate_variant_workbook.reorder_columns": "CSQ_SYMBOL CSQ_Consequence CSQ_Feature DNA Protein VF Classification Comment rawChange MANE CHROM POS REF ALT CSQ_EXON FILTER QUAL DP oncoKB PeCan cBioPortal CSQ_INTRON CSQ_IMPACT  CSQ_CADD_PHRED CSQ_REVEL CSQ_gnomADe_AF CSQ_gnomADg_AF CSQ_ClinVar CSQ_ClinVar_CLNSIG CSQ_ClinVar_CLNDN CSQ_SpliceAI_pred_DS_AG CSQ_SpliceAI_pred_DS_AL CSQ_SpliceAI_pred_DS_DG CSQ_SpliceAI_pred_DS_DL CSQ_SpliceAI_pred_DP_AG CSQ_SpliceAI_pred_DP_AL CSQ_SpliceAI_pred_DP_DG CSQ_SpliceAI_pred_DP_DL",
            "stage-generate_variant_workbook.split_hgvs": True,
            "stage-generate_variant_workbook.summary": "helios",
            "stage-mosdepth.bam": {
                "$dnanexus_link": "file-Gqz5jj04K7yJF8yg48z8J86x"
            },
            "stage-mosdepth.bed": {
                "$dnanexus_link": {
                    "id": "file-FkkZQ1Q433GYXZ892pzkgvbP",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-mosdepth.index": {
                "$dnanexus_link": "file-Gqz5jv04K7y0286Y3zG4Kbg5"
            },
            "stage-mosdepth.mosdepth_docker": {
                "$dnanexus_link": {
                    "id": "file-GbJXzq04pgpY6FX22Qvk9F9x",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-vcf_rescue.fasta_tar": {
                "$dnanexus_link": {
                    "id": "file-F3zxG0Q4fXX9YFjP1v5jK9jf",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-vcf_rescue.gvcf": {
                "$dnanexus_link": "file-Gqz5jy84K7y16XBzb1xK5P6B"
            },
            "stage-vcf_rescue.rescue_non_pass": True,
            "stage-vcf_rescue.rescue_vcf": {
                "$dnanexus_link": {
                    "id": "file-GpVgQk04949qzzZk4FJ0ZQp7",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-vcf_rescue.strip_chr": True,
            "stage-vep.config_file": {
                "$dnanexus_link": {
                    "id": "file-GqZg6VQ40P9187pkKqFB653P",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
            "stage-vep.transcript_list": {
                "$dnanexus_link": {
                    "id": "file-Gqpgx7Q45bJP8bBbJb3KBJyJ",
                    "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                }
            },
        }
        params = job_inputs_assay_handler.config["executables"][
            "app-GjF8gFQ4yyVBg4Y62BV10Vp2"
        ]
        # actual eggd_tso500 job id
        job_inputs_assay_handler.job_outputs = {
            "analysis_1": "job-Gqz41pQ4ZvYz723Py0X8jvgK"
        }
        # sample id used in the job id
        job_inputs_assay_handler.build_job_inputs(
            "workflow-Gjk42k84yfKPv0x151ZvYBpK", params, "132516078-24261S0023"
        )

        expected_output = {
            "132516078-24261S0023": {
                "workflow-Gjk42k84yfKPv0x151ZvYBpK": {
                    "job_name": "TSO500_reports_workflow_v2.0.0-132516078-24261S0023",
                    "dependent_jobs": ["job-Gqz41pQ4ZvYz723Py0X8jvgK"],
                    "extra_args": {},
                    "inputs": {
                        "stage-athena.cutoff_threshold": 100,
                        "stage-athena.exons_file": {
                            "$dnanexus_link": {
                                "id": "file-Fq18Yp0433GjB7172630p9Yv",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-athena.name": "132516078-24261S0023",
                        "stage-athena.panel_bed": {
                            "$dnanexus_link": {
                                "id": "file-G4F6jX04ZFVV3JZJG62ZQ5yJ",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-athena.panel_filters": "Glioma_PanCan:ALK,ATRX,BCOR,BRAF,CDKN2A,CDKN2B,CTNNB1,DDX3X,DICER1,EZH2,FGFR1,FGFR4,H3-3A,H3-3B,H3C14,H3C2,H3C3,HIST1H3B,IDH1,IDH2,KIT,MSH6,MYCN,NF1,NRAS,PHOX2B,PIK3CA,PMS2,PTCH1,PTCH2,PTEN,RAF1,RB1,SMARCA4,SMARCB1,SMO,SUFU,TERT,TP53,TSC1,TSC2,VHL,YAP1,EGFR Prostate:BRCA1,BRCA2,ATM,CDK12,AR,PTEN,RAD51B,KRAS,GNAS,PIK3CA,TP53,PTEN,ATM,CCND1,FANCA,FANCC,FANCG,RAD50,STK11,CHEK1,CHEK2,ERBB2,PALB2,CDKN2A Melanoma:NRAS,BRAF,KIT,MYB,RREB1,CCND1,MYC,CDKN2A,ARID2,ATM,CDK12,CDKN2A,FGFR1,FGFR2,FGFR3,IDH1,KRAS,MAP2K1,MTOR,NOTCH2,NOTCH4,PDGFRA,PTEN,RB1,SF3B1,SMARCB1,TERT,BAP1,HRAS,MET,GNA11,GNAQ,GNAS,NF1,CCND1,CDK4,FGFR1,FGFR3,KRAS,MDM2,NOTCH2,NOTCH4,PDGFRA,SMARCB1,MET Colon:KRAS,NRAS,BRAF,MLH1,MSH2,MSH6,PMS2,POLD1,POLE,ATM,CHEK1,FGFR2,FGFR3,PALB2,APC,SMAD4,FBXW7,ARID1A,RNF43,PTEN,B2N,PIK3R1,GNAS,ARID1B,BRCA2,AMER1,CREBBP,HRAS,PIK3CA,TP53,RET,ROS1,ERBB2,ERBB3,UGT1 Ovarian:BRCA1,BRCA2,SMARCA4,AKT1,ATM,ATR,CDK12,CHEK1,CHEK2,BARD1,BRIP1,ARID1A,CTNNB1,FANCL,NF1,TP53,MEK,EMSY,RB1,PALB2,PPP2R2A,PTEN,RAD54L,RAD51B,RAD51D,RAD51C,KRAS,NRAS,HRAS,BRAF Endometrial:MLH1,MSH2,MSH6,PMS2,POLE,POLD1,PIK3CA,FGFR2,FGFR3,TP53,ERBB2,ERBB2 Sarcoma:IDH1,IDH2,APC,CTNNB1,GNAS,H3-3A,H3-3B GIST:KIT,PDGFRA,NF1,SDHA,SDHB,SDHC,SDHD,SDHAF2 AFX_PDM:TP53,CDKN2A,TERT,NOTCH1,TMB,MSI,ASXL1,HRAS,KNSTRN,PIK3CA Phaeo_Para:BRAF,EPAS1,FH,HRAS,IDH1,KRAS,MAX,NF1,PTEN,RET,SDHA,SDHAF2,SDHB,SDHC,SDHD,TMEM127,TP53,VHL,SLC25A11,DNMT3A,DAXX,ATRX,MDH2,GOT2,DLST Histiocytosis:BRAF,MAP2K1,NRAS,KRAS,HRAS,ERBB3,ARAF,MAP3K1,PIK3CA,PIK3CD RENAL:CHR_8,CHR_7,CHR_17,FH,SDHA,SDHB,SDHC,SDHD,VHL,ELOC,TSC1,MET,BRAF,TSC2,TCEB1,MET,RET,MTOR,FLCN",
                        "stage-athena.per_chromosome_coverage": True,
                        "stage-athena.summary": True,
                        "stage-athena.thresholds": "50,100,150,250",
                        "stage-eggd_add_MANE_annotation.transcript_file": {
                            "$dnanexus_link": {
                                "id": "file-Gg5p2J04qJpk3GyJXjFyby66",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-generate_variant_workbook.add_classification_column": True,
                        "stage-generate_variant_workbook.add_comment_column": True,
                        "stage-generate_variant_workbook.add_raw_change": True,
                        "stage-generate_variant_workbook.additional_columns": "oncoKB PeCan cBioPortal",
                        "stage-generate_variant_workbook.additional_files": [
                            {
                                "$dnanexus_link": "file-Gqz5k0j4K7y0KKKV5kzGvz09"
                            },
                            {
                                "$dnanexus_link": "file-Gqz5k884K7y67JpQQ7bK79bP"
                            },
                        ],
                        "stage-generate_variant_workbook.colour_cells": "VF:>=0.9:#2ab600 VF:<0.9&>=0.8:#51bc00 VF:<0.8&>=0.7:#7bc100 VF:<0.7&>=0.6:#a7c700 VF:<0.6&>=0.5:#ccc300 VF:<0.5&>=0.4:#d29d00 VF:<0.4&>=0.3:#d77600 VF:<0.3&>=0.2:#dd4c00 VF:<0.2&>=0.1:#e22000 VF:<0.1:#e8000f",
                        "stage-generate_variant_workbook.exclude_columns": "ID CSQ_Allele CSQ_Mastermind_MMID3 DP_FMT",
                        "stage-generate_variant_workbook.filter": 'bcftools filter -e \'INFO/DP==0 | CSQ_gnomADe_AF > 0.01 | CSQ_gnomADg_AF > 0.01 | VF < 0.05 | CSQ_Consequence=="intron_variant&non_coding_transcript_variant" | CSQ_Consequence=="non_coding_transcript_exon_variant" | CSQ_Consequence=="3_prime_UTR_variant" | CSQ_Consequence=="5_prime_UTR_variant" | CSQ_Consequence=="downstream_gene_variant" | CSQ_Consequence=="intron_variant" | CSQ_Consequence=="splice_region_variant&intron_variant" | CSQ_Consequence=="splice_region_variant&synonymous_variant" | CSQ_Consequence=="synonymous_variant" | (CSQ_SYMBOL != "TERT" & CSQ_Consequence=="upstream_gene_variant")\'',
                        "stage-generate_variant_workbook.freeze_column": "H2",
                        "stage-generate_variant_workbook.keep_tmp": True,
                        "stage-generate_variant_workbook.reorder_columns": "CSQ_SYMBOL CSQ_Consequence CSQ_Feature DNA Protein VF Classification Comment rawChange MANE CHROM POS REF ALT CSQ_EXON FILTER QUAL DP oncoKB PeCan cBioPortal CSQ_INTRON CSQ_IMPACT  CSQ_CADD_PHRED CSQ_REVEL CSQ_gnomADe_AF CSQ_gnomADg_AF CSQ_ClinVar CSQ_ClinVar_CLNSIG CSQ_ClinVar_CLNDN CSQ_SpliceAI_pred_DS_AG CSQ_SpliceAI_pred_DS_AL CSQ_SpliceAI_pred_DS_DG CSQ_SpliceAI_pred_DS_DL CSQ_SpliceAI_pred_DP_AG CSQ_SpliceAI_pred_DP_AL CSQ_SpliceAI_pred_DP_DG CSQ_SpliceAI_pred_DP_DL",
                        "stage-generate_variant_workbook.split_hgvs": True,
                        "stage-generate_variant_workbook.summary": "helios",
                        "stage-mosdepth.bam": {
                            "$dnanexus_link": "file-Gqz5jj04K7yJF8yg48z8J86x"
                        },
                        "stage-mosdepth.bed": {
                            "$dnanexus_link": {
                                "id": "file-FkkZQ1Q433GYXZ892pzkgvbP",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-mosdepth.index": {
                            "$dnanexus_link": "file-Gqz5jv04K7y0286Y3zG4Kbg5"
                        },
                        "stage-mosdepth.mosdepth_docker": {
                            "$dnanexus_link": {
                                "id": "file-GbJXzq04pgpY6FX22Qvk9F9x",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-vcf_rescue.fasta_tar": {
                            "$dnanexus_link": {
                                "id": "file-F3zxG0Q4fXX9YFjP1v5jK9jf",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-vcf_rescue.gvcf": {
                            "$dnanexus_link": "file-Gqz5jy84K7y16XBzb1xK5P6B"
                        },
                        "stage-vcf_rescue.rescue_non_pass": True,
                        "stage-vcf_rescue.rescue_vcf": {
                            "$dnanexus_link": {
                                "id": "file-GpVgQk04949qzzZk4FJ0ZQp7",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-vcf_rescue.strip_chr": True,
                        "stage-vep.config_file": {
                            "$dnanexus_link": {
                                "id": "file-GqZg6VQ40P9187pkKqFB653P",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                        "stage-vep.transcript_list": {
                            "$dnanexus_link": {
                                "id": "file-Gqpgx7Q45bJP8bBbJb3KBJyJ",
                                "project": "project-Fkb6Gkj433GVVvj73J7x8KbV",
                            }
                        },
                    },
                }
            }
        }

        assert expected_output == job_inputs_assay_handler.job_info_per_sample
