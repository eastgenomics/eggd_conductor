import os

import pytest

from .settings import TEST_DATA_DIR
from utils.utils import (
    exclude_samples,
    select_instance_types,
    match_samples_to_assays,
    parse_sample_sheet,
    parse_run_info_xml,
)


class TestSelectInstanceTypes:
    instance_types = {
        "*": {"default_instances": ""},
        "S1": {"S1_instances_from_S1": ""},
        "S2": {"S2_instances_from_S2": ""},
        "S4": {"S4_instances_from_S4": ""},
        "xxxxxDRxx": {"SP_S1_instances_from_DR_pattern": ""},
        "xxxxxDMxx": {"S2_instances_from_DM_pattern": ""},
        "xxxxxDSxx": {"S4_instances_from_DS_pattern": ""},
        "Kxxxx": {"MiSeq_instances_from_K_pattern": ""},
    }

    def test_select_S1(self):
        """
        Tests that S1 instances can be correctly selected from an S1 flowcell
        ID pattern when the xxxxxDMxx is defined in the instance type dict
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDRXY",
            instance_types=self.instance_types,
        )

        correct_dict = {"SP_S1_instances_from_DR_pattern": ""}

        assert (
            selected_instance_types == correct_dict
        ), "wrong instances type selected for xxxxxDRxx flowcell"

    def test_select_S2(self):
        """
        Tests that S2 instances can be correctly selected from an S2 flowcell
        ID pattern where defined in instance type dict as S2
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDMXY",
            instance_types=self.instance_types,
        )

        correct_dict = {"S2_instances_from_DM_pattern": ""}

        assert (
            selected_instance_types == correct_dict
        ), "wrong instances type selected for xxxxxDMxx flowcell"

    def test_select_S4(self):
        """
        Tests that S4 instances can be correctly selected from an S4 flowcell
        ID pattern when the xxxxxDSxx is defined in the instance type dict
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLDSXY",
            instance_types=self.instance_types,
        )

        correct_dict = {"S4_instances_from_DS_pattern": ""}

        assert (
            selected_instance_types == correct_dict
        ), "wrong instances type selected for xxxxxDSxx flowcell"

    def test_select_S1_from_S1(self):
        """
        Test where S1 is in the instance types dict and xxxxxDRxx is not, that
        the dict for S1 is correctly selected
        """
        instances = self.instance_types.copy()
        instances.pop("xxxxxDRxx")

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDRXY", instance_types=instances
        )

        correct_dict = {"S1_instances_from_S1": ""}

        assert (
            selected_instance_types == correct_dict
        ), "wrong instances type selected for S1"

    def test_select_S2_from_S2(self):
        """
        Test where S2 is in the instance types dict and xxxxxDMxx is not, that
        the dict for S2 is correctly selected
        """
        instances = self.instance_types.copy()
        instances.pop("xxxxxDMxx")

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDMXY", instance_types=instances
        )

        correct_dict = {"S2_instances_from_S2": ""}

        assert (
            selected_instance_types == correct_dict
        ), "wrong instances type selected for S2"

    def test_select_S4_from_S4(self):
        """
        Test where S4 is in the instance types dict and xxxxxDSxx is not, that
        the dict for S4 is correctly selected
        """
        instances = self.instance_types.copy()
        instances.pop("xxxxxDSxx")

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDSXY", instance_types=instances
        )

        correct_dict = {"S4_instances_from_S4": ""}

        assert (
            selected_instance_types == correct_dict
        ), "wrong instances type selected for S4"

    def test_default_instance_set_used(self):
        """
        Test that when the flowcell ID matches none of the given patterns
        in the defined instance type dict, the default set is used ("*")
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYAAAXY",
            instance_types=self.instance_types,
        )

        correct_dict = {"default_instances": ""}

        assert (
            selected_instance_types == correct_dict
        ), "Incorrect default instances used"

    def test_return_none_with_no_default(self):
        """
        Test where the flowcell ID matches none of the given pattern and
        no default is provided, that None is returned which will cause
        dxpy to just use the defaults set by the app / workflow
        """
        instances = self.instance_types.copy()
        instances.pop("*")

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYAAAXY", instance_types=instances
        )

        assert (
            selected_instance_types is None
        ), "None not returned for no match"

    def test_miseq_pattern(self):
        """
        Test where flowcell ID is for MiSeq run and Kxxxx pattern is
        provided in the instance types it matches
        """
        selected_instance_types = select_instance_types(
            run_id="230201_M03595_0015_000000000-KRW44",
            instance_types=self.instance_types,
        )

        correct_dict = {"MiSeq_instances_from_K_pattern": ""}

        assert (
            selected_instance_types == correct_dict
        ), "Wrong instance types selected fro MiSeq K pattern"

    def test_return_string(self):
        """
        Test where flowcell value is a string (i.e. for an app and not
        workflow stages)
        """
        instance_types = {
            "S1": "mem2_ssd1_v2_x16",
            "S2": "mem2_ssd1_v2_x48",
            "S4": "mem2_ssd1_v2_x96",
        }

        selected_instance = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDRXY",
            instance_types=instance_types,
        )

        assert (
            selected_instance == "mem2_ssd1_v2_x16"
        ), "Wrong instance type returned where return type should be a string"


def test_parse_sample_sheet():
    """
    Test that sample list can be parsed correctly from the samplesheet
    """
    parsed_sample_list = parse_sample_sheet(
        os.path.join(TEST_DATA_DIR, "SampleSheet.csv")
    )

    correct_sample_list = [f"sample{x}" for x in range(1, 49)]

    assert (
        parsed_sample_list == correct_sample_list
    ), "list of samples wrongly parsed from samplesheet"


def test_parse_run_info_xml():
    """
    Test that run ID is correctly parsed from RunInfo.xml file
    """
    parsed_run_id = parse_run_info_xml(
        os.path.join(TEST_DATA_DIR, "RunInfo.xml")
    )

    correct_run_id = "220920_A01303_0099_AHGNJNDRX2"

    assert (
        parsed_run_id == correct_run_id
    ), "run ID not correctly parsed from RunInfo.xml"


class TestMatchSamplesToAssays:
    """
    Tests for match_samples_to_assays()
    """

    # minimal example of dict of configs that would be returned from
    # get_json_configs() and filter_highest_config_version()
    configs = {
        "EGG2|LAB123": {"assay_code": "EGG2|LAB123", "version": "1.2.0"},
        "EGG3|LAB456": {"assay_code": "EGG3|LAB456", "version": "1.1.0"},
        "EGG4": {"assay_code": "EGG4", "version": "1.0.1"},
        "EGG5": {"assay_code": "EGG5", "version": "1.1.1"},
        "EGG6": {"assay_code": "EGG6", "version": "1.2.1"},
    }

    # test lists of samples as would be parsed from samplesheet
    single_assay_sample_list = [f"sample{x}-EGG2" for x in range(1, 11)]
    mixed_assay_sample_list = single_assay_sample_list + ["sample11-EGG3"]
    sample_list_w_no_code = single_assay_sample_list + ["sample11"]

    def test_return_single_assay(self):
        """
        Test that when all samples are for one assay that they are matched
        to the correct assay code and returned
        """
        assay_samples = match_samples_to_assays(
            configs=self.configs,
            all_samples=self.single_assay_sample_list,
            testing=False,
        )

        correct_output = {
            "EGG2|LAB123": [
                "sample1-EGG2",
                "sample2-EGG2",
                "sample3-EGG2",
                "sample4-EGG2",
                "sample5-EGG2",
                "sample6-EGG2",
                "sample7-EGG2",
                "sample8-EGG2",
                "sample9-EGG2",
                "sample10-EGG2",
            ]
        }

        assert (
            assay_samples == correct_output
        ), "Incorrectly matched samples to assay codes"

    def test_selected_highest_version(self):
        """
        Test that when matching samples to assays and multiple configs match,
        that the config wiht highest version is used
        """
        configs = {
            "EGG2|LAB123": {"assay_code": "EGG2|LAB123", "version": "1.0.0"},
            "EGG2|LAB123-2": {
                "assay_code": "EGG3|LAB456-2",
                "version": "1.2.0",
            },
            "EGG2|LAB123-3": {
                "assay_code": "EGG2|LAB123-3",
                "version": "1.11.0",
            },
        }

        # samples have EGG2 in name so will match all the configs, 1.11.0
        # should be selected
        matches = match_samples_to_assays(
            configs=configs,
            all_samples=self.single_assay_sample_list,
            testing=False,
        )

        assert list(matches.keys()) == [
            "EGG2|LAB123-3"
        ], "Wrong version of config file selected when matching to samples"

    def test_select_highest_version_w_lower_code(self):
        """
        Test when matching samples to assay configs that the highest version
        is kept when the assay codes might be 'smaller'

        Test added to check for fix for the following issue:
            https://github.com/eastgenomics/eggd_conductor/issues/80
        """
        configs = {
            "EGG2|456": {"assay_code": "EGG2|456", "version": "1.1.0"},
            "EGG2|123": {"assay_code": "EGG2|123", "version": "1.2.0"},
        }

        matches = match_samples_to_assays(
            configs=configs,
            all_samples=self.single_assay_sample_list,
            testing=False,
        )

        assert list(matches.keys()) == [
            "EGG2|123"
        ], "Wrong version of config file selected when matching to samples"

    def test_mismatch_set_zero(self):
        """
        Tests when missing assay code for sample to assay matching occurs
        and mismatch set to zero (i.e. not allowed) and should raise an
        AssertionError
        """
        sample_list = [
            "sample1-EGG2",
            "sample2-EGG2",
            "sample3-EGG2",
            "sample4",
        ]

        with pytest.raises(AssertionError):
            # this should raise an AssertionError as normal for mismatch
            # between total samples and those matching assay config
            match_samples_to_assays(
                configs=self.configs, all_samples=sample_list, testing=False
            )

    def test_raise_assertion_error_on_mixed_assays(self):
        """
        Test that an AssertionError is raised when more than one assay
        code is identified in the sample list
        """
        pass

    def test_raise_assertion_error_on_sample_w_no_assay_code_match(self):
        """
        Test that an AssertionError is raised when a sample has no assay
        code in the name matching one of the found config files
        """
        with pytest.raises(AssertionError):
            match_samples_to_assays(
                configs=self.configs,
                all_samples=self.sample_list_w_no_code,
                testing=False,
            )


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (
            ["1100000-11000R1100-FJE9340-5678-M-123456"],
            [
                "1000000-10000R1000-FJE9340-1234-F-123456",
                "2000000-20000R2000-FJE9340-1234-M-123456",
                "3000000-30000R3000-FJE9340-1234-F-123456",
                "4000000-40000R4000-FJE9340-1234-F-123456",
                "5000000-50000R5000-FJE9340-1234-M-123456",
                "6000000-60000R6000-FJE9340-1234-M-123456",
                "7000000-70000R7000-FJE9340-1234-M-123456",
                "8000000-80000R8000-FJE9340-1234-F-123456",
                "9000000-90000R9000-FJE9340-1234-M-123456",
                "1200000-12000R1200-FJE9340-5678-F-123456",
                "1300000-13000R1300-FJE9340-9101-M-123456",
                "1400000-14000R1400-FJE9340-9101-M-123456",
            ],
        ),
        (
            [
                "1000000-10000R1000-FJE9340-1234-F-123456",
                "2000000-20000R2000-FJE9340-1234-M-123456",
            ],
            [
                "3000000-30000R3000-FJE9340-1234-F-123456",
                "4000000-40000R4000-FJE9340-1234-F-123456",
                "5000000-50000R5000-FJE9340-1234-M-123456",
                "6000000-60000R6000-FJE9340-1234-M-123456",
                "7000000-70000R7000-FJE9340-1234-M-123456",
                "8000000-80000R8000-FJE9340-1234-F-123456",
                "9000000-90000R9000-FJE9340-1234-M-123456",
                "1100000-11000R1100-FJE9340-5678-M-123456",
                "1200000-12000R1200-FJE9340-5678-F-123456",
                "1300000-13000R1300-FJE9340-9101-M-123456",
                "1400000-14000R1400-FJE9340-9101-M-123456",
            ],
        ),
        (
            [],
            [
                "1000000-10000R1000-FJE9340-1234-F-123456",
                "2000000-20000R2000-FJE9340-1234-M-123456",
                "3000000-30000R3000-FJE9340-1234-F-123456",
                "4000000-40000R4000-FJE9340-1234-F-123456",
                "5000000-50000R5000-FJE9340-1234-M-123456",
                "6000000-60000R6000-FJE9340-1234-M-123456",
                "7000000-70000R7000-FJE9340-1234-M-123456",
                "8000000-80000R8000-FJE9340-1234-F-123456",
                "9000000-90000R9000-FJE9340-1234-M-123456",
                "1100000-11000R1100-FJE9340-5678-M-123456",
                "1200000-12000R1200-FJE9340-5678-F-123456",
                "1300000-13000R1300-FJE9340-9101-M-123456",
                "1400000-14000R1400-FJE9340-9101-M-123456",
            ],
        ),
        (
            ["-1234-", "-5678-", "1400000-14000R1400-FJE9340-9101-M-123456"],
            ["1300000-13000R1300-FJE9340-9101-M-123456"],
        ),
    ],
)
def test_exclude_samples(test_input, expected):
    samples = [
        "1000000-10000R1000-FJE9340-1234-F-123456",
        "2000000-20000R2000-FJE9340-1234-M-123456",
        "3000000-30000R3000-FJE9340-1234-F-123456",
        "4000000-40000R4000-FJE9340-1234-F-123456",
        "5000000-50000R5000-FJE9340-1234-M-123456",
        "6000000-60000R6000-FJE9340-1234-M-123456",
        "7000000-70000R7000-FJE9340-1234-M-123456",
        "8000000-80000R8000-FJE9340-1234-F-123456",
        "9000000-90000R9000-FJE9340-1234-M-123456",
        "1100000-11000R1100-FJE9340-5678-M-123456",
        "1200000-12000R1200-FJE9340-5678-F-123456",
        "1300000-13000R1300-FJE9340-9101-M-123456",
        "1400000-14000R1400-FJE9340-9101-M-123456",
    ]

    output = exclude_samples(samples, patterns=test_input)

    assert sorted(expected) == sorted(output) and len(expected) == len(
        output
    ), "Unexpected samples kept"
