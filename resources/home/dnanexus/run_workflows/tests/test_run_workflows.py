import os
import pytest
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../../')
))

from tests import TEST_DATA_DIR
from run_workflows.run_workflows import (
    match_samples_to_assays, parse_run_info_xml, parse_sample_sheet)


def test_parse_sample_sheet():
    """
    Test that sample list can be parsed correctly from the samplesheet
    """
    parsed_sample_list = parse_sample_sheet(
        os.path.join(TEST_DATA_DIR, 'SampleSheet.csv')
    )

    correct_sample_list = [f'sample{x}' for x in range(1, 49)]

    assert parsed_sample_list == correct_sample_list, (
        'list of samples wrongly parsed from samplesheet'
    )


def test_parse_run_info_xml():
    """
    Test that run ID is correctly parsed from RunInfo.xml file
    """
    parsed_run_id = parse_run_info_xml(
        os.path.join(TEST_DATA_DIR, 'RunInfo.xml')
    )

    correct_run_id = '220920_A01303_0099_AHGNJNDRX2'

    assert parsed_run_id == correct_run_id, (
        'run ID not correctly parsed from RunInfo.xml'
    )


class TestMatchSamplesToAssays():
    """
    Tests for match_samples_to_assays()
    """
    # minimal example of dict of configs that would be returned from
    # DXManage.get_json_configs() and DXManage.filter_highest_config_version()
    configs = {
        'EGG2|LAB123': {'assay_code': 'EGG2|LAB123', 'version': '1.2.0'},
        'EGG3|LAB456': {'assay_code': 'EGG3|LAB456', 'version': '1.1.0'},
        'EGG4': {'assay_code': 'EGG4', 'version': '1.0.1'},
        'EGG5': {'assay_code': 'EGG5', 'version': '1.1.1'},
        'EGG6': {'assay_code': 'EGG6', 'version': '1.2.1'},
    }

    # test lists of samples as would be parsed from samplesheet
    single_assay_sample_list = [f'sample{x}-EGG2' for x in range(1,11)]
    mixed_assay_sample_list = single_assay_sample_list + ['sample11-EGG3']
    sample_list_w_no_code = single_assay_sample_list + ['sample11']


    def test_return_single_assay(self):
        """
        Test that when all samples are for one assay that they are matched
        to the correct assay code and returned
        """
        assay_samples = match_samples_to_assays(
            configs=self.configs,
            all_samples=self.single_assay_sample_list,
            testing=False,
            mismatch=0
        )

        correct_output = {
            'EGG2|LAB123': [
                'sample1-EGG2', 'sample2-EGG2',
                'sample3-EGG2', 'sample4-EGG2',
                'sample5-EGG2', 'sample6-EGG2',
                'sample7-EGG2', 'sample8-EGG2',
                'sample9-EGG2', 'sample10-EGG2'
            ]
        }

        assert assay_samples == correct_output, (
            'Incorrectly matched samples to assay codes'
        )


    def test_selected_highest_version(self):
        """
        Test that when matching samples to assays and multiple configs match,
        that the config wiht highest version is used
        """
        configs = {
            'EGG2|LAB123': {'assay_code': 'EGG2|LAB123', 'version': '1.0.0'},
            'EGG2|LAB123-2': {'assay_code': 'EGG3|LAB456-2', 'version': '1.2.0'},
            'EGG2|LAB123-3': {'assay_code': 'EGG2|LAB123-3', 'version': '1.11.0'}
        }

        # samples have EGG2 in name so will match all the configs, 1.11.0
        # should be selected
        matches = match_samples_to_assays(
            configs=configs,
            all_samples=self.single_assay_sample_list,
            testing=False,
            mismatch=0
        )

        assert list(matches.keys()) == ['EGG2|LAB123-3'], (
            "Wrong version of config file selected when matching to samples"
        )


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
            "sample4"
        ]

        with pytest.raises(AssertionError):
            # this should raise an AssertionError as normal for mismatch
            # between total samples and those matching assay config
            match_samples_to_assays(
                configs=self.configs,
                all_samples=sample_list,
                testing=False,
                mismatch=0
            )


    def test_mismatch_set_one(self):
        """
        Tests when missing assay code for sample to assay matching occurs
        and mismatch set to one, this should assign the missing sample to
        the same config as the other samples
        """
        sample_list = [
            "sample1-EGG2",
            "sample2-EGG2",
            "sample3-EGG2",
            "sample4"
        ]

        assay_samples = match_samples_to_assays(
            configs=self.configs,
            all_samples=sample_list,
            testing=False,
            mismatch=1
        )

        correct_output = {
            'EGG2|LAB123': [
                "sample1-EGG2",
                "sample2-EGG2",
                "sample3-EGG2",
                "sample4"
            ]
        }

        assert correct_output == assay_samples, (
            "Matching samples to assay config with single sample mismatch failed"
        )


    def test_mismatch_set_one_with_two_mismatches(self):
        """
        Tests when missing assay code for two samples when assay matching occurs
        and mismatch set to one, this should raise an AssertionError
        """
        sample_list = [
            "sample1-EGG2",
            "sample2-EGG2",
            "sample3-EGG2",
            "sample4",
            "sample5"
        ]

        with pytest.raises(AssertionError):
            match_samples_to_assays(
                configs=self.configs,
                all_samples=sample_list,
                testing=False,
                mismatch=1
            )


    def test_raise_assertion_error_on_mixed_assays(self):
        """
        Test that an AssertionError is raised when more than one assay
        code is identified in the sample list
        """
        with pytest.raises(AssertionError):
            match_samples_to_assays(
                configs=self.configs,
                all_samples=self.mixed_assay_sample_list,
                testing=False,
                mismatch=0
            )


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
                mismatch=0
            )


if __name__=="__main__":
    TestMatchSamplesToAssays().test_selected_highest_version()
