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
    # DXManage.get_json_configs()
    configs = {
        'EGG2': {'assay_code': 'EGG2'},
        'EGG3': {'assay_code': 'EGG3'},
        'EGG4': {'assay_code': 'EGG4'},
        'EGG5': {'assay_code': 'EGG5'},
        'EGG6': {'assay_code': 'EGG6'},
    }

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
            testing=False
        )

        correct_output = {
            'EGG2': [
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

    def test_raise_assertion_error_on_mixed_assays(self):
        """
        Test that an AssertionError is raised when more than one assay
        code is identified in the sample list
        """
        with pytest.raises(AssertionError):
            match_samples_to_assays(
                configs=self.configs,
                all_samples=self.mixed_assay_sample_list,
                testing=False
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
                testing=False
            )









if __name__=="__main__":
    TestMatchSamplesToAssays().test_raise_assertion_error_on_sample_w_no_assay_code_match()
