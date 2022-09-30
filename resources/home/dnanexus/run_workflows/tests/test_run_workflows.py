import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from tests import TEST_DATA_DIR
from run_workflows import parse_run_info_xml, parse_sample_sheet


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



if __name__=="__main__":
    pass
