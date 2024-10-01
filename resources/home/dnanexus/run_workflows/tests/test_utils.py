import os
import re
import sys
import unittest

import pytest

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from utils.utils import select_instance_types


class TestSelectInstanceTypes():
    instance_types = {
        "*": {
            "default_instances": ""
        },
        "S1": {
            "S1_instances_from_S1": ""
        },
        "S2": {
            "S2_instances_from_S2": ""
        },
        "S4": {
            "S4_instances_from_S4": ""
        },
        "xxxxxDRxx": {
            "SP_S1_instances_from_DR_pattern": ""
        },
        "xxxxxDMxx": {
            "S2_instances_from_DM_pattern": ""
        },
        "xxxxxDSxx": {
            "S4_instances_from_DS_pattern": ""
        },
        "Kxxxx": {
            "MiSeq_instances_from_K_pattern": ""
        }
    }

    def test_select_S1(self):
        """
        Tests that S1 instances can be correctly selected from an S1 flowcell
        ID pattern when the xxxxxDMxx is defined in the instance type dict
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDRXY",
            instance_types=self.instance_types)

        correct_dict = {"SP_S1_instances_from_DR_pattern": ""}

        assert selected_instance_types == correct_dict, (
            "wrong instances type selected for xxxxxDRxx flowcell"
        )

    def test_select_S2(self):
        """
        Tests that S2 instances can be correctly selected from an S2 flowcell
        ID pattern where defined in instance type dict as S2
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDMXY",
            instance_types=self.instance_types)

        correct_dict = {"S2_instances_from_DM_pattern": ""}

        assert selected_instance_types == correct_dict, (
            "wrong instances type selected for xxxxxDMxx flowcell"
        )

    def test_select_S4(self):
        """
        Tests that S4 instances can be correctly selected from an S4 flowcell
        ID pattern when the xxxxxDSxx is defined in the instance type dict
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLDSXY",
            instance_types=self.instance_types)

        correct_dict = {"S4_instances_from_DS_pattern": ""}

        assert selected_instance_types == correct_dict, (
            "wrong instances type selected for xxxxxDSxx flowcell"
        )

    def test_select_S1_from_S1(self):
        """
        Test where S1 is in the instance types dict and xxxxxDRxx is not, that
        the dict for S1 is correctly selected
        """
        instances = self.instance_types.copy()
        instances.pop('xxxxxDRxx')

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDRXY",
            instance_types=instances)

        correct_dict = {'S1_instances_from_S1': ''}

        assert selected_instance_types == correct_dict, (
            "wrong instances type selected for S1"
        )

    def test_select_S2_from_S2(self):
        """
        Test where S2 is in the instance types dict and xxxxxDMxx is not, that
        the dict for S2 is correctly selected
        """
        instances = self.instance_types.copy()
        instances.pop('xxxxxDMxx')

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDMXY",
            instance_types=instances)

        correct_dict = {'S2_instances_from_S2': ''}

        assert selected_instance_types == correct_dict, (
            "wrong instances type selected for S2"
        )

    def test_select_S4_from_S4(self):
        """
        Test where S4 is in the instance types dict and xxxxxDSxx is not, that
        the dict for S4 is correctly selected
        """
        instances = self.instance_types.copy()
        instances.pop('xxxxxDSxx')

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDSXY",
            instance_types=instances)

        correct_dict = {'S4_instances_from_S4': ''}

        assert selected_instance_types == correct_dict, (
            "wrong instances type selected for S4"
        )

    def test_default_instance_set_used(self):
        """
        Test that when the flowcell ID matches none of the given patterns
        in the defined instance type dict, the default set is used ("*")
        """
        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYAAAXY",
            instance_types=self.instance_types)

        correct_dict = {"default_instances": ""}

        assert selected_instance_types == correct_dict, (
            "Incorrect default instances used"
        )

    def test_return_none_with_no_default(self):
        """
        Test where the flowcell ID matches none of the given pattern and
        no default is provided, that None is returned which will cause
        dxpy to just use the defaults set by the app / workflow
        """
        instances = self.instance_types.copy()
        instances.pop("*")

        selected_instance_types = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYAAAXY",
            instance_types=instances)

        assert selected_instance_types == None, (
            "None not returned for no match"
        )

    def test_miseq_pattern(self):
        """
        Test where flowcell ID is for MiSeq run and Kxxxx pattern is
        provided in the instance types it matches
        """
        selected_instance_types = select_instance_types(
            run_id="230201_M03595_0015_000000000-KRW44",
            instance_types=self.instance_types)

        correct_dict = {"MiSeq_instances_from_K_pattern": ""}

        assert selected_instance_types == correct_dict, (
            "Wrong instance types selected fro MiSeq K pattern"
        )

    def test_return_string(self):
        """
        Test where flowcell value is a string (i.e. for an app and not
        workflow stages)
        """
        instance_types = {
            "S1": "mem2_ssd1_v2_x16",
            "S2": "mem2_ssd1_v2_x48",
            "S4": "mem2_ssd1_v2_x96"
        }

        selected_instance = select_instance_types(
            run_id="230324_A01295_0171_BHFFLYDRXY",
            instance_types=instance_types)

        assert selected_instance == "mem2_ssd1_v2_x16", (
            "Wrong instance type returned where return type should be a string"
        )


# class TestSubsetSamplesheetSamples(unittest.TestCase):
#     """
#     Tests for utils.subset_samplesheet_samples

#     Function takes a list of sample names parsed from the samplesheet and
#     a regex pattern against which to filter them down and retain samples
#     """
#     samples = ['sample-1-123-foo', 'sample-2-567-bar', 'sample-3-789-baz']

#     def test_subset_correct(self):
#         """
#         Test that the subset is returned correctly
#         """
#         subset_samples = subset_samplesheet_samples(
#             samples=self.samples,
#             subset=r'-123-|-567-'
#         )

#         expected_subset = ['sample-1-123-foo', 'sample-2-567-bar']

#         assert sorted(subset_samples) == expected_subset, (
#             'expected sample subset incorret'
#         )


#     def test_no_samples_retained_raises_assertion_error(self):
#         """
#         Test that when no samples are left after subet that we correctly
#         raise an AssertionError
#         """
#         expected_error = 'No samples left after filtering using pattern blarg'

#         with pytest.raises(AssertionError, match=expected_error):
#             subset_samplesheet_samples(
#             samples=self.samples,
#             subset='blarg'
#         )

#     def test_invalid_regex_pattern_raises_regex_error(self):
#         """
#         Test that when an invalid regex pattern is provided that a
#         re.error is raised
#         """
#         expected_error = 'Invalid subset pattern provided'

#         with pytest.raises(re.error, match=expected_error):
#             subset_samplesheet_samples(
#             samples=self.samples,
#             subset='[invalid'
#         )
