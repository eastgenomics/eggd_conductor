from unittest.mock import patch, MagicMock

import pytest

from utils.demultiplexing import (
    set_config_for_demultiplexing,
    get_demultiplex_job_details,
    additional_args_check,
)


def test_set_config_for_demultiplexing_no_configs():
    output = set_config_for_demultiplexing({"not_demultiplex_config": 1})

    assert output is None


def test_set_config_for_demultiplexing_no_core_nb():
    output = set_config_for_demultiplexing(
        {"demultiplex_config": {"not_instance_type": 1}}
    )

    assert output is None


def test_set_config_for_demultiplexing_w_core_nb():
    output = set_config_for_demultiplexing(
        {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x16"}}
    )

    assert output == {"instance_type": "mem1_ssd1_v2_x16"}


def test_set_config_for_demultiplexing_select_highest_core_nb_config():
    output = set_config_for_demultiplexing(
        {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x16"}},
        {"demultiplex_config": {"instance_type": "mem1_ssd1_v2_x72"}},
        {"demultiplex_config": {"instance_type": "mem1_ssd2_v2_x36"}},
    )

    assert output == {"instance_type": "mem1_ssd1_v2_x72"}


@patch("utils.demultiplexing.dx.search.find_data_objects")
@patch("utils.demultiplexing.dx.bindings.dxjob.DXJob")
def test_get_demultiplex_job_details(mock_job, mock_data_objects):
    mock_job.return_value = MagicMock(
        describe=MagicMock(project="project_name", folder="folder_name")
    )
    mock_data_objects.return_value = (
        {
            "id": "id1",
            "describe": {"name": "name1"},
        },
        {
            "id": "id2",
            "describe": {"name": "name2"},
        },
        {
            "id": "id3",
            "describe": {"name": "Undetermined_name2"},
        },
    )

    expected_output = [("id1", "name1"), ("id2", "name2")]

    output = get_demultiplex_job_details("")

    assert output == expected_output


class TestAdditionalArgsCheck:
    def test_no_config(self):
        test_output = additional_args_check([{"not_demultiplex_config": 1}])

        assert test_output is None

    def test_one_config_no_additional_args(self):
        test_output = additional_args_check([{"demultiplex_config": {}}])

        assert test_output is None

    def test_one_config_with_additional_args(self):
        test_output = additional_args_check(
            [{"demultiplex_config": {"additional_args": 1}}]
        )

        assert test_output == {"demultiplex_config": {"additional_args": 1}}

    def test_multiple_config(self):
        test_output = additional_args_check(
            [{"demultiplex_config": {}}, {"demultiplex_config": {}}]
        )

        assert test_output is None

    def test_multiple_config_one_additional_args(self):
        test_output = additional_args_check(
            [
                {"demultiplex_config": {}},
                {"demultiplex_config": {"additional_args": 1}},
            ]
        )

        assert test_output == {"demultiplex_config": {"additional_args": 1}}

    def test_multiple_config_multiple_additional_args(
        self,
    ):
        with pytest.raises(
            Exception, match="Multiple additional args specified"
        ):
            additional_args_check(
                [
                    {"demultiplex_config": {"additional_args": 1}},
                    {"demultiplex_config": {"additional_args": 2}},
                ]
            )
