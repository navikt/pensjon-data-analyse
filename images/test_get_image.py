import pytest
from .get_image import get_image_name


def test_all_image_paths():
    assert get_image_name("dbt")
    assert get_image_name("quarto")
    assert get_image_name("wendelboe")
    with pytest.raises(ValueError):
        get_image_name("unlikely-folder-name")
