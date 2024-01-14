import pytest


class ViewTestMixin:
    """
    Automatically load in a session and client, this is common for a lot of
    tests that work with views.
    """

    @pytest.fixture(autouse=True)
    def set_common_fixtures(self, client):
        # self.session = session
        self.client = client
