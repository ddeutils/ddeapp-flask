from .libs.tests import ViewTestMixin


class TestInitial(ViewTestMixin):

    def test_health(self):
        """Up should respond with a success 200."""
        response = self.client.get('/health')
        assert response.status_code == 200
        assert response.json == {'message': 'DFA Flask Postgres started!!!'}
