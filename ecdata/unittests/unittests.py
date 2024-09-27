import unittest
import polars as pl
import requests
from unittest.mock import patch

# Import your functions here
# from your_module import country_dictionary, link_builder, get_ecd_release, validate_input, load_ecd

class TestYourFunctions(unittest.TestCase):

    def test_country_dictionary(self):
        df = country_dictionary()
        self.assertIsInstance(df, pl.DataFrame)
        self.assertIn('name_in_dataset', df.columns)
        self.assertEqual(len(df), 41)  # Ensure the length matches the expected number of countries

    def test_link_builder_single_country(self):
        url = link_builder("Argentina", ecd_version='1.0.0')
        expected_url = "https://github.com/joshuafayallen/executivestatements/releases/download/1.0.0/argentina.parquet"
        self.assertEqual(url[0], expected_url)

    def test_link_builder_multiple_countries(self):
        urls = link_builder(["Argentina", "Brazil"], ecd_version='1.0.0')
        expected_urls = [
            "https://github.com/joshuafayallen/executivestatements/releases/download/1.0.0/argentina.parquet",
            "https://github.com/joshuafayallen/executivestatements/releases/download/1.0.0/brazil.parquet"
        ]
        self.assertEqual(len(urls), 2)
        self.assertIn(expected_urls[0], urls)
        self.assertIn(expected_urls[1], urls)

    @patch('requests.get')
    def test_get_ecd_release(self, mock_get):
        # Mocking the requests.get call
        mock_get.return_value.ok = True
        mock_get.return_value.json.return_value = [
            {"name": "Release 1", "id": 1, "body": "First release", "tag_name": "0.0.1", "draft": False, "created_at": "2021-01-01", "published_at": "2021-01-01", "html_url": "https://example.com"},
            {"name": "Release 2", "id": 2, "body": "Second release", "tag_name": "0.0.2", "draft": False, "created_at": "2021-01-02", "published_at": "2021-01-02", "html_url": "https://example.com"}
        ]

        releases = get_ecd_release(repo='joshuafayallen/executivestatements')
        self.assertEqual(len(releases), 2)
        self.assertIn("Release 1", releases)
        self.assertIn("Release 2", releases)

    def test_validate_input_valid(self):
        try:
            validate_input(country="Argentina", full_ecd=False, version='0.0.1')
        except ValueError as e:
            self.fail(f'validate_input raised ValueError unexpectedly: {str(e)}')

    def test_validate_input_invalid_country(self):
        with self.assertRaises(ValueError) as context:
            validate_input(country="InvalidCountry", full_ecd=False)
        self.assertEqual(str(context.exception), 'InvalidCountry is not a valid country name in our dataset. Call country_dictionary for a list of valid inputs')

    def test_load_ecd_valid(self):
        # Here, you'll need to mock the reading of a parquet file or test against a known dataset.
        with patch('polars.read_parquet') as mock_read_parquet:
            mock_read_parquet.return_value = pl.DataFrame({'column1': [1, 2, 3]})  # Mocked DataFrame
            data = load_ecd(country="Argentina", full_ecd=False, ecd_version='0.0.1')
            self.assertIsInstance(data, pl.DataFrame)

    def test_load_ecd_full_ecd(self):
        # Mock the reading of a parquet file for the full dataset
        with patch('polars.read_parquet') as mock_read_parquet:
            mock_read_parquet.return_value = pl.DataFrame({'column1': [1, 2, 3]})
            data = load_ecd(full_ecd=True, ecd_version='0.0.1')
            self.assertIsInstance(data, pl.DataFrame)

if __name__ == '__main__':
    unittest.main()