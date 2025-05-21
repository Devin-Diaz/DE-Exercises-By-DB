
import unittest
from utils import extract_filename

class TestUtils(unittest.TestCase):
    def test_extract_filename(self):
        uri = "https://example.com/files/data.zip"
        expected = "data.zip"
        self.assertEqual(extract_filename(uri), expected)

    def test_extract_filename_with_query(self):
        uri = "https://example.com/files/data.zip?token=abc123"
        expected = "data.zip"
        self.assertEqual(extract_filename(uri), expected)   

if __name__ == '__main__':
    unittest.main()