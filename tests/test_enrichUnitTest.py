import unittest
from Functions.enrich import Book_Returned
import pandas as pd


class TestOperations(unittest.TestCase):

    def setUp(self):
        self.test_df = pd.DataFrame({
            'Book Returned': ['1/02/2025', '2/03/2025', '2/01/2025'],
            'Book checkout': ['1/01/2025', '2/01/2025', '3/01/2025']
        })

    # Convert Columns of self.test_df to datetime
        self.test_df['Book Returned'] = pd.to_datetime(self.test_df['Book Returned'], format='mixed', errors='coerce')
        self.test_df['Book checkout'] = pd.to_datetime(self.test_df['Book checkout'], format='mixed', errors='coerce')

    # Apply your Book_Returned function to the self.test_df
        Book_Returned(self.test_df)

    def test_value_is_positive(self):
        self.assertTrue(
            (self.test_df['Rental_Length'] >= pd.Timedelta(0)).all(),
        "Lower than 0. YOU HAVE FAILED"
        )


if __name__ == "__main__":
    unittest.main()