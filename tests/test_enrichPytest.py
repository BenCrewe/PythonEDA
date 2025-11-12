import pandas as pd
import pytest
from Functions.enrich import Book_Returned

@pytest.fixture
def test_rental_length():
    test_df = pd.DataFrame({
        'Book Returned': ['1/02/2025', '2/03/2025', '2/01/2025'],
        'Book checkout': ['1/01/2025', '2/01/2025', '3/01/2025']
    })

    result_df = Book_Returned(test_df)

    assert (result_df['Rental_Length'] >= pd.Timedelta(0)).all(), "YOU ARE A FAILURE MR.TEST"


