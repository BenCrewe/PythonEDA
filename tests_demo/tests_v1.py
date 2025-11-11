import unittest
from calculator_app import Calculator

class TestOperations(unittest.TestCase):
    # def test_sum(self):
    #     calc = Calculator(10,2)
    #     self.assertEqual(calc.get_sum(), 12, "Sum is wrong")

    # def test_difference(self):
    #     calc = Calculator(10,2)
    #     self.assertEqual(calc.get_difference(), 8, "Difference is wrong")

    # def test_product(self):
    #     calc = Calculator(10,2)
    #     self.assertEqual(calc.get_product(), 20, "Product is wrong")

    # def test_quotient(self):
    #     calc = Calculator(10,2)
    #     self.assertEqual(calc.get_quotient(), 5, "Quotient is wrong")

    def setUp(self):
        self.calc = Calculator(100,2)

    def test_sum(self):
        self.assertEqual(self.calc.get_sum(),102,"The Answer is wrong")

if __name__ == "__main__":
    unittest.main()