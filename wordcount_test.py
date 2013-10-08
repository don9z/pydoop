import unittest
import wordcount_mapper
import wordcount_reducer

class WordCountTest(unittest.TestCase):
    def test_mapper(self):
        input = 'mapper input of wc'
        expected = ['mapper\t1',
                    'input\t1',
                    'of\t1',
                    'wc\t1']
        result = []
        for output in wordcount_mapper.mapper(input):
            result.append(output)
        self.assertEqual(expected, result)

    def test_reducer(self):
        key = 'word'
        values = [['1'], ['2'], ['3'], ['4']]
        expected = ['word\t10']

        result = []
        for output in wordcount_reducer.reducer(key, values):
            result.append(output)
        self.assertEqual(expected, result)

if __name__ == '__main__':
    unittest.main()
