import unittest
import runner

class RunnerokTest(unittest.TestCase):
    def test_combine_bykeys(self):
        test_stdin = ['1\tv1\tv2\tv3',
                      '1\tv4\tv5\tv6',
                      '2\tv21\tv22\tv23',
                      '2\tv24\tv25\tv26',
                      '2\tv27\tv28\tv29',
                      '3\tv31\tv32\tv33']
        expected = {'1' : [['v1', 'v2', 'v3'],
                           ['v4', 'v5', 'v6']],
                    '2' : [['v21', 'v22', 'v23'],
                           ['v24', 'v25', 'v26'],
                           ['v27', 'v28', 'v29']],
                    '3' : [['v31', 'v32', 'v33']]}
        result = {}
        for keyvalues in runner.combine_bykeys(test_stdin):
            result[keyvalues[0]] = keyvalues[1]

        self.assertEqual(expected, result)

    def test_combine_bykeys_when_empty_input(self):
        test_stdin = []
        for keyvalues in runner.combine_bykeys(test_stdin):
            # Should emit nothing
            self.assertIsNone(1)

if __name__ == '__main__':
    unittest.main()
