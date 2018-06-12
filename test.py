import unittest
from filter import Filter

filter = Filter()
test_event = {
    'id': 1,
    'timestamp': '2018-06-12T16:06:19.567000+00:00',
    'duration': 0,
    'data': {
        'str': 'str',
        'int': 123,
        'float': 123.45,
        'bool': True,
        'obj': {
            'key': 'val'
        },
        'sentence': 'This is a nice sentence.'
    }
}


class TestFilters(unittest.TestCase):

    def test_equal_str(self):
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.str', 'str'))

    def test_equal_int(self):
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.int', 123))

    def test_equal_float(self):
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.float', 123.45))

    def test_equal_bool(self):
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.bool', True))

    def test_equal_obj(self):
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.obj', {'key': 'val'}))

    def test_includes(self):
        self.assertEqual(test_event, test_criteria(
            'includes', 'event.data.sentence', ' '))

    def test_greater_than(self):
        self.assertEqual(test_event, test_criteria(
            '>', 'event.data.int', 100))
        self.assertEqual(test_event, test_criteria(
            '>', 'event.data.str', 'aaa'))

    def test_greater_than_or_equal(self):
        self.assertEqual(test_event, test_criteria(
            '>=', 'event.data.int', 100))
        self.assertEqual(test_event, test_criteria(
            '>=', 'event.data.int', 123))
        self.assertEqual(test_event, test_criteria(
            '>=', 'event.data.str', 'str'))
        self.assertEqual(test_event, test_criteria(
            '>=', 'event.data.str', 'aaa'))

    def test_lower_than(self):
        self.assertEqual(test_event, test_criteria(
            '<', 'event.data.int', 150))
        self.assertEqual(test_event, test_criteria(
            '<', 'event.data.str', 'zzz'))

    def test_lower_than_or_equal(self):
        self.assertEqual(test_event, test_criteria(
            '<=', 'event.data.int', 150))
        self.assertEqual(test_event, test_criteria(
            '<=', 'event.data.int', 123))
        self.assertEqual(test_event, test_criteria(
            '<=', 'event.data.str', 'str'))
        self.assertEqual(test_event, test_criteria(
            '<=', 'event.data.str', 'zzz'))

    def test_negate(self):
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.bool', 'True', negate=True))

    def test_action_return(self):
        # Should return event if criteria matches
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.str', 'str', action_command='return'))
        # Should return None if criteria does not match
        self.assertEqual(None, test_criteria(
            'equals', 'event.data.str', False, action_command='return'))

    def test_action_remove(self):
        # Should return None if criteria matches
        self.assertEqual(None, test_criteria(
            'equals', 'event.data.str', 'str', action_command='remove'))
        # Should return event if criteria does not match
        self.assertEqual(test_event, test_criteria(
            'equals', 'event.data.str', False, action_command='remove'))

    """ Unsupported actions by now:
    def test_action_replace(self):
        expected_result = test_event
        expected_result['data']['sentence'] = 'CENSORED'
        self.assertEqual(expected_result, test_criteria(
            'includes', 'event.data.sentence', ' ', action_command='replace', action_values=['event.data.sentence', 'CENSORED']))
    """

    """
        TODO: Add unittests for:
        - logical operators (and | or)
        - time ranges
        - metadata
        - regex
    """


def test_criteria(command, target_path, values, negate=False, action_command='return', action_values=[]):
    """
        Creates a filter and returns filter.apply_filter(test_event)
    """
    if type(values) is not list:
        values = [values]
    if type(action_values) is not list:
        action_values = [action_values]

    # Load filter
    filter.load_from_dict({
        "criteria": [
            {
                "target": target_path,
                "command": command,
                "values": values,
                "negate": negate
            }
        ],
        "action": {
            'command': action_command,
            'values': action_values
        }
    })
    # Filter event
    return filter.apply_filter(test_event)


if __name__ == '__main__':
    unittest.main()
