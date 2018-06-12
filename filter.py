import json
from schema import Schema, Use, Optional

filter_schema = Schema({
    'criteria': [{
        'target': str,
        'command': str,
        'values': list,
        Optional('logical_operator'): str,
        Optional('negate', default=False): bool
    }],
    'action': {
        'command': str,
        Optional('values', default=[]): list
    }
})

event_schema = Schema({
    'id': int,
    'timestamp': str,
    'duration': int,
    'data': dict
})


class Filter():
    """
        Used to handle filters.
    """

    def __init__(self):
        self.filter = None
        self.filter_loaded = False

    def load_from_json_str(self, json_str):
        parsed = json.loads(json_str)
        self.load_from_dict(parsed)

    def load_from_file(self, path):
        with open(path) as file:
            parsed = json.load(file)
        self.load_from_dict(parsed)

    def load_from_dict(self, filter):
        self.filter = filter_schema.validate(filter)
        self.filter_loaded = True

    def apply_filter(self, event):
        """
            Applies this filter to the specified event.
        """
        if not self.filter_loaded:
            raise 'Tried applying filter before loading filter'

        event_schema.validate(event)
        
        data = {
            'event': event
            # place metadata here
        }

        if criteria_matches(self.filter['criteria'], data):
            action = self.filter['action']
            command = action['command']
            values = action['values']

            if command == 'remove':
                return None
            elif command == 'return':
                return event
            else:
                raise 'Unsupported action'


def criteria_matches(criteria: list, data: dict):
    """
        Return True if the filter criteria matches the data
    """
    command = criteria[0]['command']
    target_val = data
    for key in criteria[0]['target'].split('.'):
        target_val = target_val[key]
    values = criteria[0]['values']

    # Evaluate command
    criteria_matched = evaluate_command(command, target_val, values)

    # Negate ("not")
    if criteria[0]['negate']:
        criteria_matched = not criteria_matched
    
    print('criteria_matched? ' + str(criteria_matched))

    # Logical Operators
    if len(criteria) == 1 or not 'logical_operator' in criteria[0]:
        print('finished')
        return criteria_matched
    elif criteria[0]['logical_operator'] == 'and':
        print('and')
        return criteria_matched and criteria_matches(criteria[1:], data)
    elif criteria[0]['logical_operator'] == 'or':
        print('or')
        return criteria_matched or criteria_matches(criteria[1:], data)
    else:
        raise 'Unsupported logical_operator'


def evaluate_command(command, target_val, values):
    if command == 'equals':
        print('equals', target_val, values[0])
        return target_val == values[0]
    
    elif command == 'includes':
        print('includes', target_val, values[0])
        return values[0] in target_val


if __name__ == '__main__':
    filter = Filter()


    # Load filter
    filter.load_from_dict({
        "criteria": [
            {
                # Only include incognito events
                "target": "event.data.incognito",
                "command": "equals",
                "values": [
                    "True"
                ],
                "logical_operator": "and"
            }, {
                # Do not include events from 2017
                "negate": True,
                "target": "event.timestamp",
                "command": "includes",
                "values": [
                    "2017"
                ]
            }
        ],
        "action": {
            'command': 'return'
        }
    })

    # Test filter
    print(filter.apply_filter({
        'id': 1,
        'timestamp': '2018-06-12T16:06:19.567000+00:00',
        'duration': 0,
        'data': {
            'incognito': 'True'
        }
    }))
