import json
from schema import Schema, Use, Optional
import logging

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

filter_commands = {
    'equals': lambda x, y: x == y,
    'includes': lambda x, y: y in x,
    '>': lambda x, y: x > y,
    '>=': lambda x, y: x >= y,
    '<': lambda x, y: x < y,
    '<=': lambda x, y: x <= y
}

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
            raise NameError 

        event_schema.validate(event)
        
        data = {
            'event': event
            # place metadata here
        }

        action = self.filter['action']
        command = action['command']
        values = action['values']

        if criteria_matches(self.filter['criteria'], data):
            if command == 'remove':
                return None
            elif command == 'return':
                return event
            else:
                raise NameError
        
        else:
            if command == 'remove':
                return event
            elif command == 'return':
                return None
            else:
                raise NameError


def criteria_matches(criteria: list, data: dict):
    """
        Return True if the filter criteria matches the data
    """
    command = criteria[0]['command']
    target_val = data
    for key in criteria[0]['target'].split('.'):
        if key in target_val:
            target_val = target_val[key]
        else:
            raise NameError
            
    values = criteria[0]['values']
    negate = criteria[0]['negate']

    # Evaluate command
    criteria_matched = evaluate_command(command, target_val, values)

    # Negate ("not")
    if negate:
        criteria_matched = not criteria_matched
    
    logging.debug('negated? %s', negate)
    logging.debug('criteria_matched? %s', criteria_matched)

    # Logical Operators
    if len(criteria) == 1 or not 'logical_operator' in criteria[0]:
        logging.debug('no more logical operators applied')
        logging.debug('returning: %s', criteria_matched)
        return criteria_matched
    elif criteria[0]['logical_operator'] == 'and':
        logging.debug('and')
        return criteria_matched and criteria_matches(criteria[1:], data)
    elif criteria[0]['logical_operator'] == 'or':
        logging.debug('or')
        return criteria_matched or criteria_matches(criteria[1:], data)
    else:
        raise NameError


def evaluate_command(command, target_val, values):
    logging.debug('%s: [%s][%s]', command, target_val, values[0])

    if command in filter_commands:
        return filter_commands[command](target_val, values[0])
    else:
        raise NameError


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

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
    logging.info('loaded filter')

    # Test filter
    result = filter.apply_filter({
        'id': 1,
        'timestamp': '2018-06-12T16:06:19.567000+00:00',
        'duration': 0,
        'data': {
            'incognito': 'True'
        }
    })

    logging.info('filter result: %s', result)
