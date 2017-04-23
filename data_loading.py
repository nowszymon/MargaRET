from datetime import datetime
import os
import sqlite3

BATCH_SIZE = 5000


def parse_file(directory, filename, datetime_format="%d.%m.%Y %H:%M:%S.000", separator=","):
    header_skipped = False
    symbol = parse_symbol(filename)

    with open(os.path.join(directory, filename), 'r') as f:
        for line in f:
            if not header_skipped:
                header_skipped = True
                continue
            values = line.split(separator)
            time = datetime.strptime(values[0], datetime_format)

            yield symbol, time, values[1], values[4]


def parse_symbol(filename):
    return filename.split('_', 1)[0]


def load_files(directory, start_date=None, end_date=None):
    _, _, filenames = next(os.walk(directory))

    with get_cursor() as cursor:
        for filename in filenames:
            batch = []
            for j, data in enumerate(parse_file(directory, filename)):
                batch.append(data)

                if j == BATCH_SIZE:
                   save_records(cursor, batch)
                   batch = []

            save_records(cursor, batch)


def save_records(cursor, batch):
    ret = cursor.executemany('''INSERT INTO price (symbol, datetime, open, close) VALUES (?, ?, ?, ?)''', batch)


def get_cursor():
    conn = sqlite3.connect('prices.db')

    return conn


def initialize_database():
    with get_cursor() as cursor:
        try:
            cursor.execute('''CREATE TABLE price (symbol text, datetime datetime, open real, close real)''')
        except sqlite3.OperationalError as err:
            if 'already exists' in str(err):
                pass
            else:
                raise


if __name__=='__main__':
    initialize_database()
    load_files('data')
