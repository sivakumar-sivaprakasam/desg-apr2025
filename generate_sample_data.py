import csv
import random
import string
from typing import Dict
from datetime import datetime, timedelta

from loguru import logger
import numpy as np

class RandomFileGenerator:
    def __init__(self, columns: Dict[str, type], records: int):
        """
        Example usage:
            generator = RandomFileGenerator(columns={}, records=100)
            generator.generate_csv()
        """
        self.columns = columns
        self.records = records
        self.filename = f"data/output_{records}.csv"

    @staticmethod
    def random_string(length=10, size=None):
        if size is None:
            return ''.join(random.choices(string.ascii_letters, k=length))
        else:
            return [''.join(random.choices(string.ascii_letters, k=length)) for _ in range(size)]

    @staticmethod
    def random_int(low=1, high=100, size=None) -> int:
         if size is None:
            return random.randint(low, high)
         else:
            return np.random.randint(low, high, size=size)

    @staticmethod
    def random_float(low=1, high=100, size=None) -> float:
        if size is None:
            return round(random.uniform(low, high), 2)
        else:
            return np.round(np.random.uniform(low, high, size=size), 2)

    @staticmethod
    def random_bool(size=None) -> bool:
        if size is None:
            return random.choice([True, False])
        else:
            return np.random.choice([True, False], size=size)
    
    @staticmethod
    def random_datetime(start_date="2000-01-01", end_date="2030-12-31", size=None):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        time_between_dates = end - start
        days_between_dates = time_between_dates.days

        if size is None:
            random_number_of_days = random.randrange(days_between_dates)
            random_date = start + timedelta(days=random_number_of_days)
            return random_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            random_number_of_days = np.random.randint(0, days_between_dates, size=size)
            random_dates = [start + timedelta(days=int(days)) for days in random_number_of_days] # Convert to int
            return [date.strftime("%Y-%m-%d %H:%M:%S") for date in random_dates]


    def generate_csv(self):
        type_generators = {
            str: self.random_string,
            int: self.random_int,
            float: self.random_float,
            bool: self.random_bool,
            datetime: self.random_datetime
        }

        with open(self.filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.columns.keys())  # Write headers

            # Generate data in chunks for better performance
            chunk_size = 10000  # Adjust chunk size as needed
            for i in range(0, self.records, chunk_size):
                chunk = min(chunk_size, self.records - i)
                rows = []
                for col_type in self.columns.values():

                    rows.append(type_generators[col_type](size=chunk))

                # Transpose the list of lists to get rows
                rows = list(zip(*rows))
                writer.writerows(rows)


        logger.info(f"CSV file '{self.filename}' generated with {self.records} records.")

generator = RandomFileGenerator(columns={
    "EmpName": str,
    "DeptName": str,
    "Age": int,
    "Salary": float,
    "IsActive": bool,
    "JoiningDate": datetime
}, records=1_000)
generator.generate_csv()