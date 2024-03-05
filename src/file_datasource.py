from csv import DictReader as reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
import config


class FileDatasource:
    def __init__(
        self,
        accelerometer_filename: str,
        gps_filename: str,
        parking_filename: str
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename

        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

        self.accelerometer_reader = None
        self.gps_reader = None
        self.parking_reader = None
        

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        accelerometer_data = next(self.accelerometer_reader)
        gps_data = next(self.gps_reader)
        parking_data = next(self.parking_reader)

        gps = Gps(gps_data["longitude"], gps_data["latitude"])
        return AggregatedData(
            Accelerometer(accelerometer_data["x"], accelerometer_data["y"], accelerometer_data["z"]),
            gps,
            Parking(parking_data["empty_count"], gps),
            datetime.now(),
            config.USER_ID,
        )

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_file = open(self.accelerometer_filename, "r")
        self.accelerometer_reader = reader(self.accelerometer_file)

        self.gps_file = open(self.gps_filename, "r")
        self.gps_reader = reader(self.gps_file)

        self.parking_file = open(self.parking_filename, "r")
        self.parking_reader = reader(self.parking_file)

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        self.gps_file.close()
        self.accelerometer_file.close()
        self.parking_file.close()

        self.accelerometer_file = None
        self.gps_file = None
        self.parking_file = None

        self.accelerometer_reader = None
        self.gps_reader = None
        self.parking_reader = None
