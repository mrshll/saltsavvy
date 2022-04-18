from abc import ABC, abstractmethod
import datetime
import dataclasses


@dataclasses.dataclass(frozen=True)
class Forecast(ABC):
    issue_time: datetime.datetime
    level: str

    @abstractmethod
    def fetch_data_for_lat_lng(self):
        pass

    @abstractmethod
    def fetch_analysis_for_lat_lng(self):
        pass
