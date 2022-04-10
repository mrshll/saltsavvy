import dataclasses
import datetime
from enum import Enum


@dataclasses.dataclass
class HRRRForecast:
    run_hour: datetime.datetime
    level: str
    model: str

    def get_explorer_url(self):
        url = "https://hrrrzarr.s3.amazonaws.com/index.html"
        url += self.run_hour.strftime(
            f"#{self.level}/%Y%m%d/%Y%m%d_%Hz_{self.model}.zarr/")
        return url

    def get_https_chunk_url(self, hrrr_var, chunk_id):
        url = "https://hrrrzarr.s3.amazonaws.com"
        url += self.run_hour.strftime(
            f"/{self.level}/%Y%m%d/%Y%m%d_%Hz_{self.model}.zarr/")
        url += f"{hrrr_var.level}/{hrrr_var.name}/{hrrr_var.level}/{hrrr_var.name}"
        url += f"/{self._format_chunk_id(chunk_id)}"
        return url

    def get_s3_chunk_url(self, hrrr_var, chunk_id, prefix=False):
        url = self._get_s3_subgroup_url(hrrr_var, prefix)
        url += f"/{hrrr_var.name}/{self._format_chunk_id(chunk_id)}"
        return url

    def _get_s3_group_url(self, hrrr_var, prefix=True):
        url = "s3://hrrrzarr/" if prefix else ""  # Skip when using boto3
        url += self.run_hour.strftime(
            f"{self.level}/%Y%m%d/%Y%m%d_%Hz_{self.model}.zarr/")
        url += f"{hrrr_var.level}/{hrrr_var.name}"
        return url

    def _get_s3_subgroup_url(self, hrrr_var, prefix=True):
        url = self._get_s3_group_url(hrrr_var, prefix)
        url += f"/{hrrr_var.level}"
        return url

    def _format_chunk_id(self, chunk_id):
        if self.model == "fcst":
            # Extra id part since forecasts have an additional (time) dimension
            return "0." + chunk_id
        else:
            return chunk_id


@dataclasses.dataclass
class HRRRVariable:
    level: str
    name: str
