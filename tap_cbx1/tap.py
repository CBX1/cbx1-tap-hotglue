"""CBX1 tap class."""

import inspect
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_cbx1 import streams


class TapCBX1(Tap):
    """CBX1 tap class."""
    def __init__(
            self,
            config=None,
            catalog=None,
            state=None,
            parse_env_config=False,
            validate_config=True,
        ) -> None:
            self.config_file = config[0]
            super().__init__(config, catalog, state, parse_env_config, validate_config)

    name = "tap-cbx1"

    config_jsonschema = th.PropertiesList(
        th.Property("access_key", th.StringType, required=True),
        th.Property("organization_id", th.StringType, required=True),
        th.Property("user_id", th.StringType, required=True)
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
            cls(self)
            for _, cls in inspect.getmembers(streams, inspect.isclass)
            if cls.__module__ == "tap_cbx1.streams" and hasattr(cls, "name")
        ]


if __name__ == "__main__":
    TapCBX1.cli()
