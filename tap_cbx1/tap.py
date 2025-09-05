"""CBX1 tap class."""

from typing import List

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_cbx1.client import CBX1Stream
from tap_cbx1.constants import ORG_ID_KEY, CODE_KEY
from tap_cbx1.streams import AccountStream, ContactStream

STREAM_TYPES = [
    AccountStream,
    ContactStream,
]

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
        th.Property(CODE_KEY, th.StringType, required=True),
        th.Property(ORG_ID_KEY, th.StringType, required=True)
    ).to_dict()

    def discover_streams(self) -> List[CBX1Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapCBX1.cli()
