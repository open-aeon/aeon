# Kafka Protocol Golden Fixtures

These fixtures lock Aeon's current Kafka wire bytes for representative protocol frames.

Format:

- Files are lowercase hex strings with a trailing newline.
- Frame fixtures include the Kafka 4-byte size prefix.
- Header-frame fixtures intentionally contain only the request header and are used to verify legacy versus flexible header encoding.

Fixture provenance:

- Generated from Aeon's current `Encode` implementation on 2026-07-03.
- Fixtures are compatibility artifacts. Any byte change must explain whether it is an intentional protocol correction or a regression.

Current fixtures:

| Fixture | API | Version | Type | Notes |
|---|---|---:|---|---|
| `api_versions_v0_response.hex` | ApiVersions | 0 | response frame | legacy response header/body |
| `api_versions_v3_response.hex` | ApiVersions | 3 | response frame | flexible response header/body |
| `metadata_v0_request_header_frame.hex` | Metadata | 0 | request header frame | legacy nullable client id |
| `metadata_v9_request_header_frame.hex` | Metadata | 9 | request header frame | flexible compact nullable client id and tagged fields |

