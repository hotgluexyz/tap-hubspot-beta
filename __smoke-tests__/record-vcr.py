import json
from typing import ClassVar
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from faker import Faker
from hotglue_smoke_test.vcr.sanitize import make_faker_replace_fn
from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class Runner(VCRTapTestRunner):
    PRESERVE_KEYS: ClassVar[set[str]] = {
        "archived",
        "hasMore",
        "has-more",
        "id",
        "listId",
        "offset",
        "paging",
        "total",
        "vidOffset",
    }

    def module(self) -> str:
        return "tap_hubspot_beta.tap"

    def launch(self):
        from tap_hubspot_beta.tap import Taphubspot

        Taphubspot.cli()

    def scrub_uri(self, uri: str) -> str:
        email_path = "/communication-preferences/v3/status/email/"
        parts = urlsplit(uri)
        if email_path not in parts.path:
            return uri
        prefix, email = parts.path.split(email_path, 1)
        if not hasattr(self, "_uri_replace"):
            faker = Faker()
            Faker.seed(hash(self.test_case) & 0xFFFFFFFF)
            self._uri_replace = make_faker_replace_fn(faker, {})
        scrubbed_email = quote(self._uri_replace("email", unquote(email)), safe="")
        return urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                prefix + email_path + scrubbed_email,
                parts.query,
                parts.fragment,
            )
        )

    def scrub_response_body(self, body: str, faker: Faker, cache: dict) -> str:
        scrubbed = json.loads(super().scrub_response_body(body, faker, cache))

        def restore_dynamic_fields(source, target):
            if isinstance(source, dict) and isinstance(target, dict):
                if "objectTypeId" in source:
                    for key in ("name", "objectTypeId"):
                        if key in source:
                            target[key] = source[key]
                for key, value in source.items():
                    if key in target:
                        restore_dynamic_fields(value, target[key])
            elif isinstance(source, list) and isinstance(target, list):
                for source_item, target_item in zip(source, target):
                    restore_dynamic_fields(source_item, target_item)

        restore_dynamic_fields(json.loads(body), scrubbed)
        return json.dumps(scrubbed)


if __name__ == "__main__":
    Runner.main()
