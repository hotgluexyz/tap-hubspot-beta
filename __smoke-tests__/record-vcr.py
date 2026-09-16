import time
from typing import ClassVar
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests
from faker import Faker
from hotglue_smoke_test.vcr.sanitize import (
    make_faker_replace_fn,
    sanitize_cassette_file,
    scrub_response_body,
)
from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class Runner(VCRTapTestRunner):
    PRESERVE_KEYS: ClassVar[set[str]] = {
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

        if self.mode != "record":
            Taphubspot.cli()
            return

        original_request = requests.Session.request

        def request_with_ssl_retry(session, method, url, **kwargs):
            for attempt in range(5):
                try:
                    return original_request(session, method, url, **kwargs)
                except requests.exceptions.SSLError:
                    if attempt == 4:
                        raise
                    time.sleep(2**attempt)

        requests.Session.request = request_with_ssl_retry
        try:
            Taphubspot.cli()
        finally:
            requests.Session.request = original_request

    def sanitize_cassette(self):
        faker = Faker()
        Faker.seed(hash(self.test_case) & 0xFFFFFFFF)
        cache = {}
        replace = make_faker_replace_fn(faker, cache)
        email_path = "/communication-preferences/v3/status/email/"

        def scrub_uri(uri: str) -> str:
            parts = urlsplit(uri)
            if email_path not in parts.path:
                return uri
            prefix, email = parts.path.split(email_path, 1)
            scrubbed_email = quote(replace("email", unquote(email)), safe="")
            return urlunsplit(
                (parts.scheme, parts.netloc, prefix + email_path + scrubbed_email, parts.query, parts.fragment)
            )

        sanitize_cassette_file(
            self.vcr_cassette_path,
            scrub_response=lambda body: scrub_response_body(
                body, set(self.PRESERVE_KEYS), faker, cache, set(self.TOKEN_KEYS)
            ),
            scrub_uri=scrub_uri,
        )


if __name__ == "__main__":
    Runner.main()
