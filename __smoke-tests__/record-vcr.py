from urllib.parse import quote, unquote, urlsplit, urlunsplit

from faker import Faker
from hotglue_smoke_test.vcr.tap import VCRTapTestRunner
from hotglue_smoke_test.vcr.sanitize import (
    make_faker_replace_fn,
    sanitize_cassette_file,
    scrub_response_body,
)


class Runner(VCRTapTestRunner):
    PRESERVE_KEYS = {
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
                (
                    parts.scheme,
                    parts.netloc,
                    prefix + email_path + scrubbed_email,
                    parts.query,
                    parts.fragment,
                )
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
